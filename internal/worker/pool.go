// Package worker implements the concurrent processing core of the pipeline.
// A Pool of goroutines reads TickData, calculates spreads, applies dedup,
// and emits ArbitrageSignals above the configured minimum threshold.
package worker

import (
	"context"
	"sync"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"go.uber.org/zap"
)

// DedupFunc is a function that returns true if the signal is novel (not a duplicate).
// Passing nil disables deduplication.
type DedupFunc func(ctx context.Context, sig market.ArbitrageSignal) (bool, error)

// Pool manages a fixed set of worker goroutines that process TickData concurrently.
type Pool struct {
	workerCount  int
	minSpreadPct float64
	logger       *zap.Logger
	dedup        DedupFunc
	wg           sync.WaitGroup
}

// New creates a worker Pool. Pass nil for dedup to skip deduplication.
func New(workerCount int, minSpreadPct float64, logger *zap.Logger, dedup DedupFunc) *Pool {
	return &Pool{
		workerCount:  workerCount,
		minSpreadPct: minSpreadPct,
		logger:       logger,
		dedup:        dedup,
	}
}

// Start spins up workerCount goroutines. Each goroutine ranges over in until
// the channel is closed. Call Wait() to block until all workers have exited.
func (p *Pool) Start(ctx context.Context, in <-chan market.TickData, out chan<- market.ArbitrageSignal) {
	p.logger.Info("worker pool: starting",
		zap.Int("count", p.workerCount),
		zap.Float64("min_spread_pct", p.minSpreadPct),
	)
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.work(ctx, i, in, out)
	}
}

// Wait blocks until all workers have finished processing.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// CalculateSpread computes the spread percentage between bid and ask prices.
// Returns 0 if bid is zero to prevent division by zero.
// Exported so it can be unit-tested independently.
func CalculateSpread(bid, ask float64) float64 {
	if bid <= 0 {
		return 0
	}
	return ((ask - bid) / bid) * 100
}

// work is the goroutine body. It ranges over in (blocking until items arrive
// or the channel is closed), calculates the spread, applies dedup, and sends
// qualifying signals to out.
//
// Shutdown flow:
//  1. ctx is cancelled → ingester stops → tickCh is closed
//  2. "range in" drains remaining ticks and exits naturally
//  3. wg.Done() is called, signalling the pool watcher to close sigCh
func (p *Pool) work(ctx context.Context, id int, in <-chan market.TickData, out chan<- market.ArbitrageSignal) {
	defer p.wg.Done()
	p.logger.Debug("worker: started", zap.Int("id", id))

	for tick := range in {
		spread := CalculateSpread(tick.BidPrice, tick.AskPrice)
		if spread < p.minSpreadPct {
			continue
		}

		sig := market.ArbitrageSignal{
			Pair:         tick.Pair,
			BuyExchange:  tick.BuyExchange,
			SellExchange: tick.SellExchange,
			BidPrice:     tick.BidPrice,
			AskPrice:     tick.AskPrice,
			SpreadPct:    spread,
			Timestamp:    tick.Timestamp,
		}

		// Optional Redis dedup: skip signals seen within the last 5 seconds
		if p.dedup != nil {
			novel, err := p.dedup(ctx, sig)
			if err != nil {
				p.logger.Warn("worker: dedup check failed, allowing signal", zap.Error(err))
			} else if !novel {
				continue // duplicate within TTL window
			}
		}

		select {
		case out <- sig:
		case <-ctx.Done():
			// Context cancelled while trying to send. Exit immediately;
			// the hard shutdown timeout in main.go handles any data still in flight.
			return
		}
	}

	p.logger.Debug("worker: input channel closed, exiting", zap.Int("id", id))
}
