// Package storage implements high-throughput database writes using pgx.Batch.
// Instead of one INSERT per signal (~1,000 round-trips/sec), the Batcher
// accumulates signals in memory and flushes them as a single batch transaction
// when either the batch is full or a ticker fires.
//
// Flush triggers:
//   - BatchSize signals have accumulated (default: 100)
//   - FlushInterval has elapsed (default: 500ms)
//   - The input channel is closed (graceful shutdown drain)
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const insertSQL = `
	INSERT INTO arbitrage_signals (pair, buy_exchange, sell_exchange, spread_pct, created_at)
	VALUES ($1, $2, $3, $4, $5)
`

// Batcher accumulates ArbitrageSignals and writes them in bulk using pgx.Batch.
type Batcher struct {
	pool          *pgxpool.Pool
	batchSize     int
	flushInterval time.Duration
	logger        *zap.Logger
	// flushFn is the actual flush implementation. Swappable for unit testing.
	flushFn func(ctx context.Context, signals []market.ArbitrageSignal) error
}

// NewBatcher creates a production Batcher backed by pgxpool.
func NewBatcher(pool *pgxpool.Pool, batchSize int, flushInterval time.Duration, logger *zap.Logger) *Batcher {
	b := &Batcher{
		pool:          pool,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
	}
	b.flushFn = b.pgxFlush
	return b
}

// NewBatcherWithFlushFn creates a Batcher with a custom flush function.
// Intended for use in unit tests to avoid a real database dependency.
func NewBatcherWithFlushFn(
	batchSize int,
	flushInterval time.Duration,
	logger *zap.Logger,
	flushFn func(context.Context, []market.ArbitrageSignal) error,
) *Batcher {
	return &Batcher{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
		flushFn:       flushFn,
	}
}

// Run reads from in until the channel is closed, then flushes any remaining
// buffered signals. It never returns before the final flush is complete,
// ensuring zero data loss during graceful shutdown.
func (b *Batcher) Run(ctx context.Context, in <-chan market.ArbitrageSignal) error {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	buf := make([]market.ArbitrageSignal, 0, b.batchSize)

	doFlush := func() {
		if len(buf) == 0 {
			return
		}
		// Use a fresh Background context for the final flush after cancellation,
		// so the DB write isn't aborted mid-batch.
		flushCtx := ctx
		if ctx.Err() != nil {
			flushCtx = context.Background()
		}
		if err := b.flushFn(flushCtx, buf); err != nil {
			b.logger.Error("batcher: flush error", zap.Error(err), zap.Int("dropped", len(buf)))
		} else {
			b.logger.Info("batcher: flushed batch", zap.Int("count", len(buf)))
		}
		buf = buf[:0]
	}

	for {
		select {
		case sig, ok := <-in:
			if !ok {
				// Channel closed — final drain and exit
				doFlush()
				b.logger.Info("batcher: input channel closed, shutdown flush done")
				return nil
			}
			buf = append(buf, sig)
			if len(buf) >= b.batchSize {
				doFlush()
			}

		case <-ticker.C:
			doFlush()
		}
	}
}

// pgxFlush executes a pgx.Batch containing one INSERT per signal.
// A single pgx.Batch is a single round-trip to PostgreSQL regardless of size.
func (b *Batcher) pgxFlush(ctx context.Context, signals []market.ArbitrageSignal) error {
	start := time.Now()

	batch := &pgx.Batch{}
	for _, sig := range signals {
		batch.Queue(insertSQL,
			sig.Pair,
			sig.BuyExchange,
			sig.SellExchange,
			sig.SpreadPct,
			sig.Timestamp,
		)
	}

	results := b.pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := range signals {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batcher: exec item %d: %w", i, err)
		}
	}

	b.logger.Debug("batcher: pgx batch executed",
		zap.Int("size", len(signals)),
		zap.Duration("duration", time.Since(start)),
	)

	return nil
}
