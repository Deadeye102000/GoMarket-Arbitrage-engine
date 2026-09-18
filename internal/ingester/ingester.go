// Package ingester simulates a WebSocket market data feed by generating
// randomised TickData and publishing it either to a Kafka topic (production)
// or directly to a Go channel (KAFKA_ENABLED=false, local dev).
//
// At ~1,000 ticks/second across 5 crypto pairs and 3 exchanges, the ingester
// creates a realistic high-frequency data stream for the worker pool to process.
package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/config"
	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	pairs     = []string{"BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD"}
	exchanges = []string{"Binance", "Coinbase", "Kraken"}

	// Base reference prices — realistic starting points for price simulation
	basePrices = map[string]float64{
		"BTC-USD": 65000.0,
		"ETH-USD": 3500.0,
		"SOL-USD": 180.0,
		"BNB-USD": 580.0,
		"XRP-USD": 0.62,
	}
)

// Ingester generates market tick data and pushes it to the pipeline.
type Ingester struct {
	cfg    *config.Config
	logger *zap.Logger
	out    chan<- market.TickData
}

// New creates an Ingester that writes to out.
func New(cfg *config.Config, logger *zap.Logger, out chan<- market.TickData) *Ingester {
	return &Ingester{cfg: cfg, logger: logger, out: out}
}

// Run starts the ingestion loop. It blocks until ctx is cancelled.
// The caller is responsible for closing the out channel after Run returns.
func (i *Ingester) Run(ctx context.Context) error {
	if i.cfg.KafkaEnabled {
		return i.runKafka(ctx)
	}
	return i.runDirect(ctx)
}

// runDirect bypasses Kafka and pushes ticks straight to the channel.
// Used when KAFKA_ENABLED=false for local development.
func (i *Ingester) runDirect(ctx context.Context) error {
	i.logger.Info("ingester: running in direct channel mode (KAFKA_ENABLED=false)")

	ticker := time.NewTicker(time.Millisecond) // 1,000 ticks/sec
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			i.logger.Info("ingester: context cancelled, stopping")
			return nil
		case <-ticker.C:
			tick := generateTick()
			select {
			case i.out <- tick:
			default:
				// Channel is full — backpressure applied. Drop the tick rather
				// than blocking, which would stall the entire ingestion loop.
				i.logger.Warn("ingester: tick channel full, dropping tick (backpressure)")
			}
		}
	}
}

// runKafka starts both a Kafka producer (goroutine) and a consumer (blocking).
func (i *Ingester) runKafka(ctx context.Context) error {
	i.logger.Info("ingester: running in Kafka mode",
		zap.Strings("brokers", i.cfg.KafkaBrokers),
		zap.String("topic", i.cfg.KafkaTopic),
	)

	go i.produce(ctx)
	return i.consume(ctx)
}

// produce publishes 1,000 randomised ticks/sec to the Kafka topic.
func (i *Ingester) produce(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(i.cfg.KafkaBrokers...),
		Topic:        i.cfg.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true, // fire-and-forget for maximum throughput
	}
	defer func() {
		if err := writer.Close(); err != nil {
			i.logger.Warn("ingester: kafka writer close error", zap.Error(err))
		}
	}()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			i.logger.Info("ingester: kafka producer shutting down")
			return
		case <-ticker.C:
			tick := generateTick()
			data, err := json.Marshal(tick)
			if err != nil {
				i.logger.Error("ingester: marshal tick", zap.Error(err))
				continue
			}
			if err := writer.WriteMessages(ctx, kafka.Message{Value: data}); err != nil {
				if ctx.Err() != nil {
					return
				}
				i.logger.Warn("ingester: write to kafka failed", zap.Error(err))
			}
		}
	}
}

// consume reads from the Kafka topic and forwards ticks to the out channel.
func (i *Ingester) consume(ctx context.Context) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     i.cfg.KafkaBrokers,
		Topic:       i.cfg.KafkaTopic,
		GroupID:     "gomarket-engine",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			i.logger.Warn("ingester: kafka reader close error", zap.Error(err))
		}
	}()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				i.logger.Info("ingester: kafka consumer shutting down")
				return nil
			}
			return fmt.Errorf("ingester: fetch message: %w", err)
		}

		var tick market.TickData
		if err := json.Unmarshal(msg.Value, &tick); err != nil {
			i.logger.Warn("ingester: unmarshal tick, skipping", zap.Error(err))
		} else {
			select {
			case i.out <- tick:
			case <-ctx.Done():
				return nil
			default:
				i.logger.Warn("ingester: tick channel full, dropping (backpressure)")
			}
		}

		// Always commit the offset, even if we dropped the tick
		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			i.logger.Error("ingester: commit message failed", zap.Error(err))
		}
	}
}

// generateTick produces a single realistic TickData with:
//   - a random pair and exchange combination
//   - bid price varying ±2% from a base price
//   - ask price 0–3% above bid (creating spreads that sometimes exceed 1%)
func generateTick() market.TickData {
	pair := pairs[rand.Intn(len(pairs))]
	buyEx := exchanges[rand.Intn(len(exchanges))]
	sellEx := exchanges[rand.Intn(len(exchanges))]

	base := basePrices[pair]
	bid := base * (1 + (rand.Float64()*0.04 - 0.02)) // ±2% of base
	ask := bid * (1 + rand.Float64()*0.03)            // ask is 0%–3% above bid

	return market.TickData{
		Pair:         pair,
		BuyExchange:  buyEx,
		SellExchange: sellEx,
		BidPrice:     bid,
		AskPrice:     ask,
		Timestamp:    time.Now().UTC(),
	}
}
