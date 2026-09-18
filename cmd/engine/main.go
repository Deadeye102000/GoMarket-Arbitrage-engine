// GoMarket Arbitrage Engine
//
// A production-grade high-frequency arbitrage signal pipeline demonstrating:
//   - Goroutines, channels, and buffered backpressure
//   - pgx connection pooling and batch inserts
//   - Kafka-driven event ingestion (Redpanda)
//   - Redis signal deduplication
//   - Local LLM analysis via Ollama (OpenAI-compatible API)
//   - Gin REST API with structured logging
//   - OpenTelemetry distributed tracing
//   - Graceful shutdown with context propagation and WaitGroup drain
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/api"
	"github.com/gomarket-oss/arbitrage-engine/internal/config"
	"github.com/gomarket-oss/arbitrage-engine/internal/dedup"
	"github.com/gomarket-oss/arbitrage-engine/internal/ingester"
	"github.com/gomarket-oss/arbitrage-engine/internal/llm"
	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/gomarket-oss/arbitrage-engine/internal/storage"
	"github.com/gomarket-oss/arbitrage-engine/internal/telemetry"
	"github.com/gomarket-oss/arbitrage-engine/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// run is the actual entry point. Returning an error from run() (not main)
// avoids calling os.Exit before deferred cleanup runs.
func run() error {
	// ── Environment ──────────────────────────────────────────────────────────
	// Load .env file in development. In production (Docker/K8s), env vars are
	// injected directly — the missing file is silently ignored.
	_ = godotenv.Load()

	// ── Logger ───────────────────────────────────────────────────────────────
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("logger init: %w", err)
	}
	defer logger.Sync() //nolint:errcheck

	// ── Config ───────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger.Info("gomarket arbitrage engine starting",
		zap.Int("workers", cfg.WorkerCount),
		zap.Int("channel_size", cfg.ChannelSize),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Duration("flush_interval", cfg.FlushInterval),
		zap.Float64("min_spread_pct", cfg.MinSpreadPct),
		zap.Bool("kafka_enabled", cfg.KafkaEnabled),
		zap.Bool("llm_enabled", cfg.LLMEnabled),
	)

	// ── OpenTelemetry ────────────────────────────────────────────────────────
	otelShutdown, err := telemetry.Init("gomarket-arbitrage-engine")
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}

	// ── Root Context (signal-aware) ───────────────────────────────────────────
	// signal.NotifyContext cancels ctx on SIGINT or SIGTERM.
	// This is the primary mechanism for Kubernetes pod eviction handling.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── PostgreSQL ───────────────────────────────────────────────────────────
	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}
	logger.Info("postgresql connected", zap.String("dsn_host", extractHost(cfg.DatabaseDSN)))

	// ── Redis ────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	logger.Info("redis connected", zap.String("addr", cfg.RedisAddr))

	// ── Channels ─────────────────────────────────────────────────────────────
	// Sized channels provide explicit backpressure:
	// - If workers are slow, tickCh fills and the ingester drops ticks (logged).
	// - If the DB is slow, sigCh fills and workers drop signals (logged).
	tickCh := make(chan market.TickData, cfg.ChannelSize)
	sigCh := make(chan market.ArbitrageSignal, cfg.ChannelSize/2)

	// ── Component Init ────────────────────────────────────────────────────────
	deduplicator := dedup.New(rdb)
	workerPool := worker.New(cfg.WorkerCount, cfg.MinSpreadPct, logger, deduplicator.IsNovel)
	batcher := storage.NewBatcher(pool, cfg.BatchSize, cfg.FlushInterval, logger)

	var analyzer *llm.Analyzer
	if cfg.LLMEnabled {
		analyzer = llm.New(cfg, pool, logger)
		logger.Info("llm analyzer enabled",
			zap.String("model", cfg.OllamaModel),
			zap.String("endpoint", cfg.OllamaBaseURL),
		)
	}

	srv := api.New(cfg, pool, rdb, analyzer, logger)

	// ── Goroutine Orchestration ───────────────────────────────────────────────
	// WaitGroup tracks every goroutine. Shutdown blocks until all are done
	// or the hard 10-second timeout fires.
	var wg sync.WaitGroup

	// 1. HTTP API server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server: unexpected exit", zap.Error(err))
		}
	}()

	// 2. Ingester: generates/consumes ticks → tickCh
	//    defer close(tickCh) propagates the shutdown signal down the pipeline.
	ingest := ingester.New(cfg, logger, tickCh)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tickCh) // ← triggers worker pool drain
		if err := ingest.Run(ctx); err != nil {
			logger.Error("ingester: error", zap.Error(err))
		}
		logger.Info("ingester: stopped")
	}()

	// 3. Worker pool: tickCh → sigCh
	workerPool.Start(ctx, tickCh, sigCh)

	// 4. Pool watcher: closes sigCh once all workers are done
	//    This triggers the batcher's final drain.
	wg.Add(1)
	go func() {
		defer wg.Done()
		workerPool.Wait()
		close(sigCh) // ← triggers batcher drain
		logger.Info("worker pool: all workers done, signal channel closed")
	}()

	// 5. DB Batcher: sigCh → PostgreSQL
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := batcher.Run(ctx, sigCh); err != nil {
			logger.Error("batcher: error", zap.Error(err))
		}
		logger.Info("batcher: stopped")
	}()

	// 6. LLM Analyzer (optional)
	if analyzer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := analyzer.Run(ctx); err != nil {
				logger.Error("llm analyzer: error", zap.Error(err))
			}
			logger.Info("llm analyzer: stopped")
		}()
	}

	logger.Info("🚀 engine running — press Ctrl+C to stop")

	// ── Wait for Shutdown Signal ──────────────────────────────────────────────
	<-ctx.Done()
	logger.Info("shutdown signal received, draining pipeline...",
		zap.String("signal", ctx.Err().Error()),
	)

	// Hard deadline: if goroutines haven't finished in 10s, force exit
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Shut down the HTTP server (stops accepting new requests, drains in-flight)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("api server: shutdown error", zap.Error(err))
	}

	// Wait for all pipeline goroutines with the hard timeout
	drainDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		logger.Info("✓ graceful shutdown complete")
	case <-shutdownCtx.Done():
		logger.Warn("⚠ shutdown timeout exceeded — forcing exit",
			zap.Duration("timeout", shutdownTimeout),
		)
	}

	// Flush OTel spans before exit
	if err := otelShutdown(context.Background()); err != nil {
		logger.Error("otel: shutdown error", zap.Error(err))
	}

	return nil
}

// extractHost pulls the host portion from a postgres DSN for safe logging
// (avoids logging credentials).
func extractHost(dsn string) string {
	// postgres://user:pass@HOST:PORT/db?...
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '@' {
			rest := dsn[i+1:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == '/' {
					return rest[:j]
				}
			}
			return rest
		}
	}
	return "unknown"
}
