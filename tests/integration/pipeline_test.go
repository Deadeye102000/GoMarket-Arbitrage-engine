// Package integration contains containerized integration tests.
// Tests spin up real Postgres and Redis containers via testcontainers-go,
// run migrations, and exercise the full pipeline end-to-end.
//
// Run with: go test -v -timeout 120s ./tests/integration/...
// Skip in short mode: go test -short ./...
package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/gomarket-oss/arbitrage-engine/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

// ── Container helpers ─────────────────────────────────────────────────────────

func startPostgres(ctx context.Context, t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	dsn = fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	return dsn, func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("warning: terminate postgres container: %v", err)
		}
	}
}

// applySchema runs the DDL directly (avoids needing golang-migrate binary in CI).
func applySchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS arbitrage_signals (
			id              BIGSERIAL       PRIMARY KEY,
			pair            VARCHAR(20)     NOT NULL,
			buy_exchange    VARCHAR(50)     NOT NULL,
			sell_exchange   VARCHAR(50)     NOT NULL,
			spread_pct      NUMERIC(10, 6)  NOT NULL,
			llm_verdict     VARCHAR(10),
			llm_confidence  NUMERIC(4, 3),
			created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		t.Fatalf("apply schema: %v", err)
	}
}

func makePool(ctx context.Context, t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	return pool
}

// ── Tests ────────────────────────────────────────────────────────────────────

// TestPipeline_SignalWrittenToDB verifies that signals sent through the Batcher
// are persisted to PostgreSQL exactly once.
func TestPipeline_SignalWrittenToDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (-short)")
	}

	ctx := context.Background()
	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

	pool := makePool(ctx, t, dsn)
	defer pool.Close()

	applySchema(ctx, t, pool)

	sigCh := make(chan market.ArbitrageSignal, 50)
	batcher := storage.NewBatcher(pool, 10, 200*time.Millisecond, zap.NewNop())

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- batcher.Run(runCtx, sigCh) }()

	const signalCount = 5
	for i := 0; i < signalCount; i++ {
		sigCh <- market.ArbitrageSignal{
			Pair:         "BTC-USD",
			BuyExchange:  "Binance",
			SellExchange: "Coinbase",
			SpreadPct:    1.5 + float64(i)*0.1,
			Timestamp:    time.Now(),
		}
	}

	close(sigCh)

	if err := <-done; err != nil {
		t.Fatalf("batcher returned error: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM arbitrage_signals").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}

	if count != signalCount {
		t.Errorf("expected %d signals in DB, got %d", signalCount, count)
	}
}

// TestPipeline_BatchFlush_100Signals verifies that exactly 100 signals trigger
// a flush (batchSize boundary) and all are written to the database.
func TestPipeline_BatchFlush_100Signals(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (-short)")
	}

	ctx := context.Background()
	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

	pool := makePool(ctx, t, dsn)
	defer pool.Close()
	applySchema(ctx, t, pool)

	sigCh := make(chan market.ArbitrageSignal, 200)
	batcher := storage.NewBatcher(pool, 100, time.Hour, zap.NewNop())

	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- batcher.Run(runCtx, sigCh) }()

	const signalCount = 100
	for i := 0; i < signalCount; i++ {
		sigCh <- market.ArbitrageSignal{
			Pair:      "ETH-USD",
			SpreadPct: 2.0,
			Timestamp: time.Now(),
		}
	}

	// Give the batcher time to flush on size trigger
	time.Sleep(500 * time.Millisecond)

	var count int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM arbitrage_signals").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != signalCount {
		t.Errorf("expected %d signals, got %d (flush-on-size may not have triggered)", signalCount, count)
	}

	close(sigCh)
	<-done
}

// TestPipeline_GracefulShutdown_NoDataLoss verifies that in-flight signals
// are not lost when the batcher is shut down by closing the channel.
func TestPipeline_GracefulShutdown_NoDataLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test (-short)")
	}

	ctx := context.Background()
	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

	pool := makePool(ctx, t, dsn)
	defer pool.Close()
	applySchema(ctx, t, pool)

	// Large batchSize and interval so only channel-close triggers the flush
	sigCh := make(chan market.ArbitrageSignal, 50)
	batcher := storage.NewBatcher(pool, 1000, time.Hour, zap.NewNop())

	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- batcher.Run(runCtx, sigCh) }()

	const signalCount = 17
	for i := 0; i < signalCount; i++ {
		sigCh <- market.ArbitrageSignal{
			Pair:      "SOL-USD",
			SpreadPct: 1.3,
			Timestamp: time.Now(),
		}
	}

	close(sigCh) // triggers final drain flush

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("batcher error on shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batcher did not complete within 5s after channel close")
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM arbitrage_signals").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != signalCount {
		t.Errorf("data loss on shutdown: expected %d, got %d", signalCount, count)
	}
}
