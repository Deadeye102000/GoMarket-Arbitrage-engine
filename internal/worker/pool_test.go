package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/gomarket-oss/arbitrage-engine/internal/worker"
	"go.uber.org/zap"
)

// ── CalculateSpread unit tests ────────────────────────────────────────────────

func TestCalculateSpread_Positive(t *testing.T) {
	// 102 - 100 = 2 → 2/100 * 100 = 2.0%
	got := worker.CalculateSpread(100.0, 102.0)
	if got != 2.0 {
		t.Errorf("expected 2.0, got %.4f", got)
	}
}

func TestCalculateSpread_ZeroBid_ReturnsZero(t *testing.T) {
	// Division by zero guard
	got := worker.CalculateSpread(0, 100.0)
	if got != 0 {
		t.Errorf("expected 0 for zero bid, got %.4f", got)
	}
}

func TestCalculateSpread_NegativeBid_ReturnsZero(t *testing.T) {
	got := worker.CalculateSpread(-50.0, 100.0)
	if got != 0 {
		t.Errorf("expected 0 for negative bid, got %.4f", got)
	}
}

func TestCalculateSpread_ExactThreshold(t *testing.T) {
	// Exactly 1.0% — boundary value test
	got := worker.CalculateSpread(100.0, 101.0)
	if got != 1.0 {
		t.Errorf("expected 1.0, got %.6f", got)
	}
}

func TestCalculateSpread_SmallPrice(t *testing.T) {
	// XRP-like prices: 0.62 bid, spread should still calculate correctly
	got := worker.CalculateSpread(0.62, 0.6262)
	want := ((0.6262 - 0.62) / 0.62) * 100
	if abs(got-want) > 1e-9 {
		t.Errorf("expected %.6f, got %.6f", want, got)
	}
}

// ── Pool filtering tests ──────────────────────────────────────────────────────

func newTestPool(minSpread float64) *worker.Pool {
	return worker.New(1, minSpread, zap.NewNop(), nil /* no dedup */)
}

func TestPool_FiltersLowSpread(t *testing.T) {
	pool := newTestPool(1.0)

	inCh := make(chan market.TickData, 10)
	outCh := make(chan market.ArbitrageSignal, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx, inCh, outCh)

	// 0.5% spread — should be filtered
	inCh <- market.TickData{
		Pair:      "BTC-USD",
		BidPrice:  100.0,
		AskPrice:  100.5,
		Timestamp: time.Now(),
	}

	close(inCh)
	pool.Wait()

	if len(outCh) != 0 {
		t.Errorf("expected 0 signals for sub-threshold spread, got %d", len(outCh))
	}
}

func TestPool_EmitsHighSpread(t *testing.T) {
	pool := newTestPool(1.0)

	inCh := make(chan market.TickData, 10)
	outCh := make(chan market.ArbitrageSignal, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx, inCh, outCh)

	// 2.0% spread — should pass
	inCh <- market.TickData{
		Pair:         "ETH-USD",
		BuyExchange:  "Kraken",
		SellExchange: "Binance",
		BidPrice:     1000.0,
		AskPrice:     1020.0,
		Timestamp:    time.Now(),
	}

	close(inCh)
	pool.Wait()

	if len(outCh) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(outCh))
	}

	sig := <-outCh
	if sig.SpreadPct != 2.0 {
		t.Errorf("expected spread 2.0, got %.6f", sig.SpreadPct)
	}
	if sig.Pair != "ETH-USD" {
		t.Errorf("expected ETH-USD, got %s", sig.Pair)
	}
}

func TestPool_MultipleWorkers_ProcessAll(t *testing.T) {
	pool := worker.New(5, 1.0, zap.NewNop(), nil)

	const tickCount = 50
	inCh := make(chan market.TickData, tickCount)
	outCh := make(chan market.ArbitrageSignal, tickCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx, inCh, outCh)

	// All ticks have 2% spread — all should produce signals
	for i := 0; i < tickCount; i++ {
		inCh <- market.TickData{
			Pair:      "SOL-USD",
			BidPrice:  100.0,
			AskPrice:  102.0,
			Timestamp: time.Now(),
		}
	}

	close(inCh)
	pool.Wait()

	if len(outCh) != tickCount {
		t.Errorf("expected %d signals, got %d", tickCount, len(outCh))
	}
}

func TestPool_DedupFunc_FiltersDuplicates(t *testing.T) {
	callCount := 0
	// Dedup: only first call is "novel"
	dedup := func(ctx context.Context, sig market.ArbitrageSignal) (bool, error) {
		callCount++
		return callCount == 1, nil
	}

	pool := worker.New(1, 1.0, zap.NewNop(), dedup)

	inCh := make(chan market.TickData, 10)
	outCh := make(chan market.ArbitrageSignal, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx, inCh, outCh)

	for i := 0; i < 3; i++ {
		inCh <- market.TickData{BidPrice: 100.0, AskPrice: 102.0, Timestamp: time.Now()}
	}

	close(inCh)
	pool.Wait()

	if len(outCh) != 1 {
		t.Errorf("expected 1 non-duplicate signal, got %d", len(outCh))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
