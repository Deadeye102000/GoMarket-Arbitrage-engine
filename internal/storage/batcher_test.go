package storage_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/gomarket-oss/arbitrage-engine/internal/storage"
	"go.uber.org/zap"
)

// makeSignal is a test helper that creates a minimal ArbitrageSignal.
func makeSignal(pair string, spread float64) market.ArbitrageSignal {
	return market.ArbitrageSignal{
		Pair:      pair,
		SpreadPct: spread,
		Timestamp: time.Now(),
	}
}

// TestBatcher_FlushOnSize verifies that a batch is flushed when it reaches
// the configured batch size, regardless of the ticker interval.
func TestBatcher_FlushOnSize(t *testing.T) {
	var flushedCount int32
	flushCh := make(chan int, 5)

	mockFlush := func(_ context.Context, sigs []market.ArbitrageSignal) error {
		atomic.AddInt32(&flushedCount, 1)
		flushCh <- len(sigs)
		return nil
	}

	b := storage.NewBatcherWithFlushFn(3, time.Hour, zap.NewNop(), mockFlush)

	in := make(chan market.ArbitrageSignal, 10)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, in) }()

	// Send exactly batchSize signals
	in <- makeSignal("BTC-USD", 1.5)
	in <- makeSignal("ETH-USD", 2.0)
	in <- makeSignal("SOL-USD", 1.8)

	// Wait for the size-triggered flush
	select {
	case count := <-flushCh:
		if count != 3 {
			t.Errorf("expected flush of 3 signals, got %d", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not occur within 2s")
	}

	close(in)
	<-done
}

// TestBatcher_FlushOnTicker verifies that a partial batch is flushed by the
// time-based ticker even when the batch size hasn't been reached.
func TestBatcher_FlushOnTicker(t *testing.T) {
	flushCh := make(chan int, 5)

	mockFlush := func(_ context.Context, sigs []market.ArbitrageSignal) error {
		flushCh <- len(sigs)
		return nil
	}

	b := storage.NewBatcherWithFlushFn(100, 100*time.Millisecond, zap.NewNop(), mockFlush)

	in := make(chan market.ArbitrageSignal, 10)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, in) }()

	// Send fewer signals than batchSize
	in <- makeSignal("BTC-USD", 1.5)
	in <- makeSignal("ETH-USD", 2.0)

	// The ticker (100ms) should fire and flush the 2 signals
	select {
	case count := <-flushCh:
		if count != 2 {
			t.Errorf("expected ticker flush of 2 signals, got %d", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ticker flush did not occur within 2s")
	}

	close(in)
	<-done
}

// TestBatcher_DrainOnShutdown verifies that signals buffered in the channel
// are NOT lost when the input channel is closed during a "graceful shutdown".
func TestBatcher_DrainOnShutdown(t *testing.T) {
	var totalFlushed int32

	mockFlush := func(_ context.Context, sigs []market.ArbitrageSignal) error {
		atomic.AddInt32(&totalFlushed, int32(len(sigs)))
		return nil
	}

	// Large batchSize and large interval — flush will only trigger on channel close
	b := storage.NewBatcherWithFlushFn(100, time.Hour, zap.NewNop(), mockFlush)

	in := make(chan market.ArbitrageSignal, 50)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, in) }()

	// Send 7 signals (less than batchSize=100)
	for i := 0; i < 7; i++ {
		in <- makeSignal("XRP-USD", 1.1)
	}

	// Closing the channel triggers the final flush
	close(in)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("batcher returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batcher did not exit within 5s after channel close")
	}

	if got := atomic.LoadInt32(&totalFlushed); got != 7 {
		t.Errorf("expected 7 signals flushed on shutdown, got %d", got)
	}
}

// TestBatcher_FlushError_ContinuesRunning verifies that a flush error does
// not crash the batcher — it logs and continues.
func TestBatcher_FlushError_ContinuesRunning(t *testing.T) {
	calls := 0
	mockFlush := func(_ context.Context, sigs []market.ArbitrageSignal) error {
		calls++
		if calls == 1 {
			return errors.New("transient db error")
		}
		return nil
	}

	b := storage.NewBatcherWithFlushFn(1, time.Hour, zap.NewNop(), mockFlush)

	in := make(chan market.ArbitrageSignal, 10)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, in) }()

	in <- makeSignal("BNB-USD", 1.5) // triggers error flush (calls==1)
	time.Sleep(50 * time.Millisecond)
	in <- makeSignal("BNB-USD", 2.0) // triggers success flush (calls==2)

	close(in)
	if err := <-done; err != nil {
		t.Errorf("batcher should not return error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 flush calls, got %d", calls)
	}
}
