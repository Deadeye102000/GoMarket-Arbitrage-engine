// Package dedup provides signal deduplication using Redis SET NX.
// Within a configurable TTL window, identical signals (same pair, exchanges, spread)
// are suppressed to prevent redundant database writes.
package dedup

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/redis/go-redis/v9"
)

const dedupTTL = 5 * time.Second

// Deduplicator checks whether an ArbitrageSignal has been seen recently.
type Deduplicator struct {
	rdb *redis.Client
}

// New creates a Deduplicator backed by the given Redis client.
func New(rdb *redis.Client) *Deduplicator {
	return &Deduplicator{rdb: rdb}
}

// IsNovel returns (true, nil) if this signal is new (not seen in the last 5 seconds).
// Returns (false, nil) if it is a duplicate.
// Returns (false, err) if the Redis operation fails — the caller should allow
// the signal through on error rather than silently dropping it.
//
// The dedup key rounds the spread to 1 decimal place to group near-identical
// signals (e.g. 1.51% and 1.54% both map to "1.5") and avoid thundering herd
// from micro-variations in the same opportunity.
func (d *Deduplicator) IsNovel(ctx context.Context, sig market.ArbitrageSignal) (bool, error) {
	roundedSpread := math.Round(sig.SpreadPct*10) / 10
	key := fmt.Sprintf("dedup:%s:%s:%s:%.1f",
		sig.Pair, sig.BuyExchange, sig.SellExchange, roundedSpread)

	// SET NX EX — atomic: set only if key does not exist, expire after TTL
	ok, err := d.rdb.SetNX(ctx, key, 1, dedupTTL).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: redis setnx %q: %w", key, err)
	}

	return ok, nil // ok=true → key was set → signal is novel
}
