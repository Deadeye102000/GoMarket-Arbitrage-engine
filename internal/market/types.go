// Package market defines the core domain types shared across all pipeline stages.
package market

import "time"

// TickData represents a single market price tick received from the ingester.
// It carries bid/ask prices for a trading pair across two exchanges,
// representing a potential arbitrage opportunity.
type TickData struct {
	Pair         string    `json:"pair"`
	BuyExchange  string    `json:"buy_exchange"`
	SellExchange string    `json:"sell_exchange"`
	BidPrice     float64   `json:"bid_price"`
	AskPrice     float64   `json:"ask_price"`
	Timestamp    time.Time `json:"timestamp"`
}

// ArbitrageSignal is emitted by the worker pool when a tick's spread
// exceeds the configured minimum threshold. It is the unit of work
// consumed by the DB batcher and LLM analyzer.
type ArbitrageSignal struct {
	Pair         string    `json:"pair"`
	BuyExchange  string    `json:"buy_exchange"`
	SellExchange string    `json:"sell_exchange"`
	BidPrice     float64   `json:"bid_price"`
	AskPrice     float64   `json:"ask_price"`
	SpreadPct    float64   `json:"spread_pct"`
	Timestamp    time.Time `json:"timestamp"`
}

// LLMAnalysis holds the structured response from the Ollama LLM worker.
// The JSON tags match the fields requested in the system prompt.
type LLMAnalysis struct {
	Verdict    string  `json:"verdict"`    // "BUY" | "WATCH" | "IGNORE"
	Confidence float64 `json:"confidence"` // 0.0 – 1.0
	Reasoning  string  `json:"reasoning"`
}

// SignalResponse is the API-facing representation of a persisted signal.
// LLMVerdict and LLMConfidence are nil when the signal hasn't been analyzed yet.
type SignalResponse struct {
	ID            int64     `json:"id"`
	Pair          string    `json:"pair"`
	BuyExchange   string    `json:"buy_exchange"`
	SellExchange  string    `json:"sell_exchange"`
	SpreadPct     float64   `json:"spread_pct"`
	LLMVerdict    *string   `json:"llm_verdict,omitempty"`
	LLMConfidence *float64  `json:"llm_confidence,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}
