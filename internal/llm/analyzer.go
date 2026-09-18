// Package llm provides LLM-powered analysis of arbitrage signals using Ollama.
// It polls the database for unanalyzed signals every 5 seconds and calls
// Ollama's OpenAI-compatible API to generate a structured JSON verdict.
//
// The go-openai SDK is pointed at Ollama's /v1 endpoint, which is fully
// API-compatible — no API key required, zero cost, runs offline.
package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomarket-oss/arbitrage-engine/internal/config"
	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"github.com/jackc/pgx/v5/pgxpool"
	openai "github.com/sashabaranov/go-openai"
	"go.uber.org/zap"
)

const (
	// pollInterval controls how often the LLM worker picks a new signal to analyze.
	pollInterval = 5 * time.Second

	// llmTimeout is the per-call timeout for Ollama inference.
	// llama3.2 typically responds in 2–10s depending on hardware.
	llmTimeout = 30 * time.Second

	systemPrompt = `You are a crypto arbitrage analyst. Analyze the provided signal and respond
with ONLY a valid JSON object matching this exact schema:
{
  "verdict":    "BUY" | "WATCH" | "IGNORE",
  "confidence": <float 0.0-1.0>,
  "reasoning":  "<max 80 words>"
}

Rules:
- BUY:    spread likely exploitable after fees (~0.5% per leg)
- WATCH:  marginal; monitor for confirmation
- IGNORE: spread too thin or exchange combination is illiquid`
)

// Analyzer queries the database for unanalyzed signals and enriches them
// with LLM-generated verdicts stored back in PostgreSQL.
type Analyzer struct {
	client *openai.Client
	pool   *pgxpool.Pool
	cfg    *config.Config
	logger *zap.Logger
}

// New creates an Analyzer that targets the Ollama API at cfg.OllamaBaseURL.
func New(cfg *config.Config, pool *pgxpool.Pool, logger *zap.Logger) *Analyzer {
	// go-openai SDK → Ollama: just change the BaseURL. The API is identical.
	clientCfg := openai.DefaultConfig("ollama") // API key field is required by SDK but ignored by Ollama
	clientCfg.BaseURL = cfg.OllamaBaseURL

	return &Analyzer{
		client: openai.NewClientWithConfig(clientCfg),
		pool:   pool,
		cfg:    cfg,
		logger: logger,
	}
}

// Run starts the polling loop. It blocks until ctx is cancelled.
func (a *Analyzer) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	a.logger.Info("llm analyzer: started",
		zap.String("model", a.cfg.OllamaModel),
		zap.String("endpoint", a.cfg.OllamaBaseURL),
		zap.Duration("poll_interval", pollInterval),
	)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("llm analyzer: shutting down")
			return nil
		case <-ticker.C:
			if err := a.analyzeOne(ctx); err != nil {
				// Log but don't crash — Ollama may be slow or temporarily unavailable
				a.logger.Warn("llm analyzer: analysis skipped", zap.Error(err))
			}
		}
	}
}

// analyzeOne fetches the most recent unanalyzed signal, calls Ollama,
// and writes the verdict back to the database.
func (a *Analyzer) analyzeOne(ctx context.Context) error {
	var id int64
	var sig market.ArbitrageSignal

	err := a.pool.QueryRow(ctx, `
		SELECT id, pair, buy_exchange, sell_exchange, spread_pct
		FROM   arbitrage_signals
		WHERE  llm_verdict IS NULL
		ORDER  BY created_at DESC
		LIMIT  1
	`).Scan(&id, &sig.Pair, &sig.BuyExchange, &sig.SellExchange, &sig.SpreadPct)
	if err != nil {
		return fmt.Errorf("no unanalyzed signals available: %w", err)
	}

	analysis, err := a.callOllama(ctx, sig)
	if err != nil {
		return fmt.Errorf("ollama call failed: %w", err)
	}

	_, err = a.pool.Exec(ctx, `
		UPDATE arbitrage_signals
		SET    llm_verdict = $1, llm_confidence = $2
		WHERE  id = $3
	`, analysis.Verdict, analysis.Confidence, id)
	if err != nil {
		return fmt.Errorf("update signal %d: %w", id, err)
	}

	a.logger.Info("llm analyzer: signal analyzed",
		zap.Int64("id", id),
		zap.String("pair", sig.Pair),
		zap.Float64("spread_pct", sig.SpreadPct),
		zap.String("verdict", analysis.Verdict),
		zap.Float64("confidence", analysis.Confidence),
	)

	return nil
}

// callOllama sends a structured prompt to Ollama and parses the JSON response.
func (a *Analyzer) callOllama(ctx context.Context, sig market.ArbitrageSignal) (*market.LLMAnalysis, error) {
	prompt := fmt.Sprintf(
		"Pair: %s | Buy on: %s | Sell on: %s | Spread: %.4f%%",
		sig.Pair, sig.BuyExchange, sig.SellExchange, sig.SpreadPct,
	)

	callCtx, cancel := context.WithTimeout(ctx, llmTimeout)
	defer cancel()

	resp, err := a.client.CreateChatCompletion(callCtx, openai.ChatCompletionRequest{
		Model: a.cfg.OllamaModel,
		Messages: []openai.ChatCompletionMessage{
			{Role: openai.ChatMessageRoleSystem, Content: systemPrompt},
			{Role: openai.ChatMessageRoleUser, Content: prompt},
		},
		ResponseFormat: &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONObject,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("chat completion: %w", err)
	}

	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("empty response from model")
	}

	var analysis market.LLMAnalysis
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &analysis); err != nil {
		return nil, fmt.Errorf("unmarshal llm response: %w", err)
	}

	return &analysis, nil
}

// AnalyzeOnDemand performs an immediate LLM analysis without hitting the database.
// Used by the POST /analyze API endpoint.
func (a *Analyzer) AnalyzeOnDemand(ctx context.Context, sig market.ArbitrageSignal) (*market.LLMAnalysis, error) {
	return a.callOllama(ctx, sig)
}
