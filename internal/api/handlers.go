package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gomarket-oss/arbitrage-engine/internal/market"
	"go.uber.org/zap"
)

// handleHealthz checks liveness of downstream dependencies.
// Used as Kubernetes liveness and readiness probe target.
//
// GET /healthz
// Response 200: {"status":"ok","db":"connected","redis":"connected"}
// Response 503: {"status":"degraded","db":"disconnected",...}
func (s *Server) handleHealthz(c *gin.Context) {
	status := gin.H{"status": "ok"}
	code := http.StatusOK

	if err := s.pool.Ping(c.Request.Context()); err != nil {
		s.logger.Error("healthz: db ping failed", zap.Error(err))
		status["db"] = "disconnected"
		status["status"] = "degraded"
		code = http.StatusServiceUnavailable
	} else {
		status["db"] = "connected"
	}

	if err := s.rdb.Ping(c.Request.Context()).Err(); err != nil {
		s.logger.Error("healthz: redis ping failed", zap.Error(err))
		status["redis"] = "disconnected"
		status["status"] = "degraded"
		code = http.StatusServiceUnavailable
	} else {
		status["redis"] = "connected"
	}

	c.JSON(code, status)
}

// handleGetSignals returns recent arbitrage signals from the database.
// Supports optional filtering by pair, with pagination.
//
// GET /signals?pair=BTC-USD&limit=50&offset=0
func (s *Server) handleGetSignals(c *gin.Context) {
	pair := c.Query("pair")

	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit <= 0 || limit > 500 {
		limit = 50
	}
	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || offset < 0 {
		offset = 0
	}

	var (
		query string
		args  []any
	)

	if pair != "" {
		query = `
			SELECT id, pair, buy_exchange, sell_exchange,
			       spread_pct, llm_verdict, llm_confidence, created_at
			FROM   arbitrage_signals
			WHERE  pair = $1
			ORDER  BY created_at DESC
			LIMIT  $2 OFFSET $3`
		args = []any{pair, limit, offset}
	} else {
		query = `
			SELECT id, pair, buy_exchange, sell_exchange,
			       spread_pct, llm_verdict, llm_confidence, created_at
			FROM   arbitrage_signals
			ORDER  BY created_at DESC
			LIMIT  $1 OFFSET $2`
		args = []any{limit, offset}
	}

	rows, err := s.pool.Query(c.Request.Context(), query, args...)
	if err != nil {
		s.logger.Error("signals: query failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query signals"})
		return
	}
	defer rows.Close()

	signals := make([]market.SignalResponse, 0, limit)
	for rows.Next() {
		var sig market.SignalResponse
		if err := rows.Scan(
			&sig.ID,
			&sig.Pair,
			&sig.BuyExchange,
			&sig.SellExchange,
			&sig.SpreadPct,
			&sig.LLMVerdict,    // *string — pgx handles NULL → nil
			&sig.LLMConfidence, // *float64 — pgx handles NULL → nil
			&sig.CreatedAt,
		); err != nil {
			s.logger.Error("signals: row scan failed", zap.Error(err))
			continue
		}
		signals = append(signals, sig)
	}
	if err := rows.Err(); err != nil {
		s.logger.Error("signals: rows iteration error", zap.Error(err))
	}

	c.JSON(http.StatusOK, gin.H{
		"signals": signals,
		"count":   len(signals),
		"limit":   limit,
		"offset":  offset,
	})
}

// handleAnalyze performs an on-demand LLM analysis of a provided signal.
// The signal is NOT persisted — this endpoint is for interactive exploration.
//
// POST /analyze
// Body: {"pair":"BTC-USD","buy_exchange":"Binance","sell_exchange":"Coinbase","spread_pct":2.5}
// Response: {"verdict":"BUY","confidence":0.82,"reasoning":"..."}
func (s *Server) handleAnalyze(c *gin.Context) {
	if s.llm == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "LLM analyzer not configured (set LLM_ENABLED=true and ensure Ollama is running)",
		})
		return
	}

	var req market.ArbitrageSignal
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Pair == "" || req.BuyExchange == "" || req.SellExchange == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "pair, buy_exchange, sell_exchange, and spread_pct are required",
		})
		return
	}

	analysis, err := s.llm.AnalyzeOnDemand(c.Request.Context(), req)
	if err != nil {
		s.logger.Error("analyze: llm call failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "LLM analysis failed — is Ollama running?"})
		return
	}

	c.JSON(http.StatusOK, analysis)
}
