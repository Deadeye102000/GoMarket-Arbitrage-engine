package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gomarket-oss/arbitrage-engine/internal/config"
	"github.com/gomarket-oss/arbitrage-engine/internal/llm"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Server holds all dependencies required by the HTTP handlers.
type Server struct {
	cfg     *config.Config
	pool    *pgxpool.Pool
	rdb     *redis.Client
	llm     *llm.Analyzer // nil when LLM_ENABLED=false
	logger  *zap.Logger
	httpSrv *http.Server
}

// New creates and configures a Server with all routes registered.
func New(cfg *config.Config, pool *pgxpool.Pool, rdb *redis.Client, analyzer *llm.Analyzer, logger *zap.Logger) *Server {
	s := &Server{
		cfg:    cfg,
		pool:   pool,
		rdb:    rdb,
		llm:    analyzer,
		logger: logger,
	}
	s.httpSrv = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      s.setupRouter(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

func (s *Server) setupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(RequestLogger(s.logger))
	r.Use(gin.Recovery()) // prevents panics from crashing the server

	r.GET("/healthz", s.handleHealthz)
	r.GET("/signals", s.handleGetSignals)
	r.POST("/analyze", s.handleAnalyze)

	return r
}

// Start begins accepting HTTP connections. It blocks until the server is stopped.
// Returns http.ErrServerClosed on graceful shutdown (not treated as an error by main).
func (s *Server) Start() error {
	s.logger.Info("api server: listening", zap.String("addr", s.httpSrv.Addr))
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests within the provided context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("api server: shutting down")
	return s.httpSrv.Shutdown(ctx)
}
