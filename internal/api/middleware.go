// Package api provides the Gin-based HTTP API for the engine.
// It exposes health, signal query, and on-demand LLM analysis endpoints.
package api

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// RequestLogger returns a Gin middleware that logs each HTTP request
// using the engine's structured zap logger.
func RequestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("ip", c.ClientIP()),
			zap.Int("bytes", c.Writer.Size()),
		}
		if query != "" {
			fields = append(fields, zap.String("query", query))
		}
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		logger.Info("http", fields...)
	}
}
