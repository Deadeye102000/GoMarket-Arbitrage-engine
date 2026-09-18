// Package config loads and validates all engine configuration from environment variables.
// All settings have sensible defaults so the engine works out of the box with docker compose.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the engine.
type Config struct {
	// Database
	DatabaseDSN string

	// Redis
	RedisAddr string

	// Kafka / Redpanda
	KafkaBrokers []string
	KafkaTopic   string
	KafkaEnabled bool

	// LLM (Ollama)
	OllamaBaseURL string
	OllamaModel   string
	LLMEnabled    bool
	LLMSampleRate int // analyze 1 out of every N signals

	// Pipeline
	WorkerCount   int
	ChannelSize   int
	BatchSize     int
	FlushInterval time.Duration
	MinSpreadPct  float64

	// Server
	Port int

	// Observability
	OTelEndpoint string // "stdout" or OTLP endpoint URL
}

// Load reads configuration from environment variables.
// Returns an error if any required variable has an invalid value.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseDSN:   getEnv("DATABASE_DSN", "postgres://gomarket:gomarket@localhost:5432/gomarket?sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "market.ticks"),
		OllamaBaseURL: getEnv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
		OllamaModel:   getEnv("OLLAMA_MODEL", "llama3.2"),
		OTelEndpoint:  getEnv("OTEL_ENDPOINT", "stdout"),
	}

	// Parse Kafka brokers (comma-separated list)
	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:19092")
	cfg.KafkaBrokers = strings.Split(brokersRaw, ",")
	for i := range cfg.KafkaBrokers {
		cfg.KafkaBrokers[i] = strings.TrimSpace(cfg.KafkaBrokers[i])
	}

	cfg.KafkaEnabled = getEnvBool("KAFKA_ENABLED", true)
	cfg.LLMEnabled = getEnvBool("LLM_ENABLED", true)

	var err error

	if cfg.WorkerCount, err = getEnvInt("WORKER_COUNT", 10); err != nil {
		return nil, fmt.Errorf("config: WORKER_COUNT: %w", err)
	}
	if cfg.ChannelSize, err = getEnvInt("CHANNEL_SIZE", 500); err != nil {
		return nil, fmt.Errorf("config: CHANNEL_SIZE: %w", err)
	}
	if cfg.BatchSize, err = getEnvInt("BATCH_SIZE", 100); err != nil {
		return nil, fmt.Errorf("config: BATCH_SIZE: %w", err)
	}
	if cfg.LLMSampleRate, err = getEnvInt("LLM_SAMPLE_RATE", 10); err != nil {
		return nil, fmt.Errorf("config: LLM_SAMPLE_RATE: %w", err)
	}
	if cfg.Port, err = getEnvInt("PORT", 8080); err != nil {
		return nil, fmt.Errorf("config: PORT: %w", err)
	}

	flushStr := getEnv("FLUSH_INTERVAL", "500ms")
	if cfg.FlushInterval, err = time.ParseDuration(flushStr); err != nil {
		return nil, fmt.Errorf("config: FLUSH_INTERVAL %q: %w", flushStr, err)
	}

	spreadStr := getEnv("MIN_SPREAD_PCT", "1.0")
	if cfg.MinSpreadPct, err = strconv.ParseFloat(spreadStr, 64); err != nil {
		return nil, fmt.Errorf("config: MIN_SPREAD_PCT %q: %w", spreadStr, err)
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getEnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q", v)
	}
	return n, nil
}
