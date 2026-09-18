# GoMarket Event Engine

> A production-grade, high-throughput event processing pipeline and analytical engine built in Go with a Python/FastAPI polyglot sidecar.
> Demonstrates real-world event-driven architecture, concurrent worker pools, LLM integration, and Kubernetes-ready deployment.

![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat&logo=go&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?style=flat&logo=fastapi&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-E34C26?style=flat)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             GoMarket Event Engine                                 │
│                                                                                  │
│  [High-Volume Tick Stream Generator]                                             │
│       │ publish 1,000+ ticks/sec                                                 │
│  [Redpanda] ← Kafka-compatible, single binary, no ZooKeeper                      │
│       │ market.ticks topic                                                       │
│  [Kafka Consumer / Ingester]                                                     │
│       │ chan TickData (buf=500) ← backpressure boundary                          │
│  [Worker Pool — 10 goroutines]                                                   │
│       │ CalculateSpread → filter spread > 1.0%                                   │
│  [Redis Dedup — SET NX 5s TTL]                                                   │
│       │ novel signals only                                                       │
│  [pgx.Batch Writer] ← flush @ 100 signals OR 500ms                               │
│       │                                                                          │
│  [PostgreSQL 16 Transactional Store]                                             │
│       ├── [LLM Worker — OpenAI SDK Compatible (Ollama local fallback)]           │
│       │        polls DB every 5s → structured JSON verdict → UPDATE row          │
│       ├── [Go REST Engine API :8080]                                             │
│       │        GET /healthz  GET /signals  POST /analyze                         │
│       └── [Python / FastAPI Analytical Sidecar :8081]                            │
│                GET /healthz  GET /api/v1/analytics/summary                       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack & Microservice Ecosystem

| Component | Technology | Role |
|---|---|---|
| Primary Backend | Go 1.22 | High-throughput ingestion, worker pool, batch storage |
| Secondary Backend | Python 3.11 + FastAPI + Pydantic v2 | Polyglot analytical sidecar microservice |
| Transactional API | Gin Web Framework | High-performance Go REST server |
| Analytical API | FastAPI + Uvicorn + asyncpg | Async Python analytical microservice |
| Database | PostgreSQL 16 + pgx/v5 + pgxpool | Transactional state & signal storage |
| Cache / Dedup | Redis 7 | Distributed deduplication sliding window |
| Message Bus | Redpanda (Kafka-compatible) | Decoupled event-driven streaming bus |
| LLM Integration | OpenAI SDK (Ollama local fallback) | Structured reasoning & signal classification |
| Observability | OpenTelemetry SDK + Zap | Distributed tracing & structured JSON logging |
| Migrations | golang-migrate | Version-controlled database schema migrations |
| Testing | testcontainers-go + Go `-race` | Containerized integration tests & race safety |
| Deployment | Docker Compose + Helm (Kubernetes/EKS)| Multi-stage non-root containers & K8s HPA |
| CI/CD | GitHub Actions | Automated linting, race-detector unit tests, container builds |

---

## Polyglot Architecture (Go + Python/FastAPI)

This platform employs a **polyglot microservices pattern** designed for enterprise workload separation:

1. **Go Core Engine (`cmd/engine`):** Engineered for max throughput, concurrency, and low latency. Handles raw event ingestion (1,000+ ticks/sec), channel backpressure, worker pool evaluation, Redis deduplication, and PostgreSQL batching.
2. **Python/FastAPI Analytics Sidecar (`services/analytics`):** Built with Python 3.11, FastAPI, Pydantic v2, and `asyncpg`. Reads from the shared PostgreSQL database to provide analytical aggregations, throughput calculations, and symbol distribution endpoints (`/api/v1/analytics/summary`, `/api/v1/analytics/symbols`).

```
[ Go Ingestion Engine ] ──> ( Writes Batches ) ──> [ PostgreSQL ] ──< ( Queries Analytics ) ──< [ Python FastAPI Sidecar ]
```

This decoupled architecture allows the Go ingestion layer to scale horizontally based on inbound event throughput, while the Python analytics service scales independently based on query load.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Deadeye102000/GoMarket-Arbitrage-engine.git
cd GoMarket-Arbitrage-engine

# 2. Start infrastructure (Postgres + Redis + Redpanda + Ollama + Analytics Sidecar)
make up

# 3. Pull the LLM model (containerized local fallback)
make pull-model

# 4. Run database migrations
make migrate

# 5. Start the engine
cp .env.example .env
make run
```

The engine begins processing **~1,000 ticks/second**. Watch the structured logs:

```json
{"level":"info","msg":"🚀 engine running"}
{"level":"info","msg":"batcher: flushed batch","count":100}
{"level":"info","msg":"llm analyzer: signal analyzed","pair":"BTC-USD","verdict":"BUY","confidence":0.87}
```

---

## API Reference

### Go Ingestion & Engine Service (`:8080`)

```bash
# Liveness + readiness probe
curl http://localhost:8080/healthz
# → {"status":"ok","db":"connected","redis":"connected"}

# Query transactional signals
curl "http://localhost:8080/signals?limit=10"

# Filter by pair with pagination
curl "http://localhost:8080/signals?pair=BTC-USD&limit=50&offset=0"

# On-demand LLM analysis endpoint
curl -X POST http://localhost:8080/analyze \
  -H "Content-Type: application/json" \
  -d '{"pair":"ETH-USD","buy_exchange":"Kraken","sell_exchange":"Coinbase","spread_pct":2.4}'
# → {"verdict":"BUY","confidence":0.84,"reasoning":"2.4% spread exceeds typical fee load..."}
```

### Python FastAPI Analytical Sidecar (`:8081`)

```bash
# Sidecar health check
curl http://localhost:8081/healthz
# → {"status":"ok","service":"analytics-sidecar"}

# Analytical summary (aggregated across all signals)
curl http://localhost:8081/api/v1/analytics/summary
# → {"total_signals":1420,"avg_spread_pct":1.4821,"max_spread_pct":3.82,"top_symbol":"BTC-USD","status":"healthy"}

# Per-symbol aggregation break-down
curl http://localhost:8081/api/v1/analytics/symbols
```

---

## Engineering Deep Dives

### Graceful Degradation & Context Management

> Relevant to: Kubernetes pod eviction, SIGTERM handling, `context.Context` propagation

The engine uses `signal.NotifyContext` to catch `SIGINT`/`SIGTERM`. Cancellation propagates down a deterministic chain — **no goroutine leaks, no data loss**:

```
SIGTERM received
  └─ root context cancelled
       ├─ Ingester: ctx.Done() fires → returns → defer close(tickCh)
       ├─ Worker Pool: range over tickCh drains remaining ticks → workers exit
       ├─ Pool watcher: workerPool.Wait() → close(sigCh)
       ├─ DB Batcher: sigCh closed → final pgx.Batch flush → returns
       ├─ LLM Worker: ctx.Done() fires → returns
       └─ Gin Server: srv.Shutdown(10s timeout) → drains in-flight HTTP requests

Hard deadline: context.WithTimeout(10s) → os.Exit if goroutines stall
```

This pattern mirrors what Kubernetes does during a rolling deployment:
the pod receives `SIGTERM`, waits `terminationGracePeriodSeconds`, then `SIGKILL`.

---

### Buffered Channels for Backpressure

> Relevant to: Memory safety, preventing OOM crashes under high load

All inter-goroutine communication uses **explicitly sized channels**:

```go
tickCh := make(chan market.TickData, 500)        // ingester → workers
sigCh  := make(chan market.ArbitrageSignal, 200) // workers  → batcher
```

**What happens when the database degrades:**

```
DB slows down
  → batcher blocks on pgx.SendBatch
  → sigCh fills to capacity (200 items)
  → workers block on "case out <- sig"
  → tickCh fills to capacity (500 items)
  → ingester's non-blocking send hits "default:" branch
  → drops tick + logs warning  ← heap stays bounded, no OOM
```

The buffered channel approach guarantees an **explicit, observable backpressure boundary** with a strictly bounded memory ceiling.

---

### pgx Connection Pooling & Batching

> Relevant to: High-volume PostgreSQL throughput, reducing round-trip latency

**Naive approach:** 1 INSERT per signal = ~1,000 DB round-trips/second.

**This engine:** `pgxpool` + `pgx.Batch` = **max 2 round-trips/second**:

```go
// All signals in one network round-trip:
batch := &pgx.Batch{}
for _, sig := range signals {           // up to 100 signals
    batch.Queue(insertSQL, sig.Pair, ...) 
}
results := pool.SendBatch(ctx, batch)   // ← single round-trip
defer results.Close()
```

Flush triggers (whichever fires first):
1. **Size trigger:** 100 signals buffered → flush immediately
2. **Time trigger:** 500ms elapsed → flush partial batch (prevents stale data)
3. **Shutdown trigger:** channel closed → flush all remaining → exit

---

### Kafka-Driven Event Ingestion

> Relevant to: Decoupled event architectures, pub/sub streaming

The ingester consumes from **Redpanda** (single binary, Kafka-compatible, no ZooKeeper):

```
Producer: market.ticks topic → consumer group: gomarket-engine
```

**Fallback mode** (`KAFKA_ENABLED=false`): the ingester bypasses Kafka and pushes directly to an internal Go channel. Same worker pool, same batcher — different transport layer.

---

### OpenAI-Compatible LLM Integration & Compliance

> Relevant to: OpenAI / Anthropic SDK usage, structured JSON output, HIPAA & SOC 2 data governance

The LLM analyzer integrates standard **OpenAI SDK client formats** (`go-openai`) with structured JSON schema responses:

```go
clientCfg := openai.DefaultConfig("api-key")
clientCfg.BaseURL = os.Getenv("OLLAMA_BASE_URL") // Defaults to local Ollama fallback container

resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
    Model: "llama3.2",
    ResponseFormat: &openai.ChatCompletionResponseFormat{
        Type: openai.ChatCompletionResponseFormatTypeJSONObject,
    },
    ...
})
```

> **Note on Local Execution:** The engine is built using standard OpenAI/Anthropic client interfaces. To allow reviewers to evaluate the system locally without needing external API keys, Docker Compose includes an OpenAI-compatible Ollama container.

#### Data Sanitization & Security (HIPAA / SOC 2 Readiness)
Before any event payload is sent to the LLM model, the analyzer executes a **Data Sanitization Pass**:
- Strips PII, authentication tokens, and internal network IP addresses from prompt contexts.
- Validates strict JSON output formats using Pydantic/Go struct parsers before persisting AI verdicts.
- Enforces per-call context timeouts (30s) and fallback paths if the model service is unreachable.

---

### OpenTelemetry Distributed Tracing

The engine initialises an OTel `TracerProvider` with 10% sampling:

```go
sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1))
```

Spans are generated across HTTP requests, PostgreSQL batch execution, and LLM inference calls.

---

## Continuous Integration & Automated Testing

The repository relies on a automated **GitHub Actions CI pipeline** (`.github/workflows/ci.yml`):

1. **Static Analysis & Linting:** Runs `golangci-lint` to enforce code quality, formatting, and idiomatic Go practices.
2. **Race-Detector Unit Testing:** Runs `go test -race -v ./internal/...` to catch concurrent memory access bugs at compile time.
3. **Containerized Integration Testing:** Uses `testcontainers-go` to spin up real PostgreSQL containers and test batch flushing and shutdown logic.
4. **Multi-Stage Docker Image Build:** Verifies scratch container image compilation on every push.

```bash
# Run unit tests with race detector locally
make test

# Run containerized integration tests
make integration
```

---

## Kubernetes Deployment (EKS via Helm)

```bash
# Deploy to EKS cluster
helm upgrade --install gomarket ./helm/gomarket \
  --set image.tag=$(git rev-parse --short HEAD) \
  --namespace gomarket \
  --create-namespace
```

Features auto-scaling via `HorizontalPodAutoscaler` (1 → 10 replicas based on CPU/Memory thresholds) and `preStop` hooks for zero-downtime rolling updates.

---

## Project Structure

```
GoMarket Event Engine/
├── cmd/engine/main.go          ← Orchestrator: wires all components, graceful shutdown
├── internal/
│   ├── config/config.go        ← Typed config from env vars
│   ├── market/types.go         ← Domain types: TickData, ArbitrageSignal, LLMAnalysis
│   ├── ingester/ingester.go    ← Kafka consumer / direct channel fallback
│   ├── worker/
│   │   ├── pool.go             ← Goroutine worker pool
│   │   └── pool_test.go        ← Unit tests (spread calc, filtering, race safety)
│   ├── dedup/redis.go          ← Redis SET NX deduplication (5s TTL)
│   ├── storage/
│   │   ├── batcher.go          ← pgx.Batch writer with dual-flush triggers
│   │   └── batcher_test.go     ← Unit tests (size, ticker, drain, error handling)
│   ├── llm/analyzer.go         ← OpenAI SDK compatible LLM worker with sanitization
│   ├── api/
│   │   ├── server.go           ← Gin server setup with proper timeouts
│   │   ├── handlers.go         ← /healthz, /signals, /analyze handlers
│   │   └── middleware.go       ← Request logger middleware
│   └── telemetry/otel.go       ← OTel TracerProvider init
├── services/analytics/         ← Python / FastAPI Polyglot Sidecar
│   ├── main.py                 ← FastAPI analytical endpoints
│   ├── requirements.txt        ← Dependencies (fastapi, uvicorn, asyncpg, pydantic)
│   └── Dockerfile              ← Python 3.11 container definition
├── migrations/                 ← PostgreSQL database schema migrations
├── tests/integration/          ← testcontainers-go integration tests
├── helm/gomarket/              ← Helm chart for Kubernetes/EKS deployment + HPA
├── .github/workflows/ci.yml    ← GitHub Actions: lint, test -race, build, docker
├── docker-compose.yml          ← Complete multi-container local stack
├── Dockerfile                  ← Go multi-stage, non-root binary build
└── Makefile                    ← Development & automation targets
```

---

## Future Roadmap

- **Dual-Database Architecture (ClickHouse Integration):** Currently routing transactional state to PostgreSQL. Next phase: dual-write high-frequency tick data into **ClickHouse** for sub-millisecond analytical aggregations over multi-billion row datasets, keeping PostgreSQL strictly responsible for transactional state.
- **Kafka Schema Registry Integration:** Introduce Avro serialization with Confluent Schema Registry for strict event schema evolution across Go and Python microservices.

---

## License

MIT — Free to use, fork, and build on.
