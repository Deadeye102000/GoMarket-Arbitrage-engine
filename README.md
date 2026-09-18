# GoMarket Arbitrage Engine

> A production-grade, high-frequency arbitrage signal pipeline built in Go.
> Demonstrates real-world event-driven architecture, concurrent processing, AI integration, and Kubernetes-ready deployment — all running **100% free and offline**.

![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat&logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-E34C26?style=flat)
![LLM](https://img.shields.io/badge/LLM-Ollama-black?style=flat)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     GoMarket Arbitrage Engine                        │
│                                                                      │
│  [Tick Simulator]                                                    │
│       │ publish 1,000 ticks/sec                                      │
│  [Redpanda] ← Kafka-compatible, single binary, no ZooKeeper         │
│       │ market.ticks topic                                           │
│  [Kafka Consumer / Ingester]                                         │
│       │ chan TickData (buf=500) ← backpressure boundary              │
│  [Worker Pool — 10 goroutines]                                       │
│       │ CalculateSpread → filter spread > 1.0%                       │
│  [Redis Dedup — SET NX 5s TTL]                                      │
│       │ novel signals only                                           │
│  [pgx.Batch Writer] ← flush @ 100 signals OR 500ms                  │
│       │                                                              │
│  [PostgreSQL 16]                                                     │
│                                                                      │
│  [LLM Worker — Ollama llama3.2] ← FREE, local, in Docker           │
│       polls DB every 5s → structured JSON verdict → UPDATE row       │
│                                                                      │
│  [Gin REST API :8080]                                                │
│       GET  /healthz   ← K8s liveness + readiness probe              │
│       GET  /signals   ← paginated query with optional pair filter    │
│       POST /analyze   ← on-demand LLM analysis                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack (100% Free & Open Source)

| Component | Technology | Cost |
|---|---|---|
| Language | Go 1.22 | Free |
| API | Gin | Free / MIT |
| Database | PostgreSQL 16 + pgx/v5 + pgxpool | Free |
| Cache / Dedup | Redis 7 | Free |
| Message Bus | Redpanda (Kafka-compatible) | Free / BSL |
| LLM | Ollama + llama3.2 (local) | Free |
| Tracing | OpenTelemetry SDK | Free |
| Logging | zap (structured JSON) | Free / MIT |
| Migrations | golang-migrate | Free |
| Integration Tests | testcontainers-go | Free |
| Containers | Docker + Helm (Kubernetes/EKS) | Free |
| CI/CD | GitHub Actions | Free (public repos) |

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/gomarket-oss/arbitrage-engine
cd arbitrage-engine

# 2. Start infrastructure (Postgres + Redis + Redpanda + Ollama)
make up

# 3. Pull the LLM model — ~2GB, cached in Docker volume after first run
#    Tip: use "tinyllama" in .env if your machine has < 8GB RAM
make pull-model

# 4. Run database migrations
make migrate

# 5. Start the engine
cp .env.example .env
make run
```

The engine begins processing **~1,000 ticks/second**. Watch the logs:

```
{"level":"info","msg":"🚀 engine running"}
{"level":"info","msg":"batcher: flushed batch","count":100}
{"level":"info","msg":"llm analyzer: signal analyzed","pair":"BTC-USD","verdict":"BUY","confidence":0.87}
```

---

## API Reference

```bash
# Liveness + readiness probe
curl http://localhost:8080/healthz
# → {"status":"ok","db":"connected","redis":"connected"}

# Recent signals (all pairs)
curl "http://localhost:8080/signals?limit=10"

# Filter by pair with pagination
curl "http://localhost:8080/signals?pair=BTC-USD&limit=50&offset=0"

# On-demand LLM analysis (no key required — uses local Ollama)
curl -X POST http://localhost:8080/analyze \
  -H "Content-Type: application/json" \
  -d '{"pair":"ETH-USD","buy_exchange":"Kraken","sell_exchange":"Coinbase","spread_pct":2.4}'
# → {"verdict":"BUY","confidence":0.84,"reasoning":"2.4% spread exceeds typical fee load..."}
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

> Relevant to: Memory safety, preventing OOM crashes when the database slows down

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

Contrast with an unbounded queue: the process would consume unbounded memory
until the OOM killer terminates it. The buffered channel approach gives you
**explicit, observable backpressure** with a predictable memory ceiling.

---

### pgx Connection Pooling & Batching

> Relevant to: High-volume PostgreSQL, reducing round-trip latency

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

`pgxpool` manages a connection pool, eliminating per-request connection overhead.
Pool size is configurable — under `WORKER_COUNT=10` workers, the pool
ensures connections are reused rather than created per batch.

---

### Kafka-Driven Event Ingestion

> Relevant to: Event-driven architectures, decoupled producers/consumers

The ingester publishes to **Redpanda** (single binary, Kafka-compatible, no ZooKeeper):

```
Producer: market.ticks topic → consumer group: gomarket-engine
```

**Fallback mode** (`KAFKA_ENABLED=false`): the ingester bypasses Kafka and
pushes directly to a Go channel. Same worker pool, same batcher — different
transport. One env var change, zero code change.

This demonstrates the **dependency inversion** principle in system design:
the pipeline core doesn't care whether data arrives from Kafka or a channel.

---

### Local LLM Integration (Structured Output)

> Relevant to: OpenAI/Anthropic SDK usage, structured outputs, LLM guardrails

The LLM worker uses the **go-openai SDK** pointed at Ollama's OpenAI-compatible endpoint:

```go
clientCfg := openai.DefaultConfig("ollama") // API key required by SDK, ignored by Ollama
clientCfg.BaseURL = "http://ollama:11434/v1"

resp, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
    Model: "llama3.2",
    ResponseFormat: &openai.ChatCompletionResponseFormat{
        Type: openai.ChatCompletionResponseFormatTypeJSONObject, // structured output
    },
    ...
})
```

Response is constrained to a JSON schema:
```json
{"verdict": "BUY", "confidence": 0.82, "reasoning": "Spread exceeds fee load..."}
```

**Production guardrails:**
- 30-second per-call timeout (`context.WithTimeout`)
- Graceful skip if Ollama is unavailable (warning logged, engine continues)
- `LLM_ENABLED=false` disables the feature entirely with no code path changes

---

### OpenTelemetry Distributed Tracing

> Relevant to: Observability, structured logging, production debugging

The engine initialises an OTel `TracerProvider` with 10% sampling:

```go
sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1))
```

In development (`OTEL_ENDPOINT=stdout`), spans are printed as JSON. In production,
set `OTEL_ENDPOINT=http://otel-collector:4317` to export to Jaeger, Tempo, or Datadog.

Spans are created on:
- Gin HTTP requests (via `RequestLogger` middleware)
- pgx batch writes
- LLM Ollama calls

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_DSN` | local postgres | PostgreSQL connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis host:port |
| `KAFKA_BROKERS` | `localhost:19092` | Comma-separated broker list |
| `KAFKA_ENABLED` | `true` | `false` = use direct channel (no Kafka) |
| `KAFKA_TOPIC` | `market.ticks` | Kafka topic name |
| `OLLAMA_BASE_URL` | `http://localhost:11434/v1` | Ollama API endpoint |
| `OLLAMA_MODEL` | `llama3.2` | Model name (use `tinyllama` for <8GB RAM) |
| `LLM_ENABLED` | `true` | Enable/disable LLM analysis |
| `LLM_SAMPLE_RATE` | `10` | Analyze 1-in-N signals |
| `WORKER_COUNT` | `10` | Goroutine pool size |
| `CHANNEL_SIZE` | `500` | Tick channel buffer |
| `BATCH_SIZE` | `100` | DB batch flush threshold |
| `FLUSH_INTERVAL` | `500ms` | DB batch time-based flush |
| `MIN_SPREAD_PCT` | `1.0` | Minimum spread % to emit a signal |
| `PORT` | `8080` | HTTP API port |
| `OTEL_ENDPOINT` | `stdout` | OTel exporter target |

---

## Testing

```bash
# Unit tests with race detector (-race catches data races at test time)
make test

# Containerized integration tests (spins up real Postgres in Docker)
make integration

# HTML coverage report
make coverage

# Static analysis
make lint
```

### Test Coverage

| Package | Test Cases |
|---|---|
| `internal/worker` | Spread calc (positive, zero bid, negative bid, boundary), pool filtering, multi-worker correctness, dedup injection |
| `internal/storage` | Flush-on-size, flush-on-ticker, zero data loss on shutdown, error resilience |
| `tests/integration` | Signal written to DB, 100-item batch flush, graceful shutdown no data loss |

The `-race` flag detects concurrent memory access bugs at test time —
a standard requirement in Go production codebases.

---

## Kubernetes Deployment (EKS via Helm)

```bash
# Deploy to your EKS cluster
helm upgrade --install gomarket ./helm/gomarket \
  --set image.tag=$(git rev-parse --short HEAD) \
  --set image.repository=YOUR_ECR_REPO/gomarket-engine \
  --namespace gomarket \
  --create-namespace

# Create the secrets (DSN, Redis addr, etc.)
kubectl create secret generic gomarket-secrets \
  --from-literal=DATABASE_DSN='postgres://...' \
  --from-literal=REDIS_ADDR='redis-svc:6379' \
  --from-literal=KAFKA_BROKERS='kafka-svc:9092' \
  --from-literal=OLLAMA_BASE_URL='http://ollama-svc:11434/v1' \
  -n gomarket

# The HPA scales 1→10 pods at 70% CPU
kubectl get hpa -n gomarket
```

The `preStop` lifecycle hook (`sleep 5`) gives Kubernetes time to remove the pod
from Service endpoints before SIGTERM is sent — preventing in-flight requests
from hitting a terminating pod.

---

## Performance Characteristics

| Metric | Value |
|---|---|
| Tick ingestion rate | ~1,000/sec |
| Worker goroutines | 10 (configurable) |
| DB round-trips | ≤ 2/sec (via batching) |
| Tick channel buffer | 500 (backpressure boundary) |
| Signal channel buffer | 200 (backpressure boundary) |
| Shutdown drain time | < 1s typical, 10s hard limit |
| Docker image size | ~12MB (multi-stage build) |

---

## Project Structure

```
GoMarket Arbitrage Engine/
├── cmd/engine/main.go          ← Orchestrator: wires all components, graceful shutdown
├── internal/
│   ├── config/config.go        ← Typed config from env vars
│   ├── market/types.go         ← Domain types: TickData, ArbitrageSignal, LLMAnalysis
│   ├── ingester/ingester.go    ← Kafka producer+consumer / direct channel fallback
│   ├── worker/
│   │   ├── pool.go             ← Goroutine pool with exported CalculateSpread
│   │   └── pool_test.go        ← Unit tests (spread, filtering, dedup injection)
│   ├── dedup/redis.go          ← Redis SET NX deduplication (5s TTL)
│   ├── storage/
│   │   ├── batcher.go          ← pgx.Batch writer with dual-flush triggers
│   │   └── batcher_test.go     ← Unit tests (size, ticker, drain, error handling)
│   ├── llm/analyzer.go         ← Ollama LLM worker (OpenAI-compatible SDK)
│   ├── api/
│   │   ├── server.go           ← Gin server setup with proper timeouts
│   │   ├── handlers.go         ← /healthz, /signals, /analyze handlers
│   │   └── middleware.go       ← Request logger middleware
│   └── telemetry/otel.go       ← OTel TracerProvider init
├── migrations/
│   ├── 000001_create_signals.up.sql
│   └── 000001_create_signals.down.sql
├── tests/integration/
│   └── pipeline_test.go        ← testcontainers-go integration tests
├── helm/gomarket/              ← Helm chart for EKS deployment + HPA
├── .github/workflows/ci.yml    ← GitHub Actions: lint, test -race, build, docker
├── docker-compose.yml          ← Full local stack (0 cloud services needed)
├── Dockerfile                  ← Multi-stage, non-root, ~12MB image
├── Makefile                    ← All development workflows
└── .env.example                ← Template (no secrets needed for local dev)
```

---

## License

MIT — free to use, fork, and build on.

---

*Built to demonstrate production Go engineering: event pipelines, concurrency patterns, database optimization, local LLM integration, and Kubernetes deployment.*
