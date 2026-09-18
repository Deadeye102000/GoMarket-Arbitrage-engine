.PHONY: all up down pull-model migrate migrate-down run build docker \
        test integration coverage lint fmt help

BINARY_NAME := engine
BUILD_DIR   := bin
MODULE      := github.com/gomarket-oss/arbitrage-engine
MIGRATE_DSN := postgres://gomarket:gomarket@localhost:5432/gomarket?sslmode=disable

# ── Default ───────────────────────────────────────────────────────────────────
all: build

# ── Infrastructure ────────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "✓ Infrastructure started. Run 'make pull-model' on first use."

down:
	docker compose down -v
	@echo "✓ All containers and volumes removed."

pull-model:
	@echo "Pulling llama3.2 model (~2GB, cached after first run)..."
	docker compose exec ollama ollama pull llama3.2
	@echo "✓ Model ready."

# ── Database ──────────────────────────────────────────────────────────────────
migrate:
	@echo "Running migrations..."
	docker run --rm \
		-v $(PWD)/migrations:/migrations \
		--network host \
		migrate/migrate \
		-path=/migrations/ \
		-database "$(MIGRATE_DSN)" \
		up
	@echo "✓ Migrations applied."

migrate-down:
	docker run --rm \
		-v $(PWD)/migrations:/migrations \
		--network host \
		migrate/migrate \
		-path=/migrations/ \
		-database "$(MIGRATE_DSN)" \
		down 1

# ── Application ───────────────────────────────────────────────────────────────
run:
	@[ -f .env ] && export $$(cat .env | xargs) || true
	go run ./cmd/engine/...

build:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build \
		-ldflags="-w -s -X main.version=$$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
		-o $(BUILD_DIR)/$(BINARY_NAME) \
		./cmd/engine
	@echo "✓ Binary: $(BUILD_DIR)/$(BINARY_NAME)"

# ── Docker ────────────────────────────────────────────────────────────────────
docker:
	docker build -t gomarket-engine:latest .
	@echo "✓ Image built: gomarket-engine:latest"

# ── Testing ───────────────────────────────────────────────────────────────────
test:
	@echo "Running unit tests with race detector..."
	go test -race -count=1 -timeout 30s ./internal/...

test-python:
	@echo "Running Python sidecar pytest suite..."
	@cd services/analytics && pytest

integration:
	@echo "Running containerized integration tests (requires Docker)..."
	go test -v -race -count=1 -timeout 120s ./tests/integration/...

coverage:
	go test -race -coverprofile=coverage.out -covermode=atomic ./internal/...
	go tool cover -html=coverage.out -o coverage.html
	go tool cover -func=coverage.out | tail -1
	@echo "✓ Coverage report: coverage.html"

# ── Quality ───────────────────────────────────────────────────────────────────
lint:
	@echo "Running go vet..."
	go vet ./...
	@echo "Running staticcheck..."
	@which staticcheck > /dev/null 2>&1 || go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...
	@echo "✓ Lint passed."

lint-python:
	@echo "Running ruff and mypy on Python analytics sidecar..."
	@cd services/analytics && ruff check . && mypy .

fmt:
	gofmt -w -s .

tidy:
	go mod tidy

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "GoMarket Arbitrage Engine — Makefile"
	@echo "======================================"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up           Start all Docker services (Postgres, Redis, Redpanda, Ollama)"
	@echo "  make down         Stop and remove all containers + volumes"
	@echo "  make pull-model   Download llama3.2 LLM model into Ollama (~2GB, one-time)"
	@echo ""
	@echo "Database:"
	@echo "  make migrate      Apply all pending SQL migrations"
	@echo "  make migrate-down Roll back the last migration"
	@echo ""
	@echo "Development:"
	@echo "  make run          Run engine locally (reads .env)"
	@echo "  make build        Compile binary to ./bin/engine"
	@echo "  make docker       Build Docker image"
	@echo ""
	@echo "Testing:"
	@echo "  make test         Unit tests with -race flag"
	@echo "  make integration  Containerized integration tests"
	@echo "  make coverage     HTML coverage report"
	@echo ""
	@echo "Quality:"
	@echo "  make lint         go vet + staticcheck"
	@echo "  make fmt          gofmt -s"
	@echo ""
