# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Download deps first (layer cached unless go.mod/go.sum change)
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source and build a statically-linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /bin/engine \
    ./cmd/engine

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata wget && \
    addgroup -S app && adduser -S -G app app

COPY --from=builder /bin/engine /usr/local/bin/engine

USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/engine"]
