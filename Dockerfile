# Build stage
FROM rust:1.83-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

# Copy manifests
COPY Cargo.toml Cargo.lock* ./

# Copy source code
COPY src ./src

# Copy migrations (needed at compile time for include_str!)
COPY migrations ./migrations

# Build all binaries
RUN cargo build --release --bin btc-bot --bin btc-probability-matrix --bin btc-logger-ws --bin btc-backtester

# Runtime stage - single container with both binaries
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies (openssl needed for Redis TLS)
RUN apk add --no-cache ca-certificates tzdata openssl

# Create non-root user
RUN adduser -D -g '' appuser

# Copy all binaries from builder
COPY --from=builder /app/target/release/btc-bot .
COPY --from=builder /app/target/release/btc-probability-matrix .
COPY --from=builder /app/target/release/btc-logger-ws .
COPY --from=builder /app/target/release/btc-backtester .

# Copy required assets
COPY config ./config

# Create output directory for matrix files
RUN mkdir -p output && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD pgrep btc-bot || exit 1

# Build matrix first, then run logger (background) and bot (foreground)
CMD sh -c "./btc-probability-matrix build && ./btc-logger-ws & exec ./btc-bot"
