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

# Build both binaries
RUN cargo build --release --bin btc-bot --bin btc-probability-matrix

# Runtime stage - single container with both binaries
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy both binaries from builder
COPY --from=builder /app/target/release/btc-bot .
COPY --from=builder /app/target/release/btc-probability-matrix .

# Copy required assets
COPY config ./config

# Create output directory for matrix files
RUN mkdir -p output && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD pgrep btc-bot || exit 1

# Build matrix first, then run bot
CMD sh -c "./btc-probability-matrix build && exec ./btc-bot"
