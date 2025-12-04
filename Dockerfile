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

# ═══════════════════════════════════════════════════════════════════════════════
# Runtime stage for the trading bot
# ═══════════════════════════════════════════════════════════════════════════════
FROM alpine:3.19 AS bot

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy bot binary from builder
COPY --from=builder /app/target/release/btc-bot .

# Copy required assets
COPY config ./config

# Set ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check (check if process is running)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep btc-bot || exit 1

# Run the bot
CMD ["./btc-bot"]

# ═══════════════════════════════════════════════════════════════════════════════
# Runtime stage for the matrix builder (cron job)
# ═══════════════════════════════════════════════════════════════════════════════
FROM alpine:3.19 AS matrix-builder

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy matrix builder binary from builder
COPY --from=builder /app/target/release/btc-probability-matrix .

# Create output directory
RUN mkdir -p output && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Run the matrix build command
# This will fetch data from DB and save the new matrix to DB
CMD ["./btc-probability-matrix", "build"]
