#!/bin/bash
# Chainlink BTC/USD Hourly Update Job
# Run via cron: 0 * * * * /path/to/chainlink_hourly_update.sh >> /var/log/chainlink_update.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$BOT_DIR/target/release/btc-probability-matrix"

echo "=========================================="
echo "Chainlink Update: $(date)"
echo "=========================================="

if [ ! -f "$BINARY" ]; then
    echo "ERROR: Binary not found at $BINARY"
    echo "Run: cd $BOT_DIR && cargo build --release"
    exit 1
fi

cd "$BOT_DIR"
"$BINARY" chainlink update

echo "Update completed at $(date)"
echo ""
