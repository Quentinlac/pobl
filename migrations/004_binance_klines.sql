-- Binance klines (candlestick) data for BTC/USDT
-- Replaces chainlink_prices for matrix building
-- Uses 1-minute intervals for high-resolution probability calculations

CREATE TABLE IF NOT EXISTS binance_klines (
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(30, 8) NOT NULL DEFAULT 0,
    quote_volume NUMERIC(30, 8) NOT NULL DEFAULT 0,
    num_trades BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, timestamp)
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_binance_klines_symbol_time
ON binance_klines (symbol, timestamp DESC);

-- Index for gap detection
CREATE INDEX IF NOT EXISTS idx_binance_klines_timestamp
ON binance_klines (timestamp);
