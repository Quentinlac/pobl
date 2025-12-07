-- Market data logger table
-- Captures order book snapshots every 200ms for BTC 15-minute markets

CREATE TABLE IF NOT EXISTS market_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_slug VARCHAR(100) NOT NULL,
    up_token_id VARCHAR(100) NOT NULL,
    down_token_id VARCHAR(100) NOT NULL,
    price_up DECIMAL(10, 4) NOT NULL,
    price_down DECIMAL(10, 4) NOT NULL,
    size_up DECIMAL(20, 6) NOT NULL,
    size_down DECIMAL(20, 6) NOT NULL,
    edge_up DECIMAL(10, 6),
    edge_down DECIMAL(10, 6),
    btc_price DECIMAL(20, 8) NOT NULL,
    time_elapsed INTEGER NOT NULL,
    price_delta DECIMAL(20, 8) NOT NULL,
    error_message TEXT
);

-- Add bid prices and sell edge columns (for selling)
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS bid_up DECIMAL(10, 4);
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS bid_down DECIMAL(10, 4);
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS edge_up_sell DECIMAL(10, 6);
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS edge_down_sell DECIMAL(10, 6);

-- Add bid sizes (liquidity available for selling)
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS bid_size_up NUMERIC(20, 6);
ALTER TABLE market_logs ADD COLUMN IF NOT EXISTS bid_size_down NUMERIC(20, 6);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_market_logs_timestamp ON market_logs (timestamp DESC);

-- Index for market-specific queries
CREATE INDEX IF NOT EXISTS idx_market_logs_slug ON market_logs (market_slug, timestamp DESC);

-- Index for buy edge analysis
CREATE INDEX IF NOT EXISTS idx_market_logs_edges ON market_logs (edge_up, edge_down) WHERE edge_up IS NOT NULL;

-- Index for sell edge analysis
CREATE INDEX IF NOT EXISTS idx_market_logs_sell_edges ON market_logs (edge_up_sell, edge_down_sell) WHERE edge_up_sell IS NOT NULL;
