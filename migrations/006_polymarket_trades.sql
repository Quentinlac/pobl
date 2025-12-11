-- Polymarket individual trades for BTC 15-minute UP/DOWN markets
-- Per-second resolution from the data-api.polymarket.com/trades endpoint

CREATE TABLE IF NOT EXISTS polymarket_trades (
    -- Trade identification
    id SERIAL PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL UNIQUE,  -- 0x + 64 hex chars

    -- Market identification
    condition_id VARCHAR(66) NOT NULL,             -- Market condition ID
    window_timestamp BIGINT NOT NULL,              -- Unix timestamp of 15-min window start
    slug VARCHAR(100) NOT NULL,                    -- e.g., btc-updown-15m-1765400400

    -- Trade details
    timestamp TIMESTAMPTZ NOT NULL,                -- Exact trade timestamp
    outcome VARCHAR(10) NOT NULL,                  -- 'Up' or 'Down'
    side VARCHAR(4) NOT NULL,                      -- 'BUY' or 'SELL'
    price NUMERIC(10, 6) NOT NULL,                 -- Trade price (0.00-1.00)
    size NUMERIC(20, 6) NOT NULL,                  -- Trade size (shares)

    -- Metadata
    asset VARCHAR(100),                            -- Token ID
    proxy_wallet VARCHAR(42),                      -- Trader address
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying by window
CREATE INDEX IF NOT EXISTS idx_polymarket_trades_window
ON polymarket_trades (window_timestamp, outcome);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_polymarket_trades_timestamp
ON polymarket_trades (timestamp DESC);

-- Index for condition ID lookups
CREATE INDEX IF NOT EXISTS idx_polymarket_trades_condition
ON polymarket_trades (condition_id);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_polymarket_trades_window_time
ON polymarket_trades (window_timestamp, timestamp, outcome);

-- View: trades with computed window info
CREATE OR REPLACE VIEW v_polymarket_trades AS
SELECT
    t.*,
    TO_TIMESTAMP(window_timestamp) AT TIME ZONE 'UTC' as window_start_utc,
    TO_TIMESTAMP(window_timestamp + 900) AT TIME ZONE 'UTC' as window_end_utc,
    EXTRACT(EPOCH FROM (timestamp - TO_TIMESTAMP(window_timestamp))) as seconds_into_window
FROM polymarket_trades t;

-- View: aggregated price by second (VWAP)
CREATE OR REPLACE VIEW v_polymarket_second_prices AS
SELECT
    window_timestamp,
    DATE_TRUNC('second', timestamp) as second_ts,
    outcome,
    SUM(price * size) / NULLIF(SUM(size), 0) as vwap_price,
    SUM(size) as total_volume,
    COUNT(*) as trade_count,
    MIN(price) as low_price,
    MAX(price) as high_price
FROM polymarket_trades
GROUP BY window_timestamp, DATE_TRUNC('second', timestamp), outcome;

-- View: combined UP/DOWN trades summary per window
CREATE OR REPLACE VIEW v_polymarket_window_summary AS
SELECT
    window_timestamp,
    TO_TIMESTAMP(window_timestamp) AT TIME ZONE 'UTC' as window_start_utc,
    slug,
    COUNT(*) FILTER (WHERE outcome = 'Up') as up_trades,
    COUNT(*) FILTER (WHERE outcome = 'Down') as down_trades,
    SUM(size) FILTER (WHERE outcome = 'Up') as up_volume,
    SUM(size) FILTER (WHERE outcome = 'Down') as down_volume,
    AVG(price) FILTER (WHERE outcome = 'Up') as avg_up_price,
    AVG(price) FILTER (WHERE outcome = 'Down') as avg_down_price,
    MIN(timestamp) as first_trade,
    MAX(timestamp) as last_trade
FROM polymarket_trades
GROUP BY window_timestamp, slug;
