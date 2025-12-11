-- Polymarket historical share prices for BTC 15-minute UP/DOWN markets
-- Stores price history for each token to analyze market behavior over time

CREATE TABLE IF NOT EXISTS polymarket_prices (
    -- Window identification
    window_timestamp BIGINT NOT NULL,           -- Unix timestamp of window start (e.g., 1765400400)

    -- Token info
    token_type VARCHAR(4) NOT NULL,             -- 'UP' or 'DOWN'
    token_id VARCHAR(100) NOT NULL,             -- CLOB token ID

    -- Price data point
    timestamp TIMESTAMPTZ NOT NULL,             -- When this price was recorded
    price NUMERIC(10, 6) NOT NULL,              -- Share price (0.000000 to 1.000000)

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (window_timestamp, token_type, timestamp)
);

-- Index for querying by window
CREATE INDEX IF NOT EXISTS idx_polymarket_prices_window
ON polymarket_prices (window_timestamp, token_type);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_polymarket_prices_timestamp
ON polymarket_prices (timestamp DESC);

-- Index for token lookups
CREATE INDEX IF NOT EXISTS idx_polymarket_prices_token_id
ON polymarket_prices (token_id);

-- Useful view: prices with window info
CREATE OR REPLACE VIEW v_polymarket_prices AS
SELECT
    window_timestamp,
    TO_TIMESTAMP(window_timestamp) AT TIME ZONE 'UTC' as window_start_utc,
    TO_TIMESTAMP(window_timestamp + 900) AT TIME ZONE 'UTC' as window_end_utc,
    token_type,
    token_id,
    timestamp,
    price,
    -- Calculate seconds into window
    EXTRACT(EPOCH FROM (timestamp - TO_TIMESTAMP(window_timestamp))) as seconds_into_window
FROM polymarket_prices;

-- Useful view: combined UP/DOWN prices per window
CREATE OR REPLACE VIEW v_polymarket_window_prices AS
SELECT
    up.window_timestamp,
    TO_TIMESTAMP(up.window_timestamp) AT TIME ZONE 'UTC' as window_start_utc,
    up.timestamp,
    EXTRACT(EPOCH FROM (up.timestamp - TO_TIMESTAMP(up.window_timestamp))) as seconds_into_window,
    up.price as up_price,
    down.price as down_price,
    up.price + down.price as price_sum  -- Should be close to 1.0
FROM polymarket_prices up
JOIN polymarket_prices down
    ON up.window_timestamp = down.window_timestamp
    AND up.timestamp = down.timestamp
    AND up.token_type = 'UP'
    AND down.token_type = 'DOWN';
