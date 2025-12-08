-- Trade Attempts Log
-- Records every trade attempt (successful or failed) for analysis
-- Can be joined with market_logs to compare simulation vs reality

CREATE TABLE IF NOT EXISTS trade_attempts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_slug VARCHAR(100),
    token_id VARCHAR(100),

    -- Trade details
    direction VARCHAR(10) NOT NULL,          -- UP/DOWN
    side VARCHAR(10) NOT NULL,               -- BUY/SELL
    strategy_type VARCHAR(20),               -- TERMINAL/EXIT
    order_type VARCHAR(10) NOT NULL,         -- FOK/GTC/GTD

    -- Prices at attempt time
    our_probability DECIMAL(10,6),
    market_price DECIMAL(10,6),              -- ask for BUY, bid for SELL
    edge DECIMAL(10,6),

    -- Order details
    bet_amount_usdc DECIMAL(10,4),
    shares DECIMAL(10,4),
    slippage_price DECIMAL(10,6),            -- price with slippage applied

    -- Context
    btc_price DECIMAL(12,2),
    price_delta DECIMAL(12,2),
    time_elapsed_secs INT,
    time_remaining_secs INT,

    -- Result
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    order_id VARCHAR(100),                   -- if successful

    -- For joining with market_logs
    time_bucket INT,
    delta_bucket INT,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_trade_attempts_timestamp ON trade_attempts(timestamp);

-- Index for joining with market_logs
CREATE INDEX IF NOT EXISTS idx_trade_attempts_buckets ON trade_attempts(time_bucket, delta_bucket);

-- Index for market analysis
CREATE INDEX IF NOT EXISTS idx_trade_attempts_market ON trade_attempts(market_slug, timestamp);

-- Index for success/failure analysis
CREATE INDEX IF NOT EXISTS idx_trade_attempts_success ON trade_attempts(success, timestamp);
