-- ═══════════════════════════════════════════════════════════════════════════════
-- BTC 15-Minute Bot - Trade Tracking Tables
-- ═══════════════════════════════════════════════════════════════════════════════

-- Table 1: bot_trades
-- Records each bet placed by the bot
CREATE TABLE IF NOT EXISTS bot_trades (
    id              SERIAL PRIMARY KEY,

    -- Market identification
    market_id       VARCHAR(100),                    -- Polymarket condition ID
    window_start    TIMESTAMPTZ NOT NULL,            -- Start of 15-min window

    -- Trade details
    direction       VARCHAR(10) NOT NULL,            -- 'UP' or 'DOWN'
    amount_usdc     NUMERIC(20, 8) NOT NULL,         -- Amount bet in USDC
    entry_price     NUMERIC(10, 6) NOT NULL,         -- Price paid (0.00-1.00)
    shares          NUMERIC(20, 8),                  -- Number of shares purchased

    -- Bot decision metrics
    time_elapsed_s  INTEGER NOT NULL,                -- Seconds into window when bet placed
    price_delta     NUMERIC(20, 8) NOT NULL,         -- BTC price delta from open
    edge_pct        NUMERIC(10, 6) NOT NULL,         -- Calculated edge percentage
    our_probability NUMERIC(10, 6) NOT NULL,         -- Our P(direction)
    market_probability NUMERIC(10, 6) NOT NULL,      -- Market implied probability
    confidence_level VARCHAR(20) NOT NULL,           -- Strong/Moderate/Weak
    kelly_fraction  NUMERIC(10, 6),                  -- Kelly fraction used

    -- Transaction info
    tx_hash         VARCHAR(100),                    -- Blockchain transaction hash
    order_id        VARCHAR(100),                    -- Polymarket order ID

    -- Timestamps
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Indexes for common queries
    CONSTRAINT valid_direction CHECK (direction IN ('UP', 'DOWN')),
    CONSTRAINT valid_confidence CHECK (confidence_level IN ('Strong', 'Moderate', 'Weak', 'Unreliable'))
);

CREATE INDEX IF NOT EXISTS idx_bot_trades_window ON bot_trades(window_start);
CREATE INDEX IF NOT EXISTS idx_bot_trades_market ON bot_trades(market_id);
CREATE INDEX IF NOT EXISTS idx_bot_trades_created ON bot_trades(created_at);


-- Table 2: market_outcomes
-- Records the result of each 15-minute market
CREATE TABLE IF NOT EXISTS market_outcomes (
    id              SERIAL PRIMARY KEY,

    -- Market identification
    market_id       VARCHAR(100),                    -- Polymarket condition ID
    window_start    TIMESTAMPTZ NOT NULL UNIQUE,     -- Start of 15-min window
    window_end      TIMESTAMPTZ NOT NULL,            -- End of 15-min window

    -- BTC prices
    btc_open_price  NUMERIC(20, 8) NOT NULL,         -- BTC price at window start
    btc_close_price NUMERIC(20, 8) NOT NULL,         -- BTC price at window end
    btc_high_price  NUMERIC(20, 8),                  -- High during window
    btc_low_price   NUMERIC(20, 8),                  -- Low during window

    -- Outcome
    outcome         VARCHAR(10) NOT NULL,            -- 'UP' or 'DOWN'
    price_change    NUMERIC(20, 8) NOT NULL,         -- Close - Open
    price_change_pct NUMERIC(10, 6) NOT NULL,        -- % change

    -- Resolution
    resolved_at     TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT valid_outcome CHECK (outcome IN ('UP', 'DOWN'))
);

CREATE INDEX IF NOT EXISTS idx_market_outcomes_window ON market_outcomes(window_start);


-- Table 3: trade_results
-- Links trades to outcomes and calculates P&L
-- (Could be a view, but table is more efficient for querying)
CREATE TABLE IF NOT EXISTS trade_results (
    id              SERIAL PRIMARY KEY,
    trade_id        INTEGER REFERENCES bot_trades(id) ON DELETE CASCADE,
    outcome_id      INTEGER REFERENCES market_outcomes(id) ON DELETE CASCADE,

    -- Result
    won             BOOLEAN NOT NULL,                -- Did we win?
    payout_usdc     NUMERIC(20, 8) NOT NULL,         -- Payout received (0 if lost)
    pnl_usdc        NUMERIC(20, 8) NOT NULL,         -- Profit/Loss = payout - amount
    roi_pct         NUMERIC(10, 6) NOT NULL,         -- ROI = pnl / amount * 100

    -- Timestamps
    calculated_at   TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_results_trade ON trade_results(trade_id);
CREATE INDEX IF NOT EXISTS idx_trade_results_won ON trade_results(won);


-- ═══════════════════════════════════════════════════════════════════════════════
-- Views for easy querying
-- ═══════════════════════════════════════════════════════════════════════════════

-- View: Full trade details with outcome
CREATE OR REPLACE VIEW v_trade_summary AS
SELECT
    t.id AS trade_id,
    t.window_start,
    t.direction,
    t.amount_usdc,
    t.entry_price,
    t.edge_pct,
    t.our_probability,
    t.market_probability,
    t.confidence_level,
    t.time_elapsed_s,
    t.price_delta,
    o.outcome AS market_outcome,
    o.btc_open_price,
    o.btc_close_price,
    o.price_change,
    r.won,
    r.payout_usdc,
    r.pnl_usdc,
    r.roi_pct,
    t.tx_hash,
    t.created_at
FROM bot_trades t
LEFT JOIN market_outcomes o ON t.window_start = o.window_start
LEFT JOIN trade_results r ON t.id = r.trade_id
ORDER BY t.window_start DESC;


-- View: Daily P&L summary
CREATE OR REPLACE VIEW v_daily_pnl AS
SELECT
    DATE(t.window_start) AS trade_date,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN r.won THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN NOT r.won THEN 1 ELSE 0 END) AS losses,
    ROUND(SUM(CASE WHEN r.won THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS win_rate_pct,
    SUM(t.amount_usdc) AS total_wagered,
    SUM(r.payout_usdc) AS total_payout,
    SUM(r.pnl_usdc) AS net_pnl,
    ROUND(SUM(r.pnl_usdc) / NULLIF(SUM(t.amount_usdc), 0) * 100, 2) AS roi_pct
FROM bot_trades t
LEFT JOIN trade_results r ON t.id = r.trade_id
GROUP BY DATE(t.window_start)
ORDER BY trade_date DESC;


-- View: Performance by confidence level
CREATE OR REPLACE VIEW v_performance_by_confidence AS
SELECT
    t.confidence_level,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN r.won THEN 1 ELSE 0 END) AS wins,
    ROUND(SUM(CASE WHEN r.won THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS win_rate_pct,
    ROUND(AVG(t.edge_pct) * 100, 2) AS avg_edge_pct,
    SUM(t.amount_usdc) AS total_wagered,
    SUM(r.pnl_usdc) AS net_pnl,
    ROUND(SUM(r.pnl_usdc) / NULLIF(SUM(t.amount_usdc), 0) * 100, 2) AS roi_pct
FROM bot_trades t
LEFT JOIN trade_results r ON t.id = r.trade_id
WHERE r.won IS NOT NULL
GROUP BY t.confidence_level
ORDER BY t.confidence_level;


-- View: Performance by edge bucket
CREATE OR REPLACE VIEW v_performance_by_edge AS
SELECT
    CASE
        WHEN t.edge_pct < 0.05 THEN '0-5%'
        WHEN t.edge_pct < 0.10 THEN '5-10%'
        WHEN t.edge_pct < 0.15 THEN '10-15%'
        WHEN t.edge_pct < 0.20 THEN '15-20%'
        ELSE '20%+'
    END AS edge_bucket,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN r.won THEN 1 ELSE 0 END) AS wins,
    ROUND(SUM(CASE WHEN r.won THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS win_rate_pct,
    SUM(t.amount_usdc) AS total_wagered,
    SUM(r.pnl_usdc) AS net_pnl,
    ROUND(SUM(r.pnl_usdc) / NULLIF(SUM(t.amount_usdc), 0) * 100, 2) AS roi_pct
FROM bot_trades t
LEFT JOIN trade_results r ON t.id = r.trade_id
WHERE r.won IS NOT NULL
GROUP BY edge_bucket
ORDER BY edge_bucket;
