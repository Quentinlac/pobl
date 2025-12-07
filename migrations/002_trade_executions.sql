-- ═══════════════════════════════════════════════════════════════════════════════
-- BTC 15-Minute Bot - Trade Execution Tracking (FAK Orders)
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- This tracks each order execution (buy and sell) with proper linking.
-- A position has one BUY and potentially one SELL, linked by position_id.
--
-- FAK (Fill-And-Kill) orders may partially fill, so we track:
--   - requested_amount: what we asked for
--   - filled_amount: what we actually got
--   - status: FILLED, PARTIAL, CANCELLED, FAILED
-- ═══════════════════════════════════════════════════════════════════════════════

-- Table: trade_executions
-- Records each order execution (BUY or SELL)
CREATE TABLE IF NOT EXISTS trade_executions (
    id                  SERIAL PRIMARY KEY,

    -- Position linking (UUID to link buy/sell)
    position_id         VARCHAR(36) NOT NULL,           -- UUID linking buy/sell pair

    -- Execution type
    side                VARCHAR(4) NOT NULL,            -- 'BUY' or 'SELL'

    -- Market context
    market_slug         VARCHAR(100),                   -- e.g., 'btc-updown-15m-1765142100'
    token_id            VARCHAR(100) NOT NULL,          -- Polymarket token ID
    direction           VARCHAR(4) NOT NULL,            -- 'UP' or 'DOWN'
    window_start        TIMESTAMPTZ NOT NULL,           -- 15-min window start

    -- Order details (requested)
    order_type          VARCHAR(10) NOT NULL,           -- 'FOK', 'FAK', 'GTD', 'GTC'
    requested_price     NUMERIC(10, 6) NOT NULL,        -- Price we requested (0.00-1.00)
    requested_amount    NUMERIC(20, 8) NOT NULL,        -- Amount requested (USDC for BUY, shares for SELL)
    requested_shares    NUMERIC(20, 8),                 -- Shares requested (for BUY: amount/price)

    -- Order result (filled)
    filled_price        NUMERIC(10, 6),                 -- Actual fill price
    filled_amount       NUMERIC(20, 8),                 -- Amount filled (USDC for BUY, shares for SELL)
    filled_shares       NUMERIC(20, 8),                 -- Shares filled

    -- Execution status
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING, FILLED, PARTIAL, CANCELLED, FAILED
    error_message       TEXT,                           -- Error message if failed

    -- Polymarket order info
    order_id            VARCHAR(100),                   -- Polymarket order ID
    tx_hash             VARCHAR(100),                   -- Blockchain transaction hash

    -- Decision metrics (for BUY orders)
    time_elapsed_s      INTEGER,                        -- Seconds into window
    btc_price           NUMERIC(20, 8),                 -- BTC price at decision time
    btc_delta           NUMERIC(20, 8),                 -- BTC delta from window open
    edge_pct            NUMERIC(10, 6),                 -- Calculated edge (for BUY)
    our_probability     NUMERIC(10, 6),                 -- Our P(direction)
    market_probability  NUMERIC(10, 6),                 -- Market mid price
    best_ask            NUMERIC(10, 6),                 -- Best ask at execution time
    best_bid            NUMERIC(10, 6),                 -- Best bid at execution time
    ask_liquidity       NUMERIC(20, 8),                 -- Available ask liquidity (shares)
    bid_liquidity       NUMERIC(20, 8),                 -- Available bid liquidity (shares)

    -- Sell-specific metrics
    sell_edge_pct       NUMERIC(10, 6),                 -- Sell edge (for SELL orders)
    profit_pct          NUMERIC(10, 6),                 -- Profit % at sell time
    entry_price         NUMERIC(10, 6),                 -- Original entry price (for SELL)

    -- Timestamps
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    filled_at           TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT valid_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT valid_direction CHECK (direction IN ('UP', 'DOWN')),
    CONSTRAINT valid_status CHECK (status IN ('PENDING', 'FILLED', 'PARTIAL', 'CANCELLED', 'FAILED')),
    CONSTRAINT valid_order_type CHECK (order_type IN ('FOK', 'FAK', 'GTD', 'GTC'))
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_executions_position ON trade_executions(position_id);
CREATE INDEX IF NOT EXISTS idx_executions_window ON trade_executions(window_start);
CREATE INDEX IF NOT EXISTS idx_executions_status ON trade_executions(status);
CREATE INDEX IF NOT EXISTS idx_executions_side ON trade_executions(side);
CREATE INDEX IF NOT EXISTS idx_executions_created ON trade_executions(created_at);

-- ═══════════════════════════════════════════════════════════════════════════════
-- Views for monitoring
-- ═══════════════════════════════════════════════════════════════════════════════

-- View: Position summary (buy + sell linked)
CREATE OR REPLACE VIEW v_positions AS
SELECT
    b.position_id,
    b.direction,
    b.window_start,
    b.market_slug,

    -- Buy info
    b.id AS buy_id,
    b.requested_amount AS buy_requested_usdc,
    b.filled_amount AS buy_filled_usdc,
    b.filled_shares AS buy_shares,
    b.filled_price AS buy_price,
    b.status AS buy_status,
    b.edge_pct AS buy_edge,
    b.created_at AS buy_time,

    -- Sell info
    s.id AS sell_id,
    s.filled_amount AS sell_filled_shares,
    s.filled_price AS sell_price,
    s.status AS sell_status,
    s.sell_edge_pct,
    s.profit_pct AS sell_profit_pct,
    s.created_at AS sell_time,

    -- Calculated P&L
    CASE
        WHEN s.status = 'FILLED' THEN
            (s.filled_price - b.filled_price) * b.filled_shares
        WHEN s.status = 'PARTIAL' THEN
            (s.filled_price - b.filled_price) * COALESCE(s.filled_shares, 0)
        ELSE NULL
    END AS realized_pnl,

    -- Position status
    CASE
        WHEN b.status != 'FILLED' AND b.status != 'PARTIAL' THEN 'FAILED_BUY'
        WHEN s.id IS NULL THEN 'OPEN'
        WHEN s.status = 'FILLED' THEN 'CLOSED'
        WHEN s.status = 'PARTIAL' THEN 'PARTIALLY_CLOSED'
        ELSE 'PENDING_SELL'
    END AS position_status

FROM trade_executions b
LEFT JOIN trade_executions s ON b.position_id = s.position_id AND s.side = 'SELL'
WHERE b.side = 'BUY'
ORDER BY b.created_at DESC;


-- View: Execution stats by order type
CREATE OR REPLACE VIEW v_execution_stats AS
SELECT
    order_type,
    side,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) AS filled,
    SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END) AS partial,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS fill_rate_pct,
    SUM(requested_amount) AS total_requested,
    SUM(filled_amount) AS total_filled,
    ROUND(SUM(filled_amount) / NULLIF(SUM(requested_amount), 0) * 100, 2) AS fill_amount_pct
FROM trade_executions
GROUP BY order_type, side
ORDER BY order_type, side;


-- View: Daily execution summary
CREATE OR REPLACE VIEW v_daily_executions AS
SELECT
    DATE(created_at) AS date,
    COUNT(*) FILTER (WHERE side = 'BUY') AS buys,
    COUNT(*) FILTER (WHERE side = 'SELL') AS sells,
    COUNT(*) FILTER (WHERE status = 'FILLED') AS filled,
    COUNT(*) FILTER (WHERE status = 'PARTIAL') AS partial,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
    SUM(filled_amount) FILTER (WHERE side = 'BUY' AND status IN ('FILLED', 'PARTIAL')) AS total_bought_usdc,
    SUM(filled_shares) FILTER (WHERE side = 'SELL' AND status IN ('FILLED', 'PARTIAL')) AS total_sold_shares
FROM trade_executions
GROUP BY DATE(created_at)
ORDER BY date DESC;
