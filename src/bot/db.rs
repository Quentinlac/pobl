use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::Decimal;
use tokio_postgres::Client;
use tracing::{info, warn};

/// Convert f64 to Decimal for PostgreSQL NUMERIC columns
fn to_dec(v: f64) -> Decimal {
    Decimal::try_from(v).unwrap_or_default()
}

/// Convert Option<f64> to Option<Decimal>
fn to_dec_opt(v: Option<f64>) -> Option<Decimal> {
    v.map(|x| Decimal::try_from(x).unwrap_or_default())
}

/// Default database configuration (Qovery PostgreSQL)
const DEFAULT_DB_HOST: &str = "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com";
const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_USER: &str = "qoveryadmin";
const DEFAULT_DB_PASSWORD: &str = "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp";
const DEFAULT_DB_NAME: &str = "polymarket";

/// Database client for trade tracking
pub struct TradeDb {
    client: Client,
}

/// Trade record to insert
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub market_id: Option<String>,
    pub window_start: DateTime<Utc>,
    pub direction: String,
    pub amount_usdc: f64,
    pub entry_price: f64,
    pub shares: Option<f64>,
    pub time_elapsed_s: i32,
    pub price_delta: f64,
    pub edge_pct: f64,
    pub our_probability: f64,
    pub market_probability: f64,
    pub confidence_level: String,
    pub kelly_fraction: Option<f64>,
    pub tx_hash: Option<String>,
    pub order_id: Option<String>,
}

/// Execution record for FAK/FOK orders (tracks buy/sell pairs)
#[derive(Debug, Clone)]
pub struct ExecutionRecord {
    pub position_id: String,        // UUID linking buy/sell pair
    pub side: String,               // "BUY" or "SELL"
    pub market_slug: Option<String>,
    pub token_id: String,
    pub direction: String,          // "UP" or "DOWN"
    pub window_start: DateTime<Utc>,

    // Order details
    pub order_type: String,         // "FOK", "FAK", "GTD", "GTC"
    pub requested_price: f64,
    pub requested_amount: f64,      // USDC for BUY, shares for SELL
    pub requested_shares: Option<f64>,

    // Fill results
    pub filled_price: Option<f64>,
    pub filled_amount: Option<f64>,
    pub filled_shares: Option<f64>,
    pub status: String,             // "PENDING", "FILLED", "PARTIAL", "CANCELLED", "FAILED"
    pub error_message: Option<String>,

    // Order info
    pub order_id: Option<String>,

    // Decision metrics (for BUY)
    pub time_elapsed_s: Option<i32>,
    pub btc_price: Option<f64>,
    pub btc_delta: Option<f64>,
    pub edge_pct: Option<f64>,
    pub our_probability: Option<f64>,
    pub market_probability: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_bid: Option<f64>,
    pub ask_liquidity: Option<f64>,
    pub bid_liquidity: Option<f64>,

    // Sell-specific
    pub sell_edge_pct: Option<f64>,
    pub profit_pct: Option<f64>,
    pub entry_price: Option<f64>,
}

/// Market outcome record
#[derive(Debug, Clone)]
pub struct MarketOutcome {
    pub market_id: Option<String>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub btc_open_price: f64,
    pub btc_close_price: f64,
    pub btc_high_price: Option<f64>,
    pub btc_low_price: Option<f64>,
    pub outcome: String,
    pub price_change: f64,
    pub price_change_pct: f64,
}

/// Trade attempt record - logs every trade attempt for analysis
#[derive(Debug, Clone)]
pub struct TradeAttempt {
    pub market_slug: Option<String>,
    pub token_id: String,
    pub direction: String,              // UP/DOWN
    pub side: String,                   // BUY/SELL
    pub strategy_type: Option<String>,  // TERMINAL/EXIT
    pub order_type: String,             // FOK/GTC/GTD

    // Prices at attempt time
    pub our_probability: Option<f64>,
    pub market_price: f64,              // ask for BUY, bid for SELL
    pub edge: Option<f64>,

    // Order details
    pub bet_amount_usdc: f64,
    pub shares: f64,
    pub slippage_price: Option<f64>,    // price with slippage applied

    // Context
    pub btc_price: Option<f64>,
    pub price_delta: Option<f64>,
    pub time_elapsed_secs: Option<i32>,
    pub time_remaining_secs: Option<i32>,

    // Result
    pub success: bool,
    pub error_message: Option<String>,
    pub order_id: Option<String>,

    // For joining with market_logs
    pub time_bucket: Option<i32>,
    pub delta_bucket: Option<i32>,

    // For simulation vs reality comparison
    pub expected_fill_price: Option<f64>,
    pub actual_fill_price: Option<f64>,
    pub slippage_pct: Option<f64>,
}

impl TradeDb {
    /// Connect to the database using default hardcoded config with TLS
    pub async fn connect(_database_url: &str) -> Result<Self> {
        // Use hardcoded config (same as logger_ws)
        let connection_string = format!(
            "host={} port={} user={} password={} dbname={}",
            DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD, DEFAULT_DB_NAME
        );

        // Create TLS connector (accept invalid certs for internal connections)
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .context("Failed to create TLS connector")?;
        let connector = MakeTlsConnector::new(connector);

        let (client, connection) = tokio_postgres::connect(&connection_string, connector)
            .await
            .context("Failed to connect to database")?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("Database connection error: {}", e);
            }
        });

        info!("Connected to trade database (Qovery PostgreSQL)");
        Ok(Self { client })
    }

    /// Run migrations
    pub async fn run_migrations(&self) -> Result<()> {
        // Run v1 migration (bot_trades, market_outcomes, trade_results)
        let migration_v1 = include_str!("../../migrations/001_bot_trades.sql");
        self.client
            .batch_execute(migration_v1)
            .await
            .context("Failed to run migration v1")?;

        // Run v2 migration (trade_executions for FOK tracking)
        let migration_v2 = include_str!("../../migrations/002_trade_executions.sql");
        self.client
            .batch_execute(migration_v2)
            .await
            .context("Failed to run migration v2")?;

        // Run v4 migration (trade_attempts for analysis)
        let migration_v4 = include_str!("../../migrations/004_trade_attempts.sql");
        self.client
            .batch_execute(migration_v4)
            .await
            .context("Failed to run migration v4")?;

        info!("Database migrations complete (v1 + v2 + v4)");
        Ok(())
    }

    /// Insert execution record (for FOK buy/sell tracking)
    pub async fn insert_execution(&self, exec: &ExecutionRecord) -> Result<i32> {
        // Convert f64 to Decimal for PostgreSQL NUMERIC columns
        let requested_price = to_dec(exec.requested_price);
        let requested_amount = to_dec(exec.requested_amount);
        let requested_shares = to_dec_opt(exec.requested_shares);
        let filled_price = to_dec_opt(exec.filled_price);
        let filled_amount = to_dec_opt(exec.filled_amount);
        let filled_shares = to_dec_opt(exec.filled_shares);
        let btc_price = to_dec_opt(exec.btc_price);
        let btc_delta = to_dec_opt(exec.btc_delta);
        let edge_pct = to_dec_opt(exec.edge_pct);
        let our_probability = to_dec_opt(exec.our_probability);
        let market_probability = to_dec_opt(exec.market_probability);
        let best_ask = to_dec_opt(exec.best_ask);
        let best_bid = to_dec_opt(exec.best_bid);
        let ask_liquidity = to_dec_opt(exec.ask_liquidity);
        let bid_liquidity = to_dec_opt(exec.bid_liquidity);
        let sell_edge_pct = to_dec_opt(exec.sell_edge_pct);
        let profit_pct = to_dec_opt(exec.profit_pct);
        let entry_price = to_dec_opt(exec.entry_price);

        let row = self.client
            .query_one(
                r#"
                INSERT INTO trade_executions (
                    position_id, side, market_slug, token_id, direction, window_start,
                    order_type, requested_price, requested_amount, requested_shares,
                    filled_price, filled_amount, filled_shares, status, error_message,
                    order_id, time_elapsed_s, btc_price, btc_delta, edge_pct,
                    our_probability, market_probability, best_ask, best_bid,
                    ask_liquidity, bid_liquidity, sell_edge_pct, profit_pct, entry_price
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29
                )
                RETURNING id
                "#,
                &[
                    &exec.position_id,
                    &exec.side,
                    &exec.market_slug,
                    &exec.token_id,
                    &exec.direction,
                    &exec.window_start,
                    &exec.order_type,
                    &requested_price,
                    &requested_amount,
                    &requested_shares,
                    &filled_price,
                    &filled_amount,
                    &filled_shares,
                    &exec.status,
                    &exec.error_message,
                    &exec.order_id,
                    &exec.time_elapsed_s,
                    &btc_price,
                    &btc_delta,
                    &edge_pct,
                    &our_probability,
                    &market_probability,
                    &best_ask,
                    &best_bid,
                    &ask_liquidity,
                    &bid_liquidity,
                    &sell_edge_pct,
                    &profit_pct,
                    &entry_price,
                ],
            )
            .await
            .context("Failed to insert execution")?;

        let id: i32 = row.get(0);
        info!("Recorded execution #{} ({} {})", id, exec.side, exec.direction);
        Ok(id)
    }

    /// Insert trade attempt record (for analysis - logs ALL attempts)
    pub async fn insert_trade_attempt(&self, attempt: &TradeAttempt) -> Result<i32> {
        // Convert f64 to Decimal for PostgreSQL NUMERIC columns
        let our_probability = to_dec_opt(attempt.our_probability);
        let market_price = to_dec(attempt.market_price);
        let edge = to_dec_opt(attempt.edge);
        let bet_amount_usdc = to_dec(attempt.bet_amount_usdc);
        let shares = to_dec(attempt.shares);
        let slippage_price = to_dec_opt(attempt.slippage_price);
        let btc_price = to_dec_opt(attempt.btc_price);
        let price_delta = to_dec_opt(attempt.price_delta);
        let expected_fill_price = to_dec_opt(attempt.expected_fill_price);
        let actual_fill_price = to_dec_opt(attempt.actual_fill_price);
        let slippage_pct = to_dec_opt(attempt.slippage_pct);

        let row = self.client
            .query_one(
                r#"
                INSERT INTO trade_attempts (
                    market_slug, token_id, direction, side, strategy_type, order_type,
                    our_probability, market_price, edge,
                    bet_amount_usdc, shares, slippage_price,
                    btc_price, price_delta, time_elapsed_secs, time_remaining_secs,
                    success, error_message, order_id,
                    time_bucket, delta_bucket,
                    expected_fill_price, actual_fill_price, slippage_pct
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
                    $22, $23, $24
                )
                RETURNING id
                "#,
                &[
                    &attempt.market_slug,
                    &attempt.token_id,
                    &attempt.direction,
                    &attempt.side,
                    &attempt.strategy_type,
                    &attempt.order_type,
                    &our_probability,
                    &market_price,
                    &edge,
                    &bet_amount_usdc,
                    &shares,
                    &slippage_price,
                    &btc_price,
                    &price_delta,
                    &attempt.time_elapsed_secs,
                    &attempt.time_remaining_secs,
                    &attempt.success,
                    &attempt.error_message,
                    &attempt.order_id,
                    &attempt.time_bucket,
                    &attempt.delta_bucket,
                    &expected_fill_price,
                    &actual_fill_price,
                    &slippage_pct,
                ],
            )
            .await
            .map_err(|e| {
                warn!("DB INSERT ATTEMPT ERROR: {} | direction={} side={} success={}",
                    e, attempt.direction, attempt.side, attempt.success);
                e
            })
            .context("Failed to insert trade attempt")?;

        let id: i32 = row.get(0);
        Ok(id)
    }

    /// Update execution status after order result
    pub async fn update_execution_status(
        &self,
        id: i32,
        status: &str,
        filled_price: Option<f64>,
        filled_amount: Option<f64>,
        filled_shares: Option<f64>,
        order_id: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        // Convert f64 to Decimal for PostgreSQL NUMERIC columns
        let filled_price_dec = to_dec_opt(filled_price);
        let filled_amount_dec = to_dec_opt(filled_amount);
        let filled_shares_dec = to_dec_opt(filled_shares);

        let rows_updated = self.client
            .execute(
                r#"
                UPDATE trade_executions
                SET status = $2,
                    filled_price = COALESCE($3, filled_price),
                    filled_amount = COALESCE($4, filled_amount),
                    filled_shares = COALESCE($5, filled_shares),
                    order_id = COALESCE($6, order_id),
                    error_message = $7,
                    filled_at = CASE WHEN $2 IN ('FILLED', 'PARTIAL') THEN NOW() ELSE filled_at END
                WHERE id = $1
                "#,
                &[&id, &status, &filled_price_dec, &filled_amount_dec, &filled_shares_dec, &order_id, &error_message],
            )
            .await
            .map_err(|e| {
                warn!("DB UPDATE ERROR: {} | id={} status={} price={:?} amount={:?} shares={:?}",
                    e, id, status, filled_price, filled_amount, filled_shares);
                e
            })
            .context("Failed to update execution status")?;

        if rows_updated == 0 {
            warn!("DB UPDATE: No rows updated for execution id={}", id);
        } else {
            info!("Updated execution #{} to status={}", id, status);
        }

        Ok(())
    }

    /// Insert a new trade
    pub async fn insert_trade(&self, trade: &TradeRecord) -> Result<i32> {
        // Convert f64 to Decimal for PostgreSQL NUMERIC columns
        let amount_usdc = to_dec(trade.amount_usdc);
        let entry_price = to_dec(trade.entry_price);
        let shares = to_dec_opt(trade.shares);
        let price_delta = to_dec(trade.price_delta);
        let edge_pct = to_dec(trade.edge_pct);
        let our_probability = to_dec(trade.our_probability);
        let market_probability = to_dec(trade.market_probability);
        let kelly_fraction = to_dec_opt(trade.kelly_fraction);

        let row = self.client
            .query_one(
                r#"
                INSERT INTO bot_trades (
                    market_id, window_start, direction, amount_usdc, entry_price, shares,
                    time_elapsed_s, price_delta, edge_pct, our_probability, market_probability,
                    confidence_level, kelly_fraction, tx_hash, order_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id
                "#,
                &[
                    &trade.market_id,
                    &trade.window_start,
                    &trade.direction,
                    &amount_usdc,
                    &entry_price,
                    &shares,
                    &trade.time_elapsed_s,
                    &price_delta,
                    &edge_pct,
                    &our_probability,
                    &market_probability,
                    &trade.confidence_level,
                    &kelly_fraction,
                    &trade.tx_hash,
                    &trade.order_id,
                ],
            )
            .await
            .map_err(|e| {
                warn!("DB INSERT ERROR: {} | direction={} confidence={} amount={} price={}",
                    e, trade.direction, trade.confidence_level, trade.amount_usdc, trade.entry_price);
                e
            })
            .context("Failed to insert trade")?;

        let id: i32 = row.get(0);
        info!("Recorded trade #{}", id);
        Ok(id)
    }

    /// Insert market outcome
    pub async fn insert_outcome(&self, outcome: &MarketOutcome) -> Result<i32> {
        let row = self.client
            .query_one(
                r#"
                INSERT INTO market_outcomes (
                    market_id, window_start, window_end, btc_open_price, btc_close_price,
                    btc_high_price, btc_low_price, outcome, price_change, price_change_pct
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (window_start) DO UPDATE SET
                    btc_close_price = EXCLUDED.btc_close_price,
                    outcome = EXCLUDED.outcome,
                    price_change = EXCLUDED.price_change,
                    price_change_pct = EXCLUDED.price_change_pct,
                    resolved_at = NOW()
                RETURNING id
                "#,
                &[
                    &outcome.market_id,
                    &outcome.window_start,
                    &outcome.window_end,
                    &outcome.btc_open_price,
                    &outcome.btc_close_price,
                    &outcome.btc_high_price,
                    &outcome.btc_low_price,
                    &outcome.outcome,
                    &outcome.price_change,
                    &outcome.price_change_pct,
                ],
            )
            .await
            .context("Failed to insert outcome")?;

        let id: i32 = row.get(0);
        info!("Recorded market outcome #{}: {}", id, outcome.outcome);
        Ok(id)
    }

    /// Calculate and insert trade results for resolved markets
    pub async fn calculate_trade_results(&self) -> Result<u64> {
        let result = self.client
            .execute(
                r#"
                INSERT INTO trade_results (trade_id, outcome_id, won, payout_usdc, pnl_usdc, roi_pct)
                SELECT
                    t.id AS trade_id,
                    o.id AS outcome_id,
                    (t.direction = o.outcome) AS won,
                    CASE
                        WHEN t.direction = o.outcome THEN t.amount_usdc / t.entry_price
                        ELSE 0
                    END AS payout_usdc,
                    CASE
                        WHEN t.direction = o.outcome THEN (t.amount_usdc / t.entry_price) - t.amount_usdc
                        ELSE -t.amount_usdc
                    END AS pnl_usdc,
                    CASE
                        WHEN t.direction = o.outcome THEN ((1 / t.entry_price) - 1) * 100
                        ELSE -100
                    END AS roi_pct
                FROM bot_trades t
                JOIN market_outcomes o ON t.window_start = o.window_start
                LEFT JOIN trade_results r ON t.id = r.trade_id
                WHERE r.id IS NULL
                ON CONFLICT (trade_id) DO NOTHING
                "#,
                &[],
            )
            .await
            .context("Failed to calculate trade results")?;

        if result > 0 {
            info!("Calculated results for {} trades", result);
        }
        Ok(result)
    }

    /// Get pending trades (trades without results)
    pub async fn get_pending_trades(&self) -> Result<Vec<(i32, DateTime<Utc>)>> {
        let rows = self.client
            .query(
                r#"
                SELECT t.id, t.window_start
                FROM bot_trades t
                LEFT JOIN trade_results r ON t.id = r.trade_id
                WHERE r.id IS NULL
                ORDER BY t.window_start
                "#,
                &[],
            )
            .await
            .context("Failed to get pending trades")?;

        let trades: Vec<(i32, DateTime<Utc>)> = rows
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();

        Ok(trades)
    }

    /// Get daily P&L summary
    pub async fn get_daily_pnl(&self) -> Result<Vec<DailyPnl>> {
        let rows = self.client
            .query(
                r#"
                SELECT
                    trade_date,
                    total_trades,
                    wins,
                    losses,
                    win_rate_pct,
                    total_wagered,
                    total_payout,
                    net_pnl,
                    roi_pct
                FROM v_daily_pnl
                ORDER BY trade_date DESC
                LIMIT 30
                "#,
                &[],
            )
            .await
            .context("Failed to get daily P&L")?;

        let pnl: Vec<DailyPnl> = rows
            .iter()
            .map(|row| DailyPnl {
                trade_date: row.get(0),
                total_trades: row.get(1),
                wins: row.get(2),
                losses: row.get(3),
                win_rate_pct: row.get::<_, Option<rust_decimal::Decimal>>(4)
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                total_wagered: row.get::<_, Option<rust_decimal::Decimal>>(5)
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                total_payout: row.get::<_, Option<rust_decimal::Decimal>>(6)
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                net_pnl: row.get::<_, Option<rust_decimal::Decimal>>(7)
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                roi_pct: row.get::<_, Option<rust_decimal::Decimal>>(8)
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
            })
            .collect();

        Ok(pnl)
    }

    /// Get overall stats
    pub async fn get_overall_stats(&self) -> Result<OverallStats> {
        let row = self.client
            .query_one(
                r#"
                SELECT
                    COUNT(*) AS total_trades,
                    SUM(CASE WHEN r.won THEN 1 ELSE 0 END) AS wins,
                    SUM(t.amount_usdc) AS total_wagered,
                    SUM(r.pnl_usdc) AS net_pnl
                FROM bot_trades t
                LEFT JOIN trade_results r ON t.id = r.trade_id
                "#,
                &[],
            )
            .await
            .context("Failed to get overall stats")?;

        Ok(OverallStats {
            total_trades: row.get::<_, i64>(0),
            wins: row.get::<_, Option<i64>>(1).unwrap_or(0),
            total_wagered: row.get::<_, Option<rust_decimal::Decimal>>(2)
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            net_pnl: row.get::<_, Option<rust_decimal::Decimal>>(3)
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
        })
    }
}

#[derive(Debug)]
pub struct DailyPnl {
    pub trade_date: chrono::NaiveDate,
    pub total_trades: i64,
    pub wins: i64,
    pub losses: i64,
    pub win_rate_pct: f64,
    pub total_wagered: f64,
    pub total_payout: f64,
    pub net_pnl: f64,
    pub roi_pct: f64,
}

#[derive(Debug)]
pub struct OverallStats {
    pub total_trades: i64,
    pub wins: i64,
    pub total_wagered: f64,
    pub net_pnl: f64,
}

/// Matrix snapshot info
#[derive(Debug)]
pub struct MatrixSnapshotInfo {
    pub id: i32,
    pub total_windows: i32,
    pub created_at: DateTime<Utc>,
}

/// Load the latest probability matrix from database
pub async fn load_matrix_from_db(database_url: &str) -> Result<Option<(super::models::ProbabilityMatrix, MatrixSnapshotInfo)>> {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .context("Failed to connect to database for matrix")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("Matrix DB connection error: {}", e);
        }
    });

    let row = client
        .query_opt(
            r#"
            SELECT id, matrix_json, total_windows, created_at
            FROM matrix_snapshots
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            &[],
        )
        .await
        .context("Failed to query matrix")?;

    match row {
        Some(row) => {
            let id: i32 = row.get(0);
            let matrix_json: serde_json::Value = row.get(1);
            let total_windows: i32 = row.get(2);
            let created_at: DateTime<Utc> = row.get(3);

            let matrix: super::models::ProbabilityMatrix = serde_json::from_value(matrix_json)
                .context("Failed to parse matrix JSON from database")?;

            let info = MatrixSnapshotInfo {
                id,
                total_windows,
                created_at,
            };

            Ok(Some((matrix, info)))
        }
        None => Ok(None),
    }
}
