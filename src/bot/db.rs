use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

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

impl TradeDb {
    /// Connect to the database
    pub async fn connect(database_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .context("Failed to connect to database")?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("Database connection error: {}", e);
            }
        });

        info!("Connected to trade database");
        Ok(Self { client })
    }

    /// Run migrations
    pub async fn run_migrations(&self) -> Result<()> {
        let migration = include_str!("../../migrations/001_bot_trades.sql");

        self.client
            .batch_execute(migration)
            .await
            .context("Failed to run migrations")?;

        info!("Database migrations complete");
        Ok(())
    }

    /// Insert a new trade
    pub async fn insert_trade(&self, trade: &TradeRecord) -> Result<i32> {
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
                    &trade.amount_usdc,
                    &trade.entry_price,
                    &trade.shares,
                    &trade.time_elapsed_s,
                    &trade.price_delta,
                    &trade.edge_pct,
                    &trade.our_probability,
                    &trade.market_probability,
                    &trade.confidence_level,
                    &trade.kelly_fraction,
                    &trade.tx_hash,
                    &trade.order_id,
                ],
            )
            .await
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
