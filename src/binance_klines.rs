//! Binance Klines Fetcher
//!
//! Fetches historical BTC/USDT klines (candlesticks) from Binance REST API
//! and stores them in PostgreSQL for matrix building.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tokio_postgres::Client;
use tracing::{debug, info, warn};

const BINANCE_API_URL: &str = "https://api.binance.com";
const SYMBOL: &str = "BTCUSDT";
const DB_SYMBOL: &str = "BTCUSDT";

/// Kline data from Binance API
#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub close_time: DateTime<Utc>,
    pub quote_volume: Decimal,
    pub num_trades: i64,
}

/// Binance klines client
pub struct BinanceKlinesClient {
    http: reqwest::Client,
}

impl BinanceKlinesClient {
    pub fn new(timeout_ms: u64) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()?;

        Ok(Self { http })
    }

    /// Fetch klines for a time range
    /// Binance limit: 1000 klines per request
    pub async fn fetch_klines(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<Kline>> {
        let url = format!(
            "{}/api/v3/klines?symbol={}&interval=1m&startTime={}&endTime={}&limit=1000",
            BINANCE_API_URL,
            SYMBOL,
            start_time.timestamp_millis(),
            end_time.timestamp_millis()
        );

        let response = self.http.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Binance API error: {} - {}", status, text));
        }

        // Binance returns klines as arrays of arrays
        let data: Vec<Vec<serde_json::Value>> = response.json().await?;

        let mut klines = Vec::with_capacity(data.len());
        for row in data {
            if row.len() < 11 {
                continue;
            }

            let open_time_ms = row[0].as_i64().unwrap_or(0);
            let close_time_ms = row[6].as_i64().unwrap_or(0);

            let kline = Kline {
                open_time: Utc.timestamp_millis_opt(open_time_ms).unwrap(),
                open: parse_decimal(&row[1])?,
                high: parse_decimal(&row[2])?,
                low: parse_decimal(&row[3])?,
                close: parse_decimal(&row[4])?,
                volume: parse_decimal(&row[5])?,
                close_time: Utc.timestamp_millis_opt(close_time_ms).unwrap(),
                quote_volume: parse_decimal(&row[7])?,
                num_trades: row[8].as_i64().unwrap_or(0),
            };

            klines.push(kline);
        }

        Ok(klines)
    }

    /// Fetch all klines from start_date to end_date
    /// Handles pagination automatically (1000 klines per request)
    pub async fn fetch_all_klines(
        &self,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        progress_callback: Option<&dyn Fn(usize, usize)>,
    ) -> Result<Vec<Kline>> {
        let total_minutes = (end_date - start_date).num_minutes() as usize;
        let total_requests = (total_minutes + 999) / 1000;

        info!(
            "Fetching {} minutes of klines ({} requests)",
            total_minutes, total_requests
        );

        let mut all_klines = Vec::new();
        let mut current_start = start_date;
        let mut request_count = 0;

        while current_start < end_date {
            // Each request fetches up to 1000 1-minute klines
            let batch_end = (current_start + Duration::minutes(1000)).min(end_date);

            let klines = self.fetch_klines(current_start, batch_end).await?;
            let count = klines.len();
            all_klines.extend(klines);

            request_count += 1;
            if let Some(cb) = progress_callback {
                cb(request_count, total_requests);
            }

            if count == 0 {
                warn!("No klines returned for range {:?} to {:?}", current_start, batch_end);
                current_start = batch_end;
            } else {
                // Move to next batch (use last kline's time + 1 minute to avoid duplicates)
                current_start = batch_end;
            }

            // Rate limiting: 1200 requests/minute, so ~20/second max
            // Be conservative: 5 requests/second
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        info!("Fetched {} total klines", all_klines.len());
        Ok(all_klines)
    }
}

fn parse_decimal(value: &serde_json::Value) -> Result<Decimal> {
    let s = value.as_str().unwrap_or("0");
    Decimal::from_str(s).context("Failed to parse decimal")
}

/// Insert klines into the database
pub async fn insert_klines(client: &Client, klines: &[Kline]) -> Result<u64> {
    if klines.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0u64;

    // Use batched inserts for efficiency
    for chunk in klines.chunks(1000) {
        let mut query = String::from(
            "INSERT INTO binance_klines (symbol, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, num_trades) VALUES "
        );

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();
        let mut param_idx = 1;

        for (i, kline) in chunk.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3,
                param_idx + 4,
                param_idx + 5,
                param_idx + 6,
                param_idx + 7,
                param_idx + 8
            ));
            param_idx += 9;

            params.push(Box::new(DB_SYMBOL.to_string()));
            params.push(Box::new(kline.open_time));
            params.push(Box::new(kline.open));
            params.push(Box::new(kline.high));
            params.push(Box::new(kline.low));
            params.push(Box::new(kline.close));
            params.push(Box::new(kline.volume));
            params.push(Box::new(kline.quote_volume));
            params.push(Box::new(kline.num_trades));
        }

        query.push_str(" ON CONFLICT (symbol, timestamp) DO NOTHING");

        // Convert to references
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = client.execute(&query, &param_refs[..]).await?;
        inserted += rows;
    }

    Ok(inserted)
}

/// Get the latest timestamp in the database
pub async fn get_latest_timestamp(client: &Client) -> Result<Option<DateTime<Utc>>> {
    let row = client
        .query_opt(
            "SELECT MAX(timestamp) FROM binance_klines WHERE symbol = $1",
            &[&DB_SYMBOL],
        )
        .await?;

    match row {
        Some(row) => {
            let ts: Option<DateTime<Utc>> = row.get(0);
            Ok(ts)
        }
        None => Ok(None),
    }
}

/// Get the earliest timestamp in the database
pub async fn get_earliest_timestamp(client: &Client) -> Result<Option<DateTime<Utc>>> {
    let row = client
        .query_opt(
            "SELECT MIN(timestamp) FROM binance_klines WHERE symbol = $1",
            &[&DB_SYMBOL],
        )
        .await?;

    match row {
        Some(row) => {
            let ts: Option<DateTime<Utc>> = row.get(0);
            Ok(ts)
        }
        None => Ok(None),
    }
}

/// Get count of klines in database
pub async fn get_kline_count(client: &Client) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM binance_klines WHERE symbol = $1",
            &[&DB_SYMBOL],
        )
        .await?;

    Ok(row.get(0))
}

/// Find gaps in the kline data
/// Returns list of (gap_start, gap_end) tuples where data is missing
pub async fn find_gaps(client: &Client) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let rows = client
        .query(
            r#"
            WITH time_diffs AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM binance_klines
                WHERE symbol = $1
            )
            SELECT
                prev_timestamp as gap_start,
                timestamp as gap_end,
                EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 60 as gap_minutes
            FROM time_diffs
            WHERE EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) > 120
            ORDER BY prev_timestamp
            LIMIT 100
            "#,
            &[&DB_SYMBOL],
        )
        .await?;

    let gaps: Vec<(DateTime<Utc>, DateTime<Utc>)> = rows
        .iter()
        .map(|row| {
            let start: DateTime<Utc> = row.get(0);
            let end: DateTime<Utc> = row.get(1);
            (start, end)
        })
        .collect();

    Ok(gaps)
}

/// Fill gaps in the data
pub async fn fill_gaps(client: &Client, binance_client: &BinanceKlinesClient) -> Result<u64> {
    let gaps = find_gaps(client).await?;

    if gaps.is_empty() {
        info!("No gaps found in data");
        return Ok(0);
    }

    info!("Found {} gaps to fill", gaps.len());

    let mut total_inserted = 0u64;

    for (gap_start, gap_end) in gaps {
        let gap_minutes = (gap_end - gap_start).num_minutes();
        info!(
            "Filling gap: {:?} to {:?} ({} minutes)",
            gap_start, gap_end, gap_minutes
        );

        let klines = binance_client
            .fetch_klines(gap_start + Duration::minutes(1), gap_end)
            .await?;

        let inserted = insert_klines(client, &klines).await?;
        total_inserted += inserted;

        info!("Inserted {} klines for gap", inserted);

        // Rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    Ok(total_inserted)
}

/// Update klines with new data since last fetch
pub async fn update_klines(client: &Client, binance_client: &BinanceKlinesClient) -> Result<u64> {
    let latest = get_latest_timestamp(client).await?;
    let now = Utc::now();

    let start_time = match latest {
        Some(ts) => ts + Duration::minutes(1), // Start from next minute
        None => {
            // No data - fetch last 6 months
            now - Duration::days(180)
        }
    };

    if start_time >= now {
        info!("Data is up to date");
        return Ok(0);
    }

    let gap_minutes = (now - start_time).num_minutes();
    info!(
        "Updating klines from {:?} to {:?} ({} minutes)",
        start_time, now, gap_minutes
    );

    let klines = binance_client
        .fetch_all_klines(start_time, now, Some(&|current, total| {
            if current % 10 == 0 || current == total {
                info!("Progress: {}/{} requests", current, total);
            }
        }))
        .await?;

    let inserted = insert_klines(client, &klines).await?;

    // Also fill any gaps
    let gap_filled = fill_gaps(client, binance_client).await?;

    Ok(inserted + gap_filled)
}

/// Run the database migration
pub async fn run_migration(client: &Client) -> Result<()> {
    let migration = include_str!("../migrations/004_binance_klines.sql");
    client.batch_execute(migration).await?;
    info!("Binance klines migration complete");
    Ok(())
}
