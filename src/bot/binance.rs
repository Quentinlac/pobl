use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

/// Binance API client for BTC price
pub struct BinanceClient {
    client: Client,
    base_url: String,
}

/// Price ticker response
#[derive(Debug, Clone, Deserialize)]
pub struct PriceTicker {
    pub symbol: String,
    pub price: String,
}

/// Kline (candlestick) data
#[derive(Debug, Clone, Deserialize)]
pub struct Kline {
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: i64,
    pub quote_volume: String,
    pub trades: i64,
    pub taker_buy_base: String,
    pub taker_buy_quote: String,
    pub ignore: String,
}

/// Current BTC price info
#[derive(Debug, Clone)]
pub struct BtcPrice {
    pub price: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl BinanceClient {
    pub fn new(timeout_ms: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            base_url: std::env::var("BINANCE_API_URL")
                .unwrap_or_else(|_| "https://api.binance.com".to_string()),
        })
    }

    /// Get current BTC/USDT price
    pub async fn get_btc_price(&self) -> Result<BtcPrice> {
        let url = format!("{}/api/v3/ticker/price?symbol=BTCUSDT", self.base_url);

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch BTC price")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Price request failed: {} - {}", status, text));
        }

        let ticker: PriceTicker = response.json().await
            .context("Failed to parse price response")?;

        let price = ticker.price.parse::<f64>()
            .context("Failed to parse price value")?;

        Ok(BtcPrice {
            price,
            timestamp: chrono::Utc::now(),
        })
    }

    /// Get the price at a specific 15-minute window start
    /// Returns the opening price of the candle that started at window_start
    pub async fn get_window_open_price(&self, window_start: chrono::DateTime<chrono::Utc>) -> Result<f64> {
        let start_ms = window_start.timestamp_millis();
        let end_ms = start_ms + 60_000; // +1 minute to get at least one candle

        let url = format!(
            "{}/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={}&endTime={}&limit=1",
            self.base_url, start_ms, end_ms
        );

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch kline")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Kline request failed: {} - {}", status, text));
        }

        // Binance returns klines as arrays, not objects
        let klines: Vec<Vec<serde_json::Value>> = response.json().await
            .context("Failed to parse kline response")?;

        if klines.is_empty() {
            return Err(anyhow!("No kline data available for window start"));
        }

        // Open price is index 1
        let open_str = klines[0][1].as_str()
            .ok_or_else(|| anyhow!("Invalid open price format"))?;

        let open_price = open_str.parse::<f64>()
            .context("Failed to parse open price")?;

        Ok(open_price)
    }

    /// Get recent 1-second klines for the current window
    pub async fn get_recent_prices(&self, limit: u32) -> Result<Vec<BtcPrice>> {
        // Note: Binance doesn't have 1s klines via REST API, so we use 1m
        // For more granular data, use WebSocket
        let url = format!(
            "{}/api/v3/klines?symbol=BTCUSDT&interval=1m&limit={}",
            self.base_url, limit
        );

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch klines")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Klines request failed: {} - {}", status, text));
        }

        let klines: Vec<Vec<serde_json::Value>> = response.json().await
            .context("Failed to parse klines response")?;

        let mut prices = Vec::new();
        for kline in klines {
            let close_time = kline[6].as_i64().unwrap_or(0);
            let close_str = kline[4].as_str().unwrap_or("0");
            let close_price = close_str.parse::<f64>().unwrap_or(0.0);

            prices.push(BtcPrice {
                price: close_price,
                timestamp: chrono::DateTime::from_timestamp_millis(close_time)
                    .unwrap_or_else(chrono::Utc::now),
            });
        }

        Ok(prices)
    }
}

/// Calculate the start of the current 15-minute window
pub fn get_current_window_start() -> chrono::DateTime<chrono::Utc> {
    let now = chrono::Utc::now();
    let minute = now.minute();
    let window_minute = (minute / 15) * 15;

    now.with_minute(window_minute)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

/// Get time elapsed in the current window (in seconds)
pub fn get_seconds_elapsed() -> u32 {
    let now = chrono::Utc::now();
    let window_start = get_current_window_start();
    let elapsed = now.signed_duration_since(window_start);
    elapsed.num_seconds().max(0) as u32
}

/// Get time remaining in the current window (in seconds)
pub fn get_seconds_remaining() -> u32 {
    let elapsed = get_seconds_elapsed();
    if elapsed >= 900 {
        0
    } else {
        900 - elapsed
    }
}

use chrono::Timelike;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_current_window_start() {
        let start = get_current_window_start();
        let minute = start.minute();
        assert!(minute == 0 || minute == 15 || minute == 30 || minute == 45);
        assert_eq!(start.second(), 0);
    }

    #[test]
    fn test_seconds_elapsed() {
        let elapsed = get_seconds_elapsed();
        assert!(elapsed < 900);
    }

    #[test]
    fn test_seconds_remaining() {
        let remaining = get_seconds_remaining();
        assert!(remaining <= 900);
    }
}
