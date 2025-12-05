//! Chainlink Oracle Price Fetcher
//!
//! Fetches BTC/USD price data from Chainlink oracle on Polygon mainnet
//! using raw JSON-RPC calls.

use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio_postgres::Client;

// Polygon RPC endpoints (free public RPCs work fine for latestRoundData/getRoundData calls)
const POLYGON_RPCS: &[&str] = &[
    "https://polygon-rpc.com",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon.llamarpc.com",
    "https://rpc.ankr.com/polygon",
];

// Chainlink BTC/USD Price Feed on Polygon Mainnet
const BTC_USD_FEED: &str = "0xc907E116054Ad103354f2D350FD2514433D57F6f";

// Function selectors (first 4 bytes of keccak256 hash of function signature)
const LATEST_ROUND_DATA_SELECTOR: &str = "feaf968c"; // latestRoundData()
const GET_ROUND_DATA_SELECTOR: &str = "9a6fc8f5";   // getRoundData(uint80)
const DECIMALS_SELECTOR: &str = "313ce567";          // decimals()

/// Chainlink round data
#[derive(Debug, Clone)]
pub struct RoundData {
    pub round_id: u128,
    pub price: Decimal,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// JSON-RPC request structure
#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    method: &'static str,
    params: Vec<serde_json::Value>,
    id: u32,
}

/// JSON-RPC response structure
#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<String>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    message: String,
}

/// Chainlink client for fetching price data
pub struct ChainlinkClient {
    http: reqwest::Client,
    rpc_url: String,
    decimals: u8,
}

impl ChainlinkClient {
    /// Create a new Chainlink client with custom timeout (in milliseconds)
    pub async fn new_with_timeout(timeout_ms: u64) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()?;

        // Find a working RPC
        let mut rpc_url = String::new();
        for rpc in POLYGON_RPCS {
            if Self::test_rpc(&http, rpc).await {
                rpc_url = rpc.to_string();
                break;
            }
        }

        if rpc_url.is_empty() {
            return Err(anyhow!("Could not connect to any Polygon RPC"));
        }

        // Get decimals
        let decimals = Self::fetch_decimals_static(&http, &rpc_url).await?;

        Ok(Self {
            http,
            rpc_url,
            decimals,
        })
    }

    /// Create a new Chainlink client with default timeout
    pub async fn new() -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        // Find a working RPC
        let mut rpc_url = String::new();
        for rpc in POLYGON_RPCS {
            if Self::test_rpc(&http, rpc).await {
                println!("  Connected to {}", rpc);
                rpc_url = rpc.to_string();
                break;
            }
        }

        if rpc_url.is_empty() {
            return Err(anyhow!("Could not connect to any Polygon RPC"));
        }

        // Get decimals
        let decimals = Self::fetch_decimals_static(&http, &rpc_url).await?;

        Ok(Self {
            http,
            rpc_url,
            decimals,
        })
    }

    async fn test_rpc(http: &reqwest::Client, rpc: &str) -> bool {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method: "eth_blockNumber",
            params: vec![],
            id: 1,
        };

        match http.post(rpc).json(&request).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    async fn fetch_decimals_static(http: &reqwest::Client, rpc_url: &str) -> Result<u8> {
        let data = format!("0x{}", DECIMALS_SELECTOR);
        let result = Self::eth_call_static(http, rpc_url, &data).await?;

        // Result is a uint8, padded to 32 bytes
        let decimals = u8::from_str_radix(&result[result.len() - 2..], 16)?;
        Ok(decimals)
    }

    async fn eth_call_static(
        http: &reqwest::Client,
        rpc_url: &str,
        data: &str,
    ) -> Result<String> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method: "eth_call",
            params: vec![
                serde_json::json!({
                    "to": BTC_USD_FEED,
                    "data": data
                }),
                serde_json::json!("latest"),
            ],
            id: 1,
        };

        let response: JsonRpcResponse = http
            .post(rpc_url)
            .json(&request)
            .send()
            .await?
            .json()
            .await?;

        if let Some(error) = response.error {
            return Err(anyhow!("RPC error: {}", error.message));
        }

        response.result.ok_or_else(|| anyhow!("No result from RPC"))
    }

    async fn eth_call(&self, data: &str) -> Result<String> {
        Self::eth_call_static(&self.http, &self.rpc_url, data).await
    }

    /// Get the latest round data
    pub async fn get_latest_round(&self) -> Result<RoundData> {
        let data = format!("0x{}", LATEST_ROUND_DATA_SELECTOR);
        let result = self.eth_call(&data).await?;
        self.parse_round_data(&result)
    }

    /// Get current BTC/USD price as f64 (for bot compatibility)
    pub async fn get_btc_price(&self) -> Result<f64> {
        let round = self.get_latest_round().await?;
        use rust_decimal::prelude::ToPrimitive;
        round.price.to_f64().ok_or_else(|| anyhow!("Failed to convert price to f64"))
    }

    /// Get data for a specific round
    pub async fn get_round_data(&self, round_id: u128) -> Result<Option<RoundData>> {
        // Encode round_id as uint80 (padded to 32 bytes)
        let data = format!(
            "0x{}{:0>64x}",
            GET_ROUND_DATA_SELECTOR,
            round_id
        );

        let result = match self.eth_call(&data).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        match self.parse_round_data(&result) {
            Ok(rd) => {
                // Validate the round data
                if rd.price <= Decimal::ZERO || rd.updated_at.timestamp() == 0 {
                    return Ok(None);
                }
                Ok(Some(rd))
            }
            Err(_) => Ok(None),
        }
    }

    fn parse_round_data(&self, hex_result: &str) -> Result<RoundData> {
        // Remove 0x prefix
        let hex = hex_result.strip_prefix("0x").unwrap_or(hex_result);

        if hex.len() < 320 {
            return Err(anyhow!("Invalid response length"));
        }

        // Parse the 5 return values (each 32 bytes = 64 hex chars)
        // roundId (uint80), answer (int256), startedAt (uint256), updatedAt (uint256), answeredInRound (uint80)
        let round_id = u128::from_str_radix(&hex[0..64].trim_start_matches('0'), 16).unwrap_or(0);
        let answer = i128::from_str_radix(&hex[64..128].trim_start_matches('0'), 16).unwrap_or(0);
        let started_at = i64::from_str_radix(&hex[128..192].trim_start_matches('0'), 16).unwrap_or(0);
        let updated_at = i64::from_str_radix(&hex[192..256].trim_start_matches('0'), 16).unwrap_or(0);

        // Convert answer to price using decimals
        let divisor = Decimal::from(10u64.pow(self.decimals as u32));
        let price = Decimal::from(answer) / divisor;

        Ok(RoundData {
            round_id,
            price,
            started_at: Utc.timestamp_opt(started_at, 0).single().unwrap_or_default(),
            updated_at: Utc.timestamp_opt(updated_at, 0).single().unwrap_or_default(),
        })
    }

    /// Fetch historical rounds going back N days
    pub async fn fetch_historical(&self, days_back: u32) -> Result<Vec<RoundData>> {
        let latest = self.get_latest_round().await?;

        println!("  Latest round: {}", latest.round_id);
        println!("  Latest price: ${:.2}", latest.price);
        println!("  Latest update: {}", latest.updated_at);

        let cutoff_date = Utc::now() - chrono::Duration::days(days_back as i64);
        println!("  Fetching data back to: {}", cutoff_date);

        let mut rounds = vec![latest.clone()];
        let mut current_round_id = latest.round_id;
        let mut consecutive_failures = 0u32;
        let max_failures = 500;

        // Extract phase info (upper 16 bits of 80-bit round_id)
        let mut phase_id = (current_round_id >> 64) as u16;
        let mut aggregator_round = current_round_id & 0xFFFFFFFFFFFFFFFF;

        println!("  Current phase: {}, aggregator round: {}", phase_id, aggregator_round);

        loop {
            // Decrement round ID
            if aggregator_round > 1 {
                aggregator_round -= 1;
            } else if phase_id > 1 {
                phase_id -= 1;
                aggregator_round = 100000; // Start high in previous phase
                println!("  Switching to phase {}", phase_id);
            } else {
                break;
            }

            current_round_id = ((phase_id as u128) << 64) | aggregator_round;

            match self.get_round_data(current_round_id).await? {
                Some(rd) => {
                    consecutive_failures = 0;

                    if rd.updated_at < cutoff_date {
                        println!("  Reached cutoff date at round {}", current_round_id);
                        break;
                    }

                    rounds.push(rd);

                    if rounds.len() % 500 == 0 {
                        println!(
                            "  Fetched {} rounds, current date: {}",
                            rounds.len(),
                            rounds.last().unwrap().updated_at
                        );
                    }
                }
                None => {
                    consecutive_failures += 1;
                    if consecutive_failures >= max_failures {
                        if phase_id > 1 {
                            phase_id -= 1;
                            aggregator_round = 100000;
                            println!("  Switching to phase {} after {} failures", phase_id, max_failures);
                            consecutive_failures = 0;
                        } else {
                            println!("  Reached beginning of data");
                            break;
                        }
                    }
                }
            }

            // Rate limiting
            if rounds.len() % 100 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // Sort by timestamp (oldest first)
        rounds.sort_by_key(|r| r.updated_at);

        println!("  Total rounds fetched: {}", rounds.len());

        Ok(rounds)
    }
}

// ============================================================================
// Database Operations
// ============================================================================

/// Insert Chainlink price data into the database
pub async fn insert_chainlink_prices(client: &Client, rounds: &[RoundData]) -> Result<u64> {
    if rounds.is_empty() {
        return Ok(0);
    }

    let stmt = client
        .prepare(
            "INSERT INTO chainlink_prices
             (symbol, timestamp, open_price, high_price, low_price, close_price,
              volume, quote_volume, num_trades, round_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (symbol, timestamp) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                round_id = EXCLUDED.round_id",
        )
        .await?;

    let mut count = 0u64;
    let zero = Decimal::ZERO;

    for rd in rounds {
        client
            .execute(
                &stmt,
                &[
                    &"BTCUSD",           // symbol
                    &rd.updated_at,      // timestamp
                    &rd.price,           // open_price
                    &rd.price,           // high_price
                    &rd.price,           // low_price
                    &rd.price,           // close_price
                    &zero,               // volume
                    &zero,               // quote_volume
                    &0i64,               // num_trades
                    &Decimal::from_str(&rd.round_id.to_string())?, // round_id
                ],
            )
            .await?;
        count += 1;

        if count % 1000 == 0 {
            println!("  Inserted {} records...", count);
        }
    }

    Ok(count)
}

/// Get the latest timestamp in chainlink_prices
pub async fn get_latest_chainlink_timestamp(client: &Client) -> Result<Option<DateTime<Utc>>> {
    let row = client
        .query_opt(
            "SELECT MAX(timestamp) FROM chainlink_prices WHERE symbol = 'BTCUSD'",
            &[],
        )
        .await?;

    Ok(row.and_then(|r| r.get(0)))
}

/// Get statistics about stored Chainlink data
pub async fn get_chainlink_stats(client: &Client) -> Result<ChainlinkStats> {
    let row = client
        .query_one(
            "SELECT
                COUNT(*) as total_rows,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                MIN(close_price) as min_price,
                MAX(close_price) as max_price
             FROM chainlink_prices
             WHERE symbol = 'BTCUSD'",
            &[],
        )
        .await?;

    Ok(ChainlinkStats {
        total_rows: row.get::<_, i64>(0) as u64,
        earliest: row.get(1),
        latest: row.get(2),
        min_price: row.get(3),
        max_price: row.get(4),
    })
}

#[derive(Debug)]
pub struct ChainlinkStats {
    pub total_rows: u64,
    pub earliest: Option<DateTime<Utc>>,
    pub latest: Option<DateTime<Utc>>,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
}
