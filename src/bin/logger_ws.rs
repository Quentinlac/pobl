//! BTC 15-Minute Market Data Logger (WebSocket Version)
//!
//! Uses WebSocket streams for real-time data:
//! - Binance: wss://stream.binance.com:9443/ws/btcusdt@trade
//! - Polymarket: wss://ws-subscriptions-clob.polymarket.com

#[path = "../models.rs"]
mod models;
#[path = "../stats.rs"]
mod stats;
#[path = "../bot/binance.rs"]
mod binance;
#[path = "../bot/polymarket.rs"]
mod polymarket;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use models::{delta_to_bucket, ProbabilityMatrix};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_postgres::Client;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

// ============================================================================
// Database
// ============================================================================

struct DbConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            host: "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com".to_string(),
            port: 5432,
            user: "qoveryadmin".to_string(),
            password: "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp".to_string(),
            database: "polymarket".to_string(),
        }
    }
}

async fn connect_db(config: &DbConfig) -> Result<Client> {
    let connection_string = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.database
    );

    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let connector = MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(&connection_string, connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

async fn run_migrations(client: &Client) -> Result<()> {
    let migration = include_str!("../../migrations/003_market_logs.sql");
    client.batch_execute(migration).await?;
    info!("Database migrations complete");
    Ok(())
}

// ============================================================================
// Shared State
// ============================================================================

#[derive(Debug, Clone, Default)]
struct MarketState {
    // BTC price from Binance
    btc_price: f64,
    btc_price_time: Option<DateTime<Utc>>,

    // Order book from Polymarket - Ask prices (for buying)
    up_best_ask: f64,
    up_best_ask_size: f64,
    down_best_ask: f64,
    down_best_ask_size: f64,

    // Order book from Polymarket - Bid prices (for selling)
    up_best_bid: f64,
    up_best_bid_size: f64,
    down_best_bid: f64,
    down_best_bid_size: f64,

    book_time: Option<DateTime<Utc>>,

    // Market info
    market_slug: String,
    up_token_id: String,
    down_token_id: String,

    // Window info
    window_start: Option<DateTime<Utc>>,
    window_open_price: f64,
}

// ============================================================================
// Binance WebSocket
// ============================================================================

#[derive(Debug, Deserialize)]
struct BinanceTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "T")]
    trade_time: i64,
}

async fn binance_ws_task(
    state: Arc<RwLock<MarketState>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    while running.load(Ordering::SeqCst) {
        info!("Connecting to Binance WebSocket...");

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Binance WebSocket");
                let (mut _write, mut read) = ws_stream.split();

                while running.load(Ordering::SeqCst) {
                    match tokio::time::timeout(Duration::from_secs(30), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                                if let Ok(price) = trade.price.parse::<f64>() {
                                    let mut state = state.write().await;
                                    state.btc_price = price;
                                    state.btc_price_time = Some(Utc::now());
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            debug!("Binance ping received");
                            // Pong is handled automatically by tungstenite
                        }
                        Ok(Some(Ok(Message::Close(_)))) => {
                            warn!("Binance WebSocket closed");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            error!("Binance WebSocket error: {}", e);
                            break;
                        }
                        Ok(None) => {
                            warn!("Binance WebSocket stream ended");
                            break;
                        }
                        Err(_) => {
                            warn!("Binance WebSocket timeout, reconnecting...");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Binance WebSocket: {}", e);
            }
        }

        if running.load(Ordering::SeqCst) {
            info!("Reconnecting to Binance in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(())
}

// ============================================================================
// Polymarket WebSocket (with correct price_changes parsing)
// ============================================================================

use serde::Serialize;

#[derive(Debug, Serialize)]
struct PolymarketSubscribe {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    msg_type: String,
}

// Message with price_changes array (the real update format)
#[derive(Debug, Deserialize)]
struct PolymarketUpdateMessage {
    #[serde(default)]
    market: String,
    #[serde(default)]
    price_changes: Vec<PriceChange>,
}

#[derive(Debug, Deserialize)]
struct PriceChange {
    asset_id: String,
    #[serde(default)]
    best_bid: Option<String>,
    #[serde(default)]
    best_ask: Option<String>,
    #[serde(default)]
    best_bid_size: Option<String>,
    #[serde(default)]
    best_ask_size: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    side: Option<String>,
}

// Initial book snapshot format
#[derive(Debug, Deserialize)]
struct BookSnapshot {
    asset_id: String,
    #[serde(default)]
    asks: Vec<BookLevel>,
    #[serde(default)]
    bids: Vec<BookLevel>,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    price: String,
    size: String,
}

async fn polymarket_ws_task(
    state: Arc<RwLock<MarketState>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

    while running.load(Ordering::SeqCst) {
        // Get current token IDs
        let (up_token, down_token) = {
            let s = state.read().await;
            (s.up_token_id.clone(), s.down_token_id.clone())
        };

        if up_token.is_empty() || down_token.is_empty() {
            debug!("Waiting for token IDs...");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        info!("Connecting to Polymarket WebSocket...");

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Polymarket WebSocket");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to both tokens
                let subscribe = PolymarketSubscribe {
                    assets_ids: vec![up_token.clone(), down_token.clone()],
                    msg_type: "market".to_string(),
                };

                let sub_msg = serde_json::to_string(&subscribe)?;
                info!("Subscribing to UP={} DOWN={}",
                    &up_token[..20.min(up_token.len())],
                    &down_token[..20.min(down_token.len())]);

                if let Err(e) = write.send(Message::Text(sub_msg)).await {
                    error!("Failed to subscribe: {}", e);
                    continue;
                }

                // Ping task + token change detection
                let running_clone = running.clone();
                let mut ping_interval = tokio::time::interval(Duration::from_secs(5));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if !running_clone.load(Ordering::SeqCst) {
                                break;
                            }

                            // Check if tokens changed (new window started)
                            let (new_up, new_down) = {
                                let s = state.read().await;
                                (s.up_token_id.clone(), s.down_token_id.clone())
                            };
                            if new_up != up_token || new_down != down_token {
                                info!("Token IDs changed, reconnecting to new market...");
                                break; // Reconnect with new tokens
                            }

                            if let Err(e) = write.send(Message::Ping(vec![])).await {
                                warn!("Failed to send ping: {}", e);
                                break;
                            }
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    // Log raw message for debugging (full message)
                                    debug!("Polymarket WS raw: {}", text);

                                    // Try to parse as array (initial snapshot)
                                    if let Ok(snapshots) = serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                                        info!("Got order book snapshot with {} assets", snapshots.len());
                                        process_snapshots(&snapshots, &state, &up_token, &down_token).await;
                                    }
                                    // Try to parse as update message with price_changes
                                    else if let Ok(update) = serde_json::from_str::<PolymarketUpdateMessage>(&text) {
                                        if !update.price_changes.is_empty() {
                                            // Log the actual fields we receive
                                            for pc in &update.price_changes {
                                                debug!(
                                                    "price_change: asset={} best_ask={:?} best_bid={:?} best_ask_size={:?} best_bid_size={:?} price={:?} size={:?} side={:?}",
                                                    &pc.asset_id[..16.min(pc.asset_id.len())],
                                                    pc.best_ask, pc.best_bid,
                                                    pc.best_ask_size, pc.best_bid_size,
                                                    pc.price, pc.size, pc.side
                                                );
                                            }
                                            process_price_changes(&update.price_changes, &state, &up_token, &down_token).await;
                                        }
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    debug!("Polymarket pong received");
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Polymarket WebSocket closed");
                                    break;
                                }
                                Some(Err(e)) => {
                                    error!("Polymarket WebSocket error: {}", e);
                                    break;
                                }
                                None => {
                                    warn!("Polymarket WebSocket stream ended");
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    if !running.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Polymarket WebSocket: {}", e);
            }
        }

        if running.load(Ordering::SeqCst) {
            info!("Reconnecting to Polymarket in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(())
}

async fn process_snapshots(
    snapshots: &[BookSnapshot],
    state: &Arc<RwLock<MarketState>>,
    up_token: &str,
    down_token: &str,
) {
    let mut s = state.write().await;

    for snapshot in snapshots {
        let is_up = snapshot.asset_id == up_token;
        let is_down = snapshot.asset_id == down_token;

        if !is_up && !is_down {
            continue;
        }

        // Find best ask (lowest price)
        let best_ask = snapshot.asks.iter()
            .filter_map(|l| {
                let price = l.price.parse::<f64>().ok()?;
                let size = l.size.parse::<f64>().ok()?;
                if size > 0.0 { Some((price, size)) } else { None }
            })
            .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Find best bid (highest price) - also capture size
        let best_bid = snapshot.bids.iter()
            .filter_map(|l| {
                let price = l.price.parse::<f64>().ok()?;
                let size = l.size.parse::<f64>().ok()?;
                if size > 0.0 { Some((price, size)) } else { None }
            })
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((price, size)) = best_ask {
            if is_up {
                s.up_best_ask = price;
                s.up_best_ask_size = size;
                debug!("Snapshot UP: best_ask={:.4} size={:.2}", price, size);
            } else {
                s.down_best_ask = price;
                s.down_best_ask_size = size;
                debug!("Snapshot DOWN: best_ask={:.4} size={:.2}", price, size);
            }
            s.book_time = Some(Utc::now());
        }

        if let Some((price, size)) = best_bid {
            if is_up {
                s.up_best_bid = price;
                s.up_best_bid_size = size;
                debug!("Snapshot UP: best_bid={:.4} size={:.2}", price, size);
            } else {
                s.down_best_bid = price;
                s.down_best_bid_size = size;
                debug!("Snapshot DOWN: best_bid={:.4} size={:.2}", price, size);
            }
        }
    }
}

async fn process_price_changes(
    changes: &[PriceChange],
    state: &Arc<RwLock<MarketState>>,
    up_token: &str,
    down_token: &str,
) {
    let mut s = state.write().await;

    for change in changes {
        let is_up = change.asset_id == up_token;
        let is_down = change.asset_id == down_token;

        if !is_up && !is_down {
            continue;
        }

        // Parse the level price and size from this update
        let level_price = change.price.as_ref().and_then(|p| p.parse::<f64>().ok());
        let level_size = change.size.as_ref().and_then(|s| s.parse::<f64>().ok());
        let side = change.side.as_deref();

        // Use best_ask from the price_change message
        if let Some(ask_str) = &change.best_ask {
            if let Ok(best_ask) = ask_str.parse::<f64>() {
                if is_up {
                    s.up_best_ask = best_ask;
                    // If this update is for the best_ask level (SELL side), use its size
                    if side == Some("SELL") {
                        if let (Some(lp), Some(ls)) = (level_price, level_size) {
                            if (lp - best_ask).abs() < 0.001 {
                                s.up_best_ask_size = ls;
                            }
                        }
                    }
                    debug!("Update UP: best_ask={:.4}", best_ask);
                } else {
                    s.down_best_ask = best_ask;
                    if side == Some("SELL") {
                        if let (Some(lp), Some(ls)) = (level_price, level_size) {
                            if (lp - best_ask).abs() < 0.001 {
                                s.down_best_ask_size = ls;
                            }
                        }
                    }
                    debug!("Update DOWN: best_ask={:.4}", best_ask);
                }
                s.book_time = Some(Utc::now());
            }
        }

        // Use best_bid from the price_change message
        if let Some(bid_str) = &change.best_bid {
            if let Ok(best_bid) = bid_str.parse::<f64>() {
                if is_up {
                    s.up_best_bid = best_bid;
                    // If this update is for the best_bid level (BUY side), use its size
                    if side == Some("BUY") {
                        if let (Some(lp), Some(ls)) = (level_price, level_size) {
                            if (lp - best_bid).abs() < 0.001 {
                                s.up_best_bid_size = ls;
                            }
                        }
                    }
                    debug!("Update UP: best_bid={:.4}", best_bid);
                } else {
                    s.down_best_bid = best_bid;
                    if side == Some("BUY") {
                        if let (Some(lp), Some(ls)) = (level_price, level_size) {
                            if (lp - best_bid).abs() < 0.001 {
                                s.down_best_bid_size = ls;
                            }
                        }
                    }
                    debug!("Update DOWN: best_bid={:.4}", best_bid);
                }
            }
        }
    }
}

// ============================================================================
// Market Info Fetcher (REST - runs periodically)
// ============================================================================

async fn market_info_task(
    state: Arc<RwLock<MarketState>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let poly_client = polymarket::PolymarketClient::new(5000)?;
    let binance_client = binance::BinanceClient::new(5000)?;

    while running.load(Ordering::SeqCst) {
        let current_window_start = binance::get_current_window_start();

        // Check if we need to update window info
        let needs_update = {
            let s = state.read().await;
            s.window_start != Some(current_window_start)
        };

        if needs_update {
            info!("Fetching new window info for {:?}", current_window_start);

            match poly_client.get_current_btc_15m_market().await {
                Ok(market) => {
                    match binance_client.get_window_open_price(current_window_start).await {
                        Ok(open_price) => {
                            let mut s = state.write().await;
                            s.window_start = Some(current_window_start);
                            s.window_open_price = open_price;
                            s.market_slug = market.slug;
                            s.up_token_id = market.up_token_id;
                            s.down_token_id = market.down_token_id;
                            // Reset book prices for new window
                            s.up_best_ask = 0.0;
                            s.down_best_ask = 0.0;
                            s.up_best_bid = 0.0;
                            s.down_best_bid = 0.0;
                            info!("New window: slug={}, open_price={:.2}", s.market_slug, open_price);
                        }
                        Err(e) => {
                            warn!("Failed to get window open price: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get market info: {}", e);
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    Ok(())
}

// ============================================================================
// Logger Task
// ============================================================================

fn load_matrix_from_file() -> Result<ProbabilityMatrix> {
    let matrix_path = std::env::var("MATRIX_PATH")
        .unwrap_or_else(|_| "output/matrix.json".to_string());
    let matrix_path = PathBuf::from(&matrix_path);

    if !matrix_path.exists() {
        anyhow::bail!("Probability matrix not found: {}", matrix_path.display());
    }

    let matrix_json = std::fs::read_to_string(&matrix_path)?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&matrix_json)?;
    Ok(matrix)
}

/// Returns (edge_up_buy, edge_down_buy, edge_up_sell, edge_down_sell)
fn calculate_edges(
    matrix: &ProbabilityMatrix,
    time_elapsed: u32,
    price_delta: f64,
    ask_up: f64,
    ask_down: f64,
    bid_up: f64,
    bid_down: f64,
) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
    let time_bucket = (time_elapsed / 15).min(59) as u8;
    let delta_bucket = delta_to_bucket(
        Decimal::try_from(price_delta).unwrap_or_default()
    );

    let cell = matrix.get(time_bucket, delta_bucket);

    if cell.total() < 10 {
        return (None, None, None, None);
    }

    let our_p_up = cell.p_up_wilson_lower;
    let our_p_down = 1.0 - cell.p_up_wilson_upper;

    // Buy edges: positive when our probability > market price (good to buy)
    let edge_up_buy = if ask_up > 0.01 {
        Some((our_p_up - ask_up) / ask_up)
    } else {
        None
    };

    let edge_down_buy = if ask_down > 0.01 {
        Some((our_p_down - ask_down) / ask_down)
    } else {
        None
    };

    // Sell edges: positive when bid price > our probability (good to sell)
    let edge_up_sell = if bid_up > 0.01 {
        Some((bid_up - our_p_up) / bid_up)
    } else {
        None
    };

    let edge_down_sell = if bid_down > 0.01 {
        Some((bid_down - our_p_down) / bid_down)
    } else {
        None
    };

    (edge_up_buy, edge_down_buy, edge_up_sell, edge_down_sell)
}

async fn logger_task(
    db_client: Client,
    state: Arc<RwLock<MarketState>>,
    matrix: ProbabilityMatrix,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut log_count: u64 = 0;
    let mut error_count: u64 = 0;
    let interval = Duration::from_millis(200); // 5 logs per second

    while running.load(Ordering::SeqCst) {
        let loop_start = std::time::Instant::now();
        let timestamp = Utc::now();

        // Get current state
        let s = state.read().await;

        // Skip if we don't have all data yet
        if s.btc_price == 0.0 || s.up_best_ask == 0.0 || s.down_best_ask == 0.0 || s.window_start.is_none() {
            drop(s);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let time_elapsed = binance::get_seconds_elapsed() as i32;
        let price_delta = s.btc_price - s.window_open_price;

        // Calculate edges (buy and sell)
        let (edge_up, edge_down, edge_up_sell, edge_down_sell) = calculate_edges(
            &matrix,
            time_elapsed as u32,
            price_delta,
            s.up_best_ask,
            s.down_best_ask,
            s.up_best_bid,
            s.down_best_bid,
        );

        // Convert to Decimal for database
        let f64_to_dec = |v: f64| Decimal::try_from(v).unwrap_or_default();

        // Insert into database
        let result = db_client.execute(
            r#"
            INSERT INTO market_logs (
                timestamp, market_slug, up_token_id, down_token_id,
                price_up, price_down, size_up, size_down,
                edge_up, edge_down, btc_price, time_elapsed, price_delta, error_message,
                bid_up, bid_down, edge_up_sell, edge_down_sell,
                bid_size_up, bid_size_down
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
            "#,
            &[
                &timestamp,
                &s.market_slug,
                &s.up_token_id,
                &s.down_token_id,
                &f64_to_dec(s.up_best_ask),
                &f64_to_dec(s.down_best_ask),
                &f64_to_dec(s.up_best_ask_size),
                &f64_to_dec(s.down_best_ask_size),
                &edge_up.map(f64_to_dec),
                &edge_down.map(f64_to_dec),
                &f64_to_dec(s.btc_price),
                &time_elapsed,
                &f64_to_dec(price_delta),
                &None::<String>,
                &(if s.up_best_bid > 0.0 { Some(f64_to_dec(s.up_best_bid)) } else { None }),
                &(if s.down_best_bid > 0.0 { Some(f64_to_dec(s.down_best_bid)) } else { None }),
                &edge_up_sell.map(f64_to_dec),
                &edge_down_sell.map(f64_to_dec),
                &(if s.up_best_bid_size > 0.0 { Some(f64_to_dec(s.up_best_bid_size)) } else { None }),
                &(if s.down_best_bid_size > 0.0 { Some(f64_to_dec(s.down_best_bid_size)) } else { None }),
            ],
        ).await;

        match result {
            Ok(_) => {
                log_count += 1;
            }
            Err(e) => {
                error!("Failed to insert log: {}", e);
                error_count += 1;
            }
        }

        // Periodic status (every 100 entries = ~20 seconds at 5/sec)
        if log_count % 100 == 0 && log_count > 0 {
            info!(
                "Logged {} ({} err) | BTC=${:.0} | UP: bid={:.2}¢ ask={:.2}¢ | DOWN: bid={:.2}¢ ask={:.2}¢ | Buy: UP={:+.1}% DOWN={:+.1}% | Sell: UP={:+.1}% DOWN={:+.1}%",
                log_count,
                error_count,
                s.btc_price,
                s.up_best_bid * 100.0,
                s.up_best_ask * 100.0,
                s.down_best_bid * 100.0,
                s.down_best_ask * 100.0,
                edge_up.unwrap_or(0.0) * 100.0,
                edge_down.unwrap_or(0.0) * 100.0,
                edge_up_sell.unwrap_or(0.0) * 100.0,
                edge_down_sell.unwrap_or(0.0) * 100.0
            );
        }

        drop(s);

        // Sleep for remaining interval time
        let elapsed = loop_start.elapsed();
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }

    info!("Logger shutdown. Total entries: {}, errors: {}", log_count, error_count);
    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,logger_ws=debug")),
        )
        .init();

    info!("Starting BTC 15-Minute Market Logger (WebSocket)");

    // Shutdown signal
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        r.store(false, Ordering::SeqCst);
    });

    // Connect to database
    let db_config = DbConfig::default();
    let db_client = connect_db(&db_config).await?;
    info!("Connected to PostgreSQL");

    run_migrations(&db_client).await?;

    // Load matrix
    let matrix = load_matrix_from_file()?;
    info!("Loaded probability matrix with {} windows", matrix.total_windows);

    // Shared state
    let state = Arc::new(RwLock::new(MarketState::default()));

    // Spawn tasks
    let binance_state = state.clone();
    let binance_running = running.clone();
    let binance_handle = tokio::spawn(async move {
        if let Err(e) = binance_ws_task(binance_state, binance_running).await {
            error!("Binance WS task error: {}", e);
        }
    });

    let poly_state = state.clone();
    let poly_running = running.clone();
    let poly_handle = tokio::spawn(async move {
        if let Err(e) = polymarket_ws_task(poly_state, poly_running).await {
            error!("Polymarket WS task error: {}", e);
        }
    });

    let market_state = state.clone();
    let market_running = running.clone();
    let market_handle = tokio::spawn(async move {
        if let Err(e) = market_info_task(market_state, market_running).await {
            error!("Market info task error: {}", e);
        }
    });

    let logger_state = state.clone();
    let logger_running = running.clone();
    let logger_handle = tokio::spawn(async move {
        if let Err(e) = logger_task(db_client, logger_state, matrix, logger_running).await {
            error!("Logger task error: {}", e);
        }
    });

    // Wait for all tasks
    let _ = tokio::join!(binance_handle, poly_handle, market_handle, logger_handle);

    info!("All tasks completed");
    Ok(())
}
