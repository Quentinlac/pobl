//! WebSocket connections for real-time data
//!
//! - Binance: wss://stream.binance.com:9443/ws/btcusdt@trade
//! - Polymarket: wss://ws-subscriptions-clob.polymarket.com

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::polymarket::PriceQuote;

// ============================================================================
// Shared State
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct MarketState {
    // BTC price from Binance
    pub btc_price: f64,
    pub btc_price_time: Option<DateTime<Utc>>,

    // Order book from Polymarket - UP token
    pub up_best_ask: f64,
    pub up_best_bid: f64,
    pub up_ask_liquidity: f64,
    pub up_bid_liquidity: f64,

    // Order book from Polymarket - DOWN token
    pub down_best_ask: f64,
    pub down_best_bid: f64,
    pub down_ask_liquidity: f64,
    pub down_bid_liquidity: f64,

    pub book_time: Option<DateTime<Utc>>,

    // Market info (set by main loop when market changes)
    pub market_slug: String,
    pub up_token_id: String,
    pub down_token_id: String,

    // Window info
    pub window_start: Option<DateTime<Utc>>,
    pub window_open_price: f64,
}

impl MarketState {
    /// Get UP token quote from current state
    pub fn up_quote(&self) -> PriceQuote {
        let mid = (self.up_best_bid + self.up_best_ask) / 2.0;
        let spread = self.up_best_ask - self.up_best_bid;
        PriceQuote {
            token_id: self.up_token_id.clone(),
            best_bid: self.up_best_bid,
            best_ask: self.up_best_ask,
            mid_price: mid,
            spread,
            spread_pct: if mid > 0.0 { spread / mid } else { 0.0 },
            bid_liquidity: self.up_bid_liquidity,
            ask_liquidity: self.up_ask_liquidity,
        }
    }

    /// Get DOWN token quote from current state
    pub fn down_quote(&self) -> PriceQuote {
        let mid = (self.down_best_bid + self.down_best_ask) / 2.0;
        let spread = self.down_best_ask - self.down_best_bid;
        PriceQuote {
            token_id: self.down_token_id.clone(),
            best_bid: self.down_best_bid,
            best_ask: self.down_best_ask,
            mid_price: mid,
            spread,
            spread_pct: if mid > 0.0 { spread / mid } else { 0.0 },
            bid_liquidity: self.down_bid_liquidity,
            ask_liquidity: self.down_ask_liquidity,
        }
    }

    /// Check if we have valid data
    pub fn is_ready(&self) -> bool {
        self.btc_price > 0.0
            && self.up_best_ask > 0.0
            && self.down_best_ask > 0.0
            && !self.up_token_id.is_empty()
            && !self.down_token_id.is_empty()
    }
}

// ============================================================================
// Binance WebSocket
// ============================================================================

#[derive(Debug, Deserialize)]
struct BinanceTrade {
    #[serde(rename = "p")]
    price: String,
}

pub async fn binance_ws_task(
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
                                    let mut s = state.write().await;
                                    s.btc_price = price;
                                    s.btc_price_time = Some(Utc::now());
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            debug!("Binance ping received");
                            let _ = data; // Pong is auto-sent by tungstenite
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
            info!("Reconnecting to Binance in 2 seconds...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    Ok(())
}

// ============================================================================
// Polymarket WebSocket
// ============================================================================

#[derive(Debug, Deserialize)]
struct BookSnapshot {
    asset_id: String,
    #[serde(default)]
    bids: Vec<OrderLevel>,
    #[serde(default)]
    asks: Vec<OrderLevel>,
}

#[derive(Debug, Deserialize)]
struct OrderLevel {
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
}

#[derive(Debug, Deserialize)]
struct PolymarketUpdateMessage {
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
}

pub async fn polymarket_ws_task(
    state: Arc<RwLock<MarketState>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    info!(">>> Polymarket WebSocket task STARTED <<<");

    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let mut connection_attempts = 0u32;

    while running.load(Ordering::SeqCst) {
        // Get current tokens
        let (up_token, down_token) = {
            let s = state.read().await;
            (s.up_token_id.clone(), s.down_token_id.clone())
        };

        // Wait for tokens to be set
        if up_token.is_empty() || down_token.is_empty() {
            info!("Polymarket WS: waiting for token IDs (up={}, down={})...",
                  up_token.len(), down_token.len());
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        connection_attempts += 1;
        info!("Connecting to Polymarket WebSocket (attempt #{})...", connection_attempts);

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("Connected to Polymarket WebSocket");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to both tokens
                let subscribe_msg = serde_json::json!({
                    "type": "market",
                    "assets_ids": [&up_token, &down_token]
                });

                if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                    error!("Failed to subscribe: {}", e);
                    continue;
                }
                info!("Subscribed to UP={:.16}... DOWN={:.16}...", &up_token[..16.min(up_token.len())], &down_token[..16.min(down_token.len())]);

                // Ping interval to keep connection alive (Polymarket requires pings every 10s)
                let mut ping_interval = tokio::time::interval(Duration::from_secs(5));
                ping_interval.tick().await; // Skip the first immediate tick

                let mut messages_received = 0u32;

                loop {
                    if !running.load(Ordering::SeqCst) {
                        break;
                    }

                    tokio::select! {
                        // Send ping every 5 seconds to keep connection alive
                        _ = ping_interval.tick() => {
                            // Check if tokens changed (new window)
                            let (current_up, current_down) = {
                                let s = state.read().await;
                                (s.up_token_id.clone(), s.down_token_id.clone())
                            };

                            if current_up != up_token || current_down != down_token {
                                info!("Market changed, reconnecting to new tokens...");
                                break;
                            }

                            // Send ping to keep connection alive
                            if let Err(e) = write.send(Message::Ping(vec![])).await {
                                warn!("Failed to send ping: {}", e);
                                break;
                            }
                            debug!("Sent ping to Polymarket");
                        }

                        // Handle incoming messages
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    messages_received += 1;
                                    if messages_received == 1 {
                                        info!("First WebSocket message received ({} bytes)", text.len());
                                    }
                                    // Log first 200 chars to see what we're receiving
                                    debug!("WS message #{}: {}", messages_received, &text[..200.min(text.len())]);

                                    // Try to parse as initial snapshot (array)
                                    match serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                                        Ok(snapshots) => {
                                            info!("Received order book snapshot with {} assets", snapshots.len());
                                            for snap in &snapshots {
                                                info!("Snapshot asset={}: {} bids, {} asks",
                                                       &snap.asset_id[..20.min(snap.asset_id.len())],
                                                       snap.bids.len(), snap.asks.len());
                                            }
                                            process_snapshots(&snapshots, &state, &up_token, &down_token).await;

                                            // Log the resulting state - IMPORTANT for debugging
                                            let s = state.read().await;
                                            info!("✓ SNAPSHOT PROCESSED: UP bid={:.2}¢ ask={:.2}¢, DOWN bid={:.2}¢ ask={:.2}¢",
                                                  s.up_best_bid * 100.0, s.up_best_ask * 100.0,
                                                  s.down_best_bid * 100.0, s.down_best_ask * 100.0);
                                        }
                                        Err(e) => {
                                            // Not a snapshot, try update message
                                            match serde_json::from_str::<PolymarketUpdateMessage>(&text) {
                                                Ok(update) => {
                                                    if !update.price_changes.is_empty() {
                                                        debug!("Got {} price_changes", update.price_changes.len());
                                                        process_price_changes(&update.price_changes, &state, &up_token, &down_token).await;
                                                    }
                                                }
                                                Err(e2) => {
                                                    // Neither format worked - log what we got
                                                    warn!("Unknown WS message format. snapshot_err={}, update_err={}. First 300 chars: {}",
                                                          e, e2, &text[..300.min(text.len())]);
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    debug!("Polymarket pong received");
                                }
                                Some(Ok(Message::Ping(_))) => {
                                    debug!("Polymarket ping received");
                                    // Pong is auto-sent by tungstenite
                                }
                                Some(Ok(Message::Close(_))) => {
                                    warn!("Polymarket WebSocket closed by server");
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
                }
            }
            Err(e) => {
                error!("Failed to connect to Polymarket WebSocket: {}", e);
            }
        }

        if running.load(Ordering::SeqCst) {
            info!("Reconnecting to Polymarket in 2 seconds...");
            tokio::time::sleep(Duration::from_secs(2)).await;
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

        info!("Snapshot asset_id={}, up_token={}, down_token={}, is_up={}, is_down={}",
               &snapshot.asset_id[..20.min(snapshot.asset_id.len())],
               &up_token[..20.min(up_token.len())],
               &down_token[..20.min(down_token.len())],
               is_up, is_down);

        if !is_up && !is_down {
            warn!("Snapshot asset_id doesn't match any token! Skipping.");
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

        // Find best bid (highest price)
        let best_bid = snapshot.bids.iter()
            .filter_map(|l| {
                let price = l.price.parse::<f64>().ok()?;
                let size = l.size.parse::<f64>().ok()?;
                if size > 0.0 { Some((price, size)) } else { None }
            })
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Total liquidity
        let ask_liquidity: f64 = snapshot.asks.iter()
            .filter_map(|l| l.size.parse::<f64>().ok())
            .sum();
        let bid_liquidity: f64 = snapshot.bids.iter()
            .filter_map(|l| l.size.parse::<f64>().ok())
            .sum();

        let token_name = if is_up { "UP" } else { "DOWN" };
        info!("Processing {} snapshot: best_bid={:?}, best_ask={:?}",
              token_name, best_bid, best_ask);

        if let Some((price, _)) = best_ask {
            if is_up {
                s.up_best_ask = price;
                s.up_ask_liquidity = ask_liquidity;
            } else {
                s.down_best_ask = price;
                s.down_ask_liquidity = ask_liquidity;
            }
        }

        if let Some((price, _)) = best_bid {
            if is_up {
                s.up_best_bid = price;
                s.up_bid_liquidity = bid_liquidity;
            } else {
                s.down_best_bid = price;
                s.down_bid_liquidity = bid_liquidity;
            }
        }

        s.book_time = Some(Utc::now());
        debug!("Snapshot {}: bid={:.2} ask={:.2}", if is_up { "UP" } else { "DOWN" },
               if is_up { s.up_best_bid } else { s.down_best_bid },
               if is_up { s.up_best_ask } else { s.down_best_ask });
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

        if let Some(ask_str) = &change.best_ask {
            if let Ok(price) = ask_str.parse::<f64>() {
                if is_up {
                    s.up_best_ask = price;
                } else {
                    s.down_best_ask = price;
                }
                s.book_time = Some(Utc::now());
            }
        }

        if let Some(bid_str) = &change.best_bid {
            if let Ok(price) = bid_str.parse::<f64>() {
                if is_up {
                    s.up_best_bid = price;
                } else {
                    s.down_best_bid = price;
                }
            }
        }
    }
}
