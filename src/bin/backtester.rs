//! Backtester for BTC 15-Minute Markets
//!
//! Analyzes historical market_logs data to simulate trading strategies
//! and calculate profit/loss.

use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_postgres::Client;
use tracing::{info, warn};
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
            host: std::env::var("DB_HOST")
                .unwrap_or_else(|_| "dpg-ctj7t5ij1k6c73b518fg-a.frankfurt-postgres.render.com".to_string()),
            port: 5432,
            user: std::env::var("DB_USER")
                .unwrap_or_else(|_| "btc_probability_matrix_user".to_string()),
            password: std::env::var("DB_PASSWORD")
                .unwrap_or_else(|_| "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp".to_string()),
            database: std::env::var("DB_NAME")
                .unwrap_or_else(|_| "btc_probability_matrix".to_string()),
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
            eprintln!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone)]
struct MarketSnapshot {
    timestamp: chrono::DateTime<chrono::Utc>,
    price_up: f64,
    price_down: f64,
    size_up: f64,
    size_down: f64,
    edge_up: Option<f64>,
    edge_down: Option<f64>,
    bid_up: Option<f64>,
    bid_down: Option<f64>,
    btc_price: f64,
    time_elapsed: i32,
    price_delta: f64,
}

#[derive(Debug, Clone)]
struct Market {
    slug: String,
    snapshots: Vec<MarketSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Outcome {
    UpWins,
    DownWins,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Side {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct Trade {
    market_slug: String,
    side: Side,
    entry_price: f64,
    entry_time_elapsed: i32,
    entry_edge: f64,
    exit_price: Option<f64>,  // None = hold to expiration
    outcome: Outcome,
    pnl_percent: f64,
}

// ============================================================================
// Data Loading
// ============================================================================

async fn load_markets(client: &Client) -> Result<Vec<Market>> {
    info!("Loading market data from database...");

    let rows = client
        .query(
            r#"
            SELECT
                market_slug,
                timestamp,
                price_up,
                price_down,
                size_up,
                size_down,
                edge_up,
                edge_down,
                bid_up,
                bid_down,
                btc_price,
                time_elapsed,
                price_delta
            FROM market_logs
            ORDER BY market_slug, timestamp
            "#,
            &[],
        )
        .await
        .context("Failed to query market_logs")?;

    info!("Loaded {} rows from database", rows.len());

    // Group by market_slug
    let mut markets_map: HashMap<String, Vec<MarketSnapshot>> = HashMap::new();

    for row in rows {
        let slug: String = row.get("market_slug");
        let snapshot = MarketSnapshot {
            timestamp: row.get("timestamp"),
            price_up: row.get::<_, Decimal>("price_up").to_f64().unwrap_or(0.0),
            price_down: row.get::<_, Decimal>("price_down").to_f64().unwrap_or(0.0),
            size_up: row.get::<_, Decimal>("size_up").to_f64().unwrap_or(0.0),
            size_down: row.get::<_, Decimal>("size_down").to_f64().unwrap_or(0.0),
            edge_up: row.get::<_, Option<Decimal>>("edge_up").and_then(|d| d.to_f64()),
            edge_down: row.get::<_, Option<Decimal>>("edge_down").and_then(|d| d.to_f64()),
            bid_up: row.get::<_, Option<Decimal>>("bid_up").and_then(|d| d.to_f64()),
            bid_down: row.get::<_, Option<Decimal>>("bid_down").and_then(|d| d.to_f64()),
            btc_price: row.get::<_, Decimal>("btc_price").to_f64().unwrap_or(0.0),
            time_elapsed: row.get("time_elapsed"),
            price_delta: row.get::<_, Decimal>("price_delta").to_f64().unwrap_or(0.0),
        };

        markets_map.entry(slug).or_default().push(snapshot);
    }

    let markets: Vec<Market> = markets_map
        .into_iter()
        .map(|(slug, snapshots)| Market { slug, snapshots })
        .collect();

    info!("Grouped into {} distinct markets", markets.len());
    Ok(markets)
}

// ============================================================================
// Market Outcome Detection
// ============================================================================

fn determine_outcome(market: &Market) -> Outcome {
    if market.snapshots.is_empty() {
        return Outcome::Unknown;
    }

    // Get the last snapshot
    let last = market.snapshots.last().unwrap();

    // Method 1: Check if prices went to extremes (0.99/0.01)
    if last.price_up >= 0.95 || last.price_down <= 0.05 {
        return Outcome::UpWins;
    }
    if last.price_down >= 0.95 || last.price_up <= 0.05 {
        return Outcome::DownWins;
    }

    // Method 2: Check the final price_delta sign
    // price_delta > 0 means BTC went UP from window open
    if last.price_delta > 0.0 {
        return Outcome::UpWins;
    } else if last.price_delta < 0.0 {
        return Outcome::DownWins;
    }

    // Method 3: Check time_elapsed - if we have data near end of window (900s)
    // and prices haven't resolved, market might still be active
    if last.time_elapsed < 800 {
        return Outcome::Unknown; // Market might not have finished
    }

    Outcome::Unknown
}

// ============================================================================
// Strategy Simulation
// ============================================================================

struct StrategyConfig {
    name: String,
    min_edge: f64,           // Minimum edge to enter (e.g., 0.05 = 5%)
    max_entry_time: i32,     // Only enter before this time (seconds)
    min_entry_time: i32,     // Only enter after this time (seconds)
    min_liquidity: f64,      // Minimum size to enter
    hold_to_expiration: bool, // If false, could implement early exit
}

fn simulate_strategy(markets: &[Market], config: &StrategyConfig) -> Vec<Trade> {
    let mut trades = Vec::new();

    for market in markets {
        let outcome = determine_outcome(market);
        if outcome == Outcome::Unknown {
            continue; // Skip markets with unknown outcomes
        }

        // Look for entry opportunities
        for snapshot in &market.snapshots {
            // Check time constraints
            if snapshot.time_elapsed < config.min_entry_time {
                continue;
            }
            if snapshot.time_elapsed > config.max_entry_time {
                break; // No more entries for this market
            }

            // Check UP edge
            if let Some(edge_up) = snapshot.edge_up {
                if edge_up >= config.min_edge && snapshot.size_up >= config.min_liquidity {
                    let won = outcome == Outcome::UpWins;
                    let pnl = if won {
                        (1.0 - snapshot.price_up) / snapshot.price_up * 100.0
                    } else {
                        -100.0
                    };

                    trades.push(Trade {
                        market_slug: market.slug.clone(),
                        side: Side::Up,
                        entry_price: snapshot.price_up,
                        entry_time_elapsed: snapshot.time_elapsed,
                        entry_edge: edge_up,
                        exit_price: None,
                        outcome,
                        pnl_percent: pnl,
                    });
                    break; // Only one trade per market per side
                }
            }

            // Check DOWN edge
            if let Some(edge_down) = snapshot.edge_down {
                if edge_down >= config.min_edge && snapshot.size_down >= config.min_liquidity {
                    let won = outcome == Outcome::DownWins;
                    let pnl = if won {
                        (1.0 - snapshot.price_down) / snapshot.price_down * 100.0
                    } else {
                        -100.0
                    };

                    trades.push(Trade {
                        market_slug: market.slug.clone(),
                        side: Side::Down,
                        entry_price: snapshot.price_down,
                        entry_time_elapsed: snapshot.time_elapsed,
                        entry_edge: edge_down,
                        exit_price: None,
                        outcome,
                        pnl_percent: pnl,
                    });
                    break; // Only one trade per market per side
                }
            }
        }
    }

    trades
}

// ============================================================================
// Results Analysis
// ============================================================================

fn analyze_trades(trades: &[Trade], strategy_name: &str) {
    if trades.is_empty() {
        println!("\n{}: No trades executed", strategy_name);
        return;
    }

    let total_trades = trades.len();
    let winning_trades = trades.iter().filter(|t| t.pnl_percent > 0.0).count();
    let losing_trades = total_trades - winning_trades;
    let win_rate = winning_trades as f64 / total_trades as f64 * 100.0;

    let total_pnl: f64 = trades.iter().map(|t| t.pnl_percent).sum();
    let avg_pnl = total_pnl / total_trades as f64;

    let avg_win: f64 = if winning_trades > 0 {
        trades.iter().filter(|t| t.pnl_percent > 0.0).map(|t| t.pnl_percent).sum::<f64>() / winning_trades as f64
    } else {
        0.0
    };

    let up_trades = trades.iter().filter(|t| t.side == Side::Up).count();
    let down_trades = trades.iter().filter(|t| t.side == Side::Down).count();

    let up_wins = trades.iter().filter(|t| t.side == Side::Up && t.pnl_percent > 0.0).count();
    let down_wins = trades.iter().filter(|t| t.side == Side::Down && t.pnl_percent > 0.0).count();

    println!("\n========================================");
    println!("Strategy: {}", strategy_name);
    println!("========================================");
    println!("Total trades:    {}", total_trades);
    println!("Winning trades:  {} ({:.1}%)", winning_trades, win_rate);
    println!("Losing trades:   {}", losing_trades);
    println!("----------------------------------------");
    println!("Total P&L:       {:.1}%", total_pnl);
    println!("Average P&L:     {:.1}%", avg_pnl);
    println!("Average win:     {:.1}%", avg_win);
    println!("----------------------------------------");
    println!("UP trades:       {} (won: {}, {:.1}%)", up_trades, up_wins,
             if up_trades > 0 { up_wins as f64 / up_trades as f64 * 100.0 } else { 0.0 });
    println!("DOWN trades:     {} (won: {}, {:.1}%)", down_trades, down_wins,
             if down_trades > 0 { down_wins as f64 / down_trades as f64 * 100.0 } else { 0.0 });
    println!("----------------------------------------");

    // Show edge distribution
    let mut edge_buckets: HashMap<i32, (i32, i32)> = HashMap::new(); // edge% -> (trades, wins)
    for trade in trades {
        let bucket = (trade.entry_edge * 100.0).round() as i32;
        let entry = edge_buckets.entry(bucket).or_insert((0, 0));
        entry.0 += 1;
        if trade.pnl_percent > 0.0 {
            entry.1 += 1;
        }
    }

    println!("\nEdge distribution:");
    let mut buckets: Vec<_> = edge_buckets.iter().collect();
    buckets.sort_by_key(|(k, _)| *k);
    for (edge, (count, wins)) in buckets {
        let win_pct = *wins as f64 / *count as f64 * 100.0;
        println!("  Edge {:>3}%: {:>4} trades, {:>4} wins ({:.1}%)", edge, count, wins, win_pct);
    }

    // Show some sample trades
    println!("\nSample trades (first 10):");
    for trade in trades.iter().take(10) {
        let outcome_str = if trade.pnl_percent > 0.0 { "WIN" } else { "LOSS" };
        println!("  {} {:?} @ {:.2}c, edge={:.1}%, t={}s -> {} ({:.1}%)",
                 &trade.market_slug[..trade.market_slug.len().min(30)],
                 trade.side,
                 trade.entry_price * 100.0,
                 trade.entry_edge * 100.0,
                 trade.entry_time_elapsed,
                 outcome_str,
                 trade.pnl_percent);
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("backtester=info".parse()?))
        .init();

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║           BTC 15-Minute Market Backtester                      ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    // Connect to database
    let config = DbConfig::default();
    info!("Connecting to database at {}...", config.host);
    let client = connect_db(&config).await?;
    info!("Connected to database");

    // Load all market data
    let markets = load_markets(&client).await?;

    // Show market summary
    let total_snapshots: usize = markets.iter().map(|m| m.snapshots.len()).sum();
    println!("\nData Summary:");
    println!("  Total markets: {}", markets.len());
    println!("  Total snapshots: {}", total_snapshots);

    // Count outcomes
    let mut up_wins = 0;
    let mut down_wins = 0;
    let mut unknown = 0;
    for market in &markets {
        match determine_outcome(market) {
            Outcome::UpWins => up_wins += 1,
            Outcome::DownWins => down_wins += 1,
            Outcome::Unknown => unknown += 1,
        }
    }
    println!("  UP won: {}", up_wins);
    println!("  DOWN won: {}", down_wins);
    println!("  Unknown/Active: {}", unknown);

    // Test different strategies
    let strategies = vec![
        StrategyConfig {
            name: "Edge >= 5%, any time".to_string(),
            min_edge: 0.05,
            max_entry_time: 900,
            min_entry_time: 0,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 10%, any time".to_string(),
            min_edge: 0.10,
            max_entry_time: 900,
            min_entry_time: 0,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 15%, any time".to_string(),
            min_edge: 0.15,
            max_entry_time: 900,
            min_entry_time: 0,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 20%, any time".to_string(),
            min_edge: 0.20,
            max_entry_time: 900,
            min_entry_time: 0,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 10%, first 5 min only".to_string(),
            min_edge: 0.10,
            max_entry_time: 300,
            min_entry_time: 0,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 10%, after 5 min".to_string(),
            min_edge: 0.10,
            max_entry_time: 900,
            min_entry_time: 300,
            min_liquidity: 0.0,
            hold_to_expiration: true,
        },
        StrategyConfig {
            name: "Edge >= 5%, min 100 liquidity".to_string(),
            min_edge: 0.05,
            max_entry_time: 900,
            min_entry_time: 0,
            min_liquidity: 100.0,
            hold_to_expiration: true,
        },
    ];

    for config in &strategies {
        let trades = simulate_strategy(&markets, config);
        analyze_trades(&trades, &config.name);
    }

    println!("\n========================================");
    println!("Backtesting complete!");
    println!("========================================");

    Ok(())
}
