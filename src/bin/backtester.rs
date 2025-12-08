//! Backtester for BTC 15-Minute Markets
//!
//! Simulates the full buy/sell strategy:
//! - BUY when buy_edge >= threshold
//! - SELL when sell_edge >= threshold AND profit >= min_profit
//! - Or hold to expiration if sell conditions never met

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use tokio_postgres::Client;
use tracing::info;
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
    timestamp: DateTime<Utc>,
    price_up: f64,      // Ask price (cost to buy UP)
    price_down: f64,    // Ask price (cost to buy DOWN)
    size_up: f64,       // Liquidity for buying UP
    size_down: f64,     // Liquidity for buying DOWN
    edge_up: Option<f64>,    // BUY edge for UP
    edge_down: Option<f64>,  // BUY edge for DOWN
    bid_up: Option<f64>,     // Bid price (what we get selling UP)
    bid_down: Option<f64>,   // Bid price (what we get selling DOWN)
    edge_up_sell: Option<f64>,   // SELL edge for UP
    edge_down_sell: Option<f64>, // SELL edge for DOWN
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
struct Position {
    side: Side,
    entry_price: f64,
    entry_time: i32,
}

#[derive(Debug, Clone)]
struct TradeResult {
    market_slug: String,
    side: Side,
    entry_price: f64,
    entry_time: i32,
    exit_price: Option<f64>,  // Some = sold early, None = held to expiration
    exit_time: Option<i32>,
    exit_reason: String,      // "SELL_EDGE", "EXPIRATION_WIN", "EXPIRATION_LOSS"
    pnl_percent: f64,
}

// ============================================================================
// Scenario Configuration
// ============================================================================

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    min_buy_edge: f64,
    min_sell_edge: f64,
    min_profit_to_sell: f64,
    // Dynamic profit thresholds based on time
    profit_first_5min: Option<f64>,  // If Some, use this for first 300s
    profit_after_5min: Option<f64>,  // If Some, use this after 300s
    min_entry_time: i32,
    max_entry_time: i32,
    min_liquidity: f64,
}

impl Scenario {
    fn get_min_profit(&self, time_elapsed: i32) -> f64 {
        // If dynamic thresholds are set, use them
        if let (Some(early), Some(late)) = (self.profit_first_5min, self.profit_after_5min) {
            if time_elapsed <= 300 {
                early
            } else {
                late
            }
        } else {
            self.min_profit_to_sell
        }
    }
}

fn get_scenarios() -> Vec<Scenario> {
    let mut scenarios = Vec::new();

    // Buy edge options: 5%, 7%, 10%
    let buy_edges = vec![0.05, 0.07, 0.10];

    // Sell edge options: 5%, 10%
    let sell_edges = vec![0.05, 0.10];

    // Static profit thresholds
    let profit_thresholds = vec![0.0, 0.05, 0.10, 0.20, 0.50, 0.75, 1.0];

    // Generate static profit combinations
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            for &profit in &profit_thresholds {
                let name = format!(
                    "B{:.0}%_S{:.0}%_P{:.0}%",
                    buy_edge * 100.0,
                    sell_edge * 100.0,
                    profit * 100.0
                );
                scenarios.push(Scenario {
                    name,
                    min_buy_edge: buy_edge,
                    min_sell_edge: sell_edge,
                    min_profit_to_sell: profit,
                    profit_first_5min: None,
                    profit_after_5min: None,
                    min_entry_time: 0,
                    max_entry_time: 660,
                    min_liquidity: 0.0,
                });
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // DYNAMIC PROFIT SCENARIOS (different threshold based on time)
    // ═══════════════════════════════════════════════════════════════

    // Dynamic: 150% first 5min, 75% after
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            let name = format!(
                "B{:.0}%_S{:.0}%_P150>75%",
                buy_edge * 100.0,
                sell_edge * 100.0
            );
            scenarios.push(Scenario {
                name,
                min_buy_edge: buy_edge,
                min_sell_edge: sell_edge,
                min_profit_to_sell: 0.0,  // Not used when dynamic is set
                profit_first_5min: Some(1.5),   // 150% first 5 min
                profit_after_5min: Some(0.75),  // 75% after
                min_entry_time: 0,
                max_entry_time: 660,
                min_liquidity: 0.0,
            });
        }
    }

    // Dynamic: 100% first 5min, 50% after
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            let name = format!(
                "B{:.0}%_S{:.0}%_P100>50%",
                buy_edge * 100.0,
                sell_edge * 100.0
            );
            scenarios.push(Scenario {
                name,
                min_buy_edge: buy_edge,
                min_sell_edge: sell_edge,
                min_profit_to_sell: 0.0,
                profit_first_5min: Some(1.0),   // 100% first 5 min
                profit_after_5min: Some(0.5),   // 50% after
                min_entry_time: 0,
                max_entry_time: 660,
                min_liquidity: 0.0,
            });
        }
    }

    // Dynamic: 100% first 5min, 20% after
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            let name = format!(
                "B{:.0}%_S{:.0}%_P100>20%",
                buy_edge * 100.0,
                sell_edge * 100.0
            );
            scenarios.push(Scenario {
                name,
                min_buy_edge: buy_edge,
                min_sell_edge: sell_edge,
                min_profit_to_sell: 0.0,
                profit_first_5min: Some(1.0),   // 100% first 5 min
                profit_after_5min: Some(0.2),   // 20% after
                min_entry_time: 0,
                max_entry_time: 660,
                min_liquidity: 0.0,
            });
        }
    }

    // Dynamic: 75% first 5min, 20% after
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            let name = format!(
                "B{:.0}%_S{:.0}%_P75>20%",
                buy_edge * 100.0,
                sell_edge * 100.0
            );
            scenarios.push(Scenario {
                name,
                min_buy_edge: buy_edge,
                min_sell_edge: sell_edge,
                min_profit_to_sell: 0.0,
                profit_first_5min: Some(0.75),  // 75% first 5 min
                profit_after_5min: Some(0.2),   // 20% after
                min_entry_time: 0,
                max_entry_time: 660,
                min_liquidity: 0.0,
            });
        }
    }

    // Dynamic: 50% first 5min, 10% after
    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            let name = format!(
                "B{:.0}%_S{:.0}%_P50>10%",
                buy_edge * 100.0,
                sell_edge * 100.0
            );
            scenarios.push(Scenario {
                name,
                min_buy_edge: buy_edge,
                min_sell_edge: sell_edge,
                min_profit_to_sell: 0.0,
                profit_first_5min: Some(0.5),   // 50% first 5 min
                profit_after_5min: Some(0.1),   // 10% after
                min_entry_time: 0,
                max_entry_time: 660,
                min_liquidity: 0.0,
            });
        }
    }

    // Add hold-to-end baselines for comparison
    scenarios.push(Scenario {
        name: "B5%_HoldToEnd".to_string(),
        min_buy_edge: 0.05,
        min_sell_edge: 10.0,
        min_profit_to_sell: 10.0,
        profit_first_5min: None,
        profit_after_5min: None,
        min_entry_time: 0,
        max_entry_time: 660,
        min_liquidity: 0.0,
    });
    scenarios.push(Scenario {
        name: "B7%_HoldToEnd".to_string(),
        min_buy_edge: 0.07,
        min_sell_edge: 10.0,
        min_profit_to_sell: 10.0,
        profit_first_5min: None,
        profit_after_5min: None,
        min_entry_time: 0,
        max_entry_time: 660,
        min_liquidity: 0.0,
    });
    scenarios.push(Scenario {
        name: "B10%_HoldToEnd".to_string(),
        min_buy_edge: 0.10,
        min_sell_edge: 10.0,
        min_profit_to_sell: 10.0,
        profit_first_5min: None,
        profit_after_5min: None,
        min_entry_time: 0,
        max_entry_time: 660,
        min_liquidity: 0.0,
    });

    scenarios
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
                edge_up_sell,
                edge_down_sell,
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
            edge_up_sell: row.get::<_, Option<Decimal>>("edge_up_sell").and_then(|d| d.to_f64()),
            edge_down_sell: row.get::<_, Option<Decimal>>("edge_down_sell").and_then(|d| d.to_f64()),
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
// Outcome Detection
// ============================================================================

fn determine_outcome(market: &Market) -> Outcome {
    if market.snapshots.is_empty() {
        return Outcome::Unknown;
    }

    let last = market.snapshots.last().unwrap();

    // Check final prices - if one side is near 0.99, that side won
    if last.price_up >= 0.95 || last.price_down <= 0.05 {
        return Outcome::UpWins;
    }
    if last.price_down >= 0.95 || last.price_up <= 0.05 {
        return Outcome::DownWins;
    }

    // Check price_delta sign
    if last.price_delta > 0.0 {
        return Outcome::UpWins;
    } else if last.price_delta < 0.0 {
        return Outcome::DownWins;
    }

    // If we have data near end of window, use that
    if last.time_elapsed >= 800 {
        // Near end, use price_delta even if 0
        if last.price_delta >= 0.0 {
            return Outcome::UpWins;  // Tie goes to UP
        }
        return Outcome::DownWins;
    }

    Outcome::Unknown
}

// ============================================================================
// Strategy Simulation
// ============================================================================

fn simulate_market(market: &Market, scenario: &Scenario, outcome: Outcome) -> Vec<TradeResult> {
    let mut trades = Vec::new();
    let mut position: Option<Position> = None;

    for snapshot in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: Check SELL conditions if we have a position
        // ═══════════════════════════════════════════════════════════════
        if let Some(ref pos) = position {
            let (sell_edge, bid_price) = match pos.side {
                Side::Up => (snapshot.edge_up_sell, snapshot.bid_up),
                Side::Down => (snapshot.edge_down_sell, snapshot.bid_down),
            };

            if let (Some(sell_edge), Some(bid)) = (sell_edge, bid_price) {
                let profit_pct = (bid - pos.entry_price) / pos.entry_price;

                // Get dynamic profit threshold based on time
                let min_profit = scenario.get_min_profit(snapshot.time_elapsed);

                // Check sell conditions
                if sell_edge >= scenario.min_sell_edge && profit_pct >= min_profit {
                    let pnl = profit_pct * 100.0;  // Convert to percentage

                    trades.push(TradeResult {
                        market_slug: market.slug.clone(),
                        side: pos.side,
                        entry_price: pos.entry_price,
                        entry_time: pos.entry_time,
                        exit_price: Some(bid),
                        exit_time: Some(snapshot.time_elapsed),
                        exit_reason: "SELL_EDGE".to_string(),
                        pnl_percent: pnl,
                    });

                    position = None;
                    continue;
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Check BUY conditions if no position
        // ═══════════════════════════════════════════════════════════════
        if position.is_none() {
            // Check time constraints
            if snapshot.time_elapsed < scenario.min_entry_time {
                continue;
            }
            if snapshot.time_elapsed > scenario.max_entry_time {
                continue;
            }

            // Check UP buy
            if let Some(edge_up) = snapshot.edge_up {
                if edge_up >= scenario.min_buy_edge && snapshot.size_up >= scenario.min_liquidity {
                    position = Some(Position {
                        side: Side::Up,
                        entry_price: snapshot.price_up,
                        entry_time: snapshot.time_elapsed,
                    });
                    continue;
                }
            }

            // Check DOWN buy
            if let Some(edge_down) = snapshot.edge_down {
                if edge_down >= scenario.min_buy_edge && snapshot.size_down >= scenario.min_liquidity {
                    position = Some(Position {
                        side: Side::Down,
                        entry_price: snapshot.price_down,
                        entry_time: snapshot.time_elapsed,
                    });
                    continue;
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // STEP 3: If still holding at end, settle at expiration
    // ═══════════════════════════════════════════════════════════════
    if let Some(pos) = position {
        let won = match (pos.side, outcome) {
            (Side::Up, Outcome::UpWins) => true,
            (Side::Down, Outcome::DownWins) => true,
            _ => false,
        };

        let pnl = if won {
            (1.0 - pos.entry_price) / pos.entry_price * 100.0
        } else {
            -100.0
        };

        trades.push(TradeResult {
            market_slug: market.slug.clone(),
            side: pos.side,
            entry_price: pos.entry_price,
            entry_time: pos.entry_time,
            exit_price: if won { Some(1.0) } else { Some(0.0) },
            exit_time: Some(900),
            exit_reason: if won { "EXPIRATION_WIN".to_string() } else { "EXPIRATION_LOSS".to_string() },
            pnl_percent: pnl,
        });
    }

    trades
}

// ============================================================================
// CSV Output
// ============================================================================

fn write_csv(markets: &[Market], scenarios: &[Scenario], output_path: &str) -> Result<()> {
    let mut file = File::create(output_path)?;

    // Header: market_slug, outcome, then each scenario name
    let mut header = "market_slug,outcome,total_snapshots".to_string();
    for scenario in scenarios {
        header.push_str(&format!(",{}", scenario.name));
    }
    writeln!(file, "{}", header)?;

    // Sort markets by slug
    let mut sorted_markets: Vec<_> = markets.iter().collect();
    sorted_markets.sort_by(|a, b| a.slug.cmp(&b.slug));

    // For each market, run all scenarios
    for market in sorted_markets {
        let outcome = determine_outcome(market);
        if outcome == Outcome::Unknown {
            continue;  // Skip markets with unknown outcomes
        }

        let outcome_str = match outcome {
            Outcome::UpWins => "UP",
            Outcome::DownWins => "DOWN",
            Outcome::Unknown => "?",
        };

        let mut row = format!("{},{},{}", market.slug, outcome_str, market.snapshots.len());

        for scenario in scenarios {
            let trades = simulate_market(market, scenario, outcome);

            // Calculate total P&L for this market under this scenario
            let total_pnl: f64 = trades.iter().map(|t| t.pnl_percent).sum();
            let trade_count = trades.len();

            // Format: P&L% (n trades)
            if trade_count > 0 {
                row.push_str(&format!(",{:.1}% ({} trades)", total_pnl, trade_count));
            } else {
                row.push_str(",no trade");
            }
        }

        writeln!(file, "{}", row)?;
    }

    Ok(())
}

// ============================================================================
// Summary Analysis
// ============================================================================

const BET_AMOUNT: f64 = 30.0;  // €30 per trade

fn print_summary(markets: &[Market], scenarios: &[Scenario]) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                   SCENARIO COMPARISON (€{:.0} per trade)                      ║", BET_AMOUNT);
    println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

    // Table header
    println!("{:<35} {:>8} {:>8} {:>10} {:>12} {:>10}",
             "Scenario", "Trades", "Wins", "Win%", "P&L (€)", "ROI%");
    println!("{}", "-".repeat(90));

    for scenario in scenarios {
        let mut all_trades = Vec::new();

        for market in markets {
            let outcome = determine_outcome(market);
            if outcome == Outcome::Unknown {
                continue;
            }
            let trades = simulate_market(market, scenario, outcome);
            all_trades.extend(trades);
        }

        if all_trades.is_empty() {
            println!("{:<35} {:>8} {:>8} {:>10} {:>12} {:>10}",
                     scenario.name, 0, 0, "N/A", "N/A", "N/A");
            continue;
        }

        let total_trades = all_trades.len();
        let winning_trades = all_trades.iter().filter(|t| t.pnl_percent > 0.0).count();
        let win_rate = winning_trades as f64 / total_trades as f64 * 100.0;

        // Calculate actual € P&L
        let total_pnl_eur: f64 = all_trades.iter()
            .map(|t| BET_AMOUNT * t.pnl_percent / 100.0)
            .sum();

        let total_invested = total_trades as f64 * BET_AMOUNT;
        let roi = total_pnl_eur / total_invested * 100.0;

        println!("{:<35} {:>8} {:>8} {:>9.1}% {:>11.2}€ {:>9.1}%",
                 scenario.name, total_trades, winning_trades, win_rate, total_pnl_eur, roi);
    }

    // Only show detailed breakdown for top scenarios (skip if too many)
    if scenarios.len() > 25 {
        println!("\n(Detailed breakdown skipped - {} scenarios. See CSV for details)\n", scenarios.len());
        return;
    }

    println!("\n");

    // Detailed breakdown for each scenario
    for scenario in scenarios {
        let mut all_trades = Vec::new();
        let mut sell_edge_exits = 0;
        let mut expiration_wins = 0;
        let mut expiration_losses = 0;

        for market in markets {
            let outcome = determine_outcome(market);
            if outcome == Outcome::Unknown {
                continue;
            }
            let trades = simulate_market(market, scenario, outcome);
            for trade in &trades {
                match trade.exit_reason.as_str() {
                    "SELL_EDGE" => sell_edge_exits += 1,
                    "EXPIRATION_WIN" => expiration_wins += 1,
                    "EXPIRATION_LOSS" => expiration_losses += 1,
                    _ => {}
                }
            }
            all_trades.extend(trades);
        }

        if all_trades.is_empty() {
            continue;
        }

        let up_trades = all_trades.iter().filter(|t| t.side == Side::Up).count();
        let down_trades = all_trades.iter().filter(|t| t.side == Side::Down).count();
        let up_wins = all_trades.iter().filter(|t| t.side == Side::Up && t.pnl_percent > 0.0).count();
        let down_wins = all_trades.iter().filter(|t| t.side == Side::Down && t.pnl_percent > 0.0).count();

        // Calculate € amounts
        let total_pnl_eur: f64 = all_trades.iter()
            .map(|t| BET_AMOUNT * t.pnl_percent / 100.0)
            .sum();
        let sell_edge_pnl: f64 = all_trades.iter()
            .filter(|t| t.exit_reason == "SELL_EDGE")
            .map(|t| BET_AMOUNT * t.pnl_percent / 100.0)
            .sum();
        let exp_win_pnl: f64 = all_trades.iter()
            .filter(|t| t.exit_reason == "EXPIRATION_WIN")
            .map(|t| BET_AMOUNT * t.pnl_percent / 100.0)
            .sum();
        let exp_loss_pnl: f64 = all_trades.iter()
            .filter(|t| t.exit_reason == "EXPIRATION_LOSS")
            .map(|t| BET_AMOUNT * t.pnl_percent / 100.0)
            .sum();

        println!("┌─ {} ─┐", scenario.name);
        println!("│ Total P&L: €{:.2}", total_pnl_eur);
        println!("│ Exit breakdown:");
        println!("│   SELL_EDGE:       {:>3} trades → €{:>7.2}", sell_edge_exits, sell_edge_pnl);
        println!("│   EXPIRATION_WIN:  {:>3} trades → €{:>7.2}", expiration_wins, exp_win_pnl);
        println!("│   EXPIRATION_LOSS: {:>3} trades → €{:>7.2}", expiration_losses, exp_loss_pnl);
        println!("│ Side breakdown:");
        println!("│   UP:   {} trades, {} wins ({:.1}%)", up_trades, up_wins,
                 if up_trades > 0 { up_wins as f64 / up_trades as f64 * 100.0 } else { 0.0 });
        println!("│   DOWN: {} trades, {} wins ({:.1}%)", down_trades, down_wins,
                 if down_trades > 0 { down_wins as f64 / down_trades as f64 * 100.0 } else { 0.0 });
        println!("└────────────────────────────────────────────┘\n");
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

    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║           BTC 15-Minute Market Backtester v2 (Buy + Sell)                  ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

    // Connect to database
    let config = DbConfig::default();
    info!("Connecting to database at {}...", config.host);
    let client = connect_db(&config).await?;
    info!("Connected to database");

    // Load all market data
    let markets = load_markets(&client).await?;

    // Get scenarios
    let scenarios = get_scenarios();

    // Show data summary
    let total_snapshots: usize = markets.iter().map(|m| m.snapshots.len()).sum();
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

    println!("Data Summary:");
    println!("  Total markets: {}", markets.len());
    println!("  Total snapshots: {}", total_snapshots);
    println!("  UP won: {}", up_wins);
    println!("  DOWN won: {}", down_wins);
    println!("  Unknown/Active: {}", unknown);

    // Print summary to console
    print_summary(&markets, &scenarios);

    // Write detailed CSV
    let csv_path = "output/backtest_results.csv";
    std::fs::create_dir_all("output")?;
    write_csv(&markets, &scenarios, csv_path)?;
    println!("Detailed results written to: {}", csv_path);

    println!("\n═══════════════════════════════════════════════════════════════════════════════");
    println!("                              BACKTESTING COMPLETE");
    println!("═══════════════════════════════════════════════════════════════════════════════\n");

    Ok(())
}
