//! Strategy Simulator for BTC 15-Minute Markets
//!
//! Simulates different trading scenarios to compare:
//! - Buy edge thresholds
//! - Sell edge thresholds
//! - Min profit requirements
//! - Max trades per window
//! - Cooldown periods
//!
//! Usage:
//!   btc-strategy-sim --hours 4
//!   btc-strategy-sim --hours 24 --compare-buy-edges "0.05,0.07,0.10"
//!   btc-strategy-sim --hours 24 --compare-sell-edges "0.05,0.10,0.15"
//!   btc-strategy-sim --hours 24 --compare-min-profits "0.0,0.10,0.20,0.50"

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
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
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "btc-strategy-sim")]
#[command(about = "Simulate different trading strategies on historical market data")]
struct Args {
    /// Number of hours of data to analyze
    #[arg(short = 'H', long, default_value = "4")]
    hours: u32,

    /// Minimum BUY edge threshold (e.g., 0.07 = 7%)
    #[arg(long, default_value = "0.05")]
    min_buy_edge: f64,

    /// Minimum SELL edge threshold (e.g., 0.10 = 10%)
    #[arg(long, default_value = "0.10")]
    min_sell_edge: f64,

    /// Minimum profit before allowing sell (e.g., 0.0 = 0%)
    #[arg(long, default_value = "0.0")]
    min_profit: f64,

    /// Maximum spread allowed (e.g., 0.30 = 30%)
    #[arg(short = 's', long, default_value = "0.30")]
    max_spread: f64,

    /// Bet amount in USDC
    #[arg(short = 'b', long, default_value = "1.0")]
    bet_amount: f64,

    /// Output CSV file (optional)
    #[arg(short = 'o', long)]
    output: Option<String>,

    /// Show per-market breakdown
    #[arg(short = 'm', long)]
    per_market: bool,

    /// Compare multiple BUY edge thresholds (comma-separated)
    #[arg(long)]
    compare_buy_edges: Option<String>,

    /// Compare multiple SELL edge thresholds (comma-separated)
    #[arg(long)]
    compare_sell_edges: Option<String>,

    /// Compare multiple min profit thresholds (comma-separated)
    #[arg(long)]
    compare_min_profits: Option<String>,

    /// Compare multiple cooldowns (comma-separated, in seconds)
    #[arg(long)]
    compare_cooldowns: Option<String>,

    /// Compare multiple max-trades (comma-separated)
    #[arg(long)]
    compare_max_trades: Option<String>,
}

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
                .unwrap_or_else(|_| "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com".to_string()),
            port: 5432,
            user: std::env::var("DB_USER").unwrap_or_else(|_| "qoveryadmin".to_string()),
            password: std::env::var("DB_PASSWORD")
                .unwrap_or_else(|_| "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp".to_string()),
            database: std::env::var("DB_NAME").unwrap_or_else(|_| "polymarket".to_string()),
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
    time_elapsed: i32,
    // UP side
    price_up: f64,      // Ask (cost to buy)
    bid_up: f64,        // Bid (what we get if we sell)
    edge_up: f64,       // Buy edge
    edge_up_sell: f64,  // Sell edge
    // DOWN side
    price_down: f64,
    bid_down: f64,
    edge_down: f64,
    edge_down_sell: f64,
    // Spread
    spread_up: f64,
    spread_down: f64,
}

#[derive(Debug, Clone)]
struct Market {
    slug: String,
    outcome: Outcome,
    snapshots: Vec<MarketSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Up => write!(f, "UP"),
            Direction::Down => write!(f, "DOWN"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    UpWins,
    DownWins,
}

#[derive(Debug, Clone)]
struct Position {
    direction: Direction,
    entry_price: f64,
    entry_time: i32,
}

#[derive(Debug, Clone)]
struct Trade {
    direction: Direction,
    entry_price: f64,
    entry_time: i32,
    exit_price: f64,
    exit_time: i32,
    exit_reason: String,  // "SELL_EDGE", "EXPIRATION_WIN", "EXPIRATION_LOSS"
    pnl_pct: f64,
}

impl Trade {
    fn pnl(&self, bet_amount: f64) -> f64 {
        bet_amount * self.pnl_pct / 100.0
    }

    fn is_win(&self) -> bool {
        self.pnl_pct > 0.0
    }
}

#[derive(Debug, Clone)]
struct ScenarioConfig {
    name: String,
    min_buy_edge: f64,
    min_sell_edge: f64,
    min_profit: f64,
    max_spread: f64,
    cooldown_seconds: i32,
    max_trades_per_market: Option<u32>,
}

#[derive(Debug, Clone, Default)]
struct ScenarioResult {
    name: String,
    total_trades: u32,
    wins: u32,
    losses: u32,
    total_pnl: f64,
    markets_traded: u32,
    sell_edge_exits: u32,
    expiration_wins: u32,
    expiration_losses: u32,
    // Capital metrics (simple = total bets placed)
    total_capital_invested: f64,
    max_capital_per_window: f64,
    capital_per_window_sum: f64,  // For calculating average
    // Real capital metrics (peak concurrent = max held at any moment, accounting for sells)
    total_real_capital: f64,      // Sum of peak capital across all windows
    max_real_capital_per_window: f64,
    real_capital_per_window_sum: f64,
}

#[derive(Debug, Clone, Default)]
struct MarketCapitalInfo {
    simple_capital: f64,    // Total bets placed
    peak_capital: f64,      // Max concurrent capital (accounting for sells)
}

impl ScenarioResult {
    fn win_rate(&self) -> f64 {
        if self.total_trades == 0 { 0.0 } else { self.wins as f64 / self.total_trades as f64 * 100.0 }
    }

    fn pnl_per_trade(&self) -> f64 {
        if self.total_trades == 0 { 0.0 } else { self.total_pnl / self.total_trades as f64 }
    }

    fn total_return_pct(&self) -> f64 {
        if self.total_capital_invested == 0.0 { 0.0 } else { self.total_pnl / self.total_capital_invested * 100.0 }
    }

    fn avg_capital_per_window(&self) -> f64 {
        if self.markets_traded == 0 { 0.0 } else { self.capital_per_window_sum / self.markets_traded as f64 }
    }

    // Real capital metrics (accounting for sells freeing capital)
    fn real_return_pct(&self) -> f64 {
        if self.total_real_capital == 0.0 { 0.0 } else { self.total_pnl / self.total_real_capital * 100.0 }
    }

    fn avg_real_capital_per_window(&self) -> f64 {
        if self.markets_traded == 0 { 0.0 } else { self.real_capital_per_window_sum / self.markets_traded as f64 }
    }
}

#[derive(Debug, Clone)]
struct MarketResult {
    market_slug: String,
    outcome: Outcome,
    trades: u32,
    wins: u32,
    pnl: f64,
    sell_exits: u32,
    exp_wins: u32,
    exp_losses: u32,
    capital_invested: f64,  // Capital used in this market
}

// ============================================================================
// Data Loading
// ============================================================================

async fn load_markets(client: &Client, hours: u32) -> Result<Vec<Market>> {
    info!("Loading market data from last {} hours...", hours);

    let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);

    // First get resolved markets and their outcomes
    let outcomes_query = r#"
        SELECT market_slug,
               (ARRAY_AGG(price_up ORDER BY timestamp DESC))[1] as final_up_price,
               (ARRAY_AGG(price_delta ORDER BY timestamp DESC))[1] as final_delta
        FROM market_logs
        WHERE timestamp > $1
        GROUP BY market_slug
        HAVING MAX(time_elapsed) >= 800
    "#;

    let outcome_rows = client.query(outcomes_query, &[&cutoff]).await?;

    let mut market_outcomes: HashMap<String, Outcome> = HashMap::new();
    for row in outcome_rows {
        let slug: String = row.get("market_slug");
        let final_up: f64 = row.get::<_, Decimal>("final_up_price").to_f64().unwrap_or(0.5);
        let final_delta: f64 = row.get::<_, Decimal>("final_delta").to_f64().unwrap_or(0.0);

        let outcome = if final_up >= 0.95 || final_delta > 20.0 {
            Outcome::UpWins
        } else if final_up <= 0.05 || final_delta < -20.0 {
            Outcome::DownWins
        } else if final_delta > 0.0 {
            Outcome::UpWins
        } else {
            Outcome::DownWins
        };

        market_outcomes.insert(slug, outcome);
    }

    info!("Found {} resolved markets", market_outcomes.len());

    // Now load all snapshots for these markets
    let snapshots_query = r#"
        SELECT
            market_slug,
            time_elapsed,
            price_up,
            price_down,
            bid_up,
            bid_down,
            edge_up,
            edge_down,
            edge_up_sell,
            edge_down_sell
        FROM market_logs
        WHERE timestamp > $1
          AND time_elapsed <= 885
        ORDER BY market_slug, time_elapsed
    "#;

    let rows = client.query(snapshots_query, &[&cutoff]).await?;

    let mut markets_map: HashMap<String, Vec<MarketSnapshot>> = HashMap::new();

    for row in rows {
        let slug: String = row.get("market_slug");

        // Skip if not a resolved market
        if !market_outcomes.contains_key(&slug) {
            continue;
        }

        let price_up = row.get::<_, Decimal>("price_up").to_f64().unwrap_or(0.5);
        let price_down = row.get::<_, Decimal>("price_down").to_f64().unwrap_or(0.5);
        let bid_up = row.get::<_, Option<Decimal>>("bid_up").and_then(|d| d.to_f64()).unwrap_or(0.0);
        let bid_down = row.get::<_, Option<Decimal>>("bid_down").and_then(|d| d.to_f64()).unwrap_or(0.0);

        let spread_up = if bid_up > 0.0 { (price_up - bid_up) / ((price_up + bid_up) / 2.0) } else { 1.0 };
        let spread_down = if bid_down > 0.0 { (price_down - bid_down) / ((price_down + bid_down) / 2.0) } else { 1.0 };

        let snapshot = MarketSnapshot {
            time_elapsed: row.get("time_elapsed"),
            price_up,
            price_down,
            bid_up,
            bid_down,
            edge_up: row.get::<_, Option<Decimal>>("edge_up").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_down: row.get::<_, Option<Decimal>>("edge_down").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_up_sell: row.get::<_, Option<Decimal>>("edge_up_sell").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_down_sell: row.get::<_, Option<Decimal>>("edge_down_sell").and_then(|d| d.to_f64()).unwrap_or(0.0),
            spread_up,
            spread_down,
        };

        markets_map.entry(slug).or_default().push(snapshot);
    }

    let markets: Vec<Market> = markets_map
        .into_iter()
        .filter_map(|(slug, snapshots)| {
            market_outcomes.get(&slug).map(|&outcome| Market {
                slug,
                outcome,
                snapshots,
            })
        })
        .collect();

    let total_snapshots: usize = markets.iter().map(|m| m.snapshots.len()).sum();
    info!("Loaded {} snapshots from {} markets", total_snapshots, markets.len());

    Ok(markets)
}

// ============================================================================
// Strategy Simulation
// ============================================================================

fn simulate_market(market: &Market, config: &ScenarioConfig, bet_amount: f64) -> (Vec<Trade>, MarketCapitalInfo) {
    let mut trades: Vec<Trade> = Vec::new();
    let mut open_positions: Vec<Position> = Vec::new();  // Can hold MULTIPLE positions
    let mut last_entry_time: Option<i32> = None;
    let mut trades_count = 0u32;

    // Capital tracking
    let mut total_bets_placed = 0.0;      // Simple: total $ bet
    let mut current_capital = 0.0;         // Real: currently held
    let mut peak_capital = 0.0;            // Real: max concurrent

    for snapshot in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: Check SELL conditions if we have ANY positions
        // ═══════════════════════════════════════════════════════════════
        if !open_positions.is_empty() {
            // Get the direction of our positions (all should be same direction)
            let direction = open_positions[0].direction;
            let (sell_edge, bid_price) = match direction {
                Direction::Up => (snapshot.edge_up_sell, snapshot.bid_up),
                Direction::Down => (snapshot.edge_down_sell, snapshot.bid_down),
            };

            if bid_price > 0.0 && sell_edge >= config.min_sell_edge {
                // Check if we should sell ALL positions
                // Calculate average entry price for profit check
                let avg_entry: f64 = open_positions.iter().map(|p| p.entry_price).sum::<f64>()
                    / open_positions.len() as f64;
                let profit_pct = (bid_price - avg_entry) / avg_entry;

                if profit_pct >= config.min_profit {
                    // Sell ALL positions at current bid - CAPITAL FREED!
                    let positions_sold = open_positions.len();
                    for pos in open_positions.drain(..) {
                        let pos_profit_pct = (bid_price - pos.entry_price) / pos.entry_price * 100.0;
                        trades.push(Trade {
                            direction: pos.direction,
                            entry_price: pos.entry_price,
                            entry_time: pos.entry_time,
                            exit_price: bid_price,
                            exit_time: snapshot.time_elapsed,
                            exit_reason: "SELL_EDGE".to_string(),
                            pnl_pct: pos_profit_pct,
                        });
                    }
                    // Capital comes back when we sell!
                    current_capital -= positions_sold as f64 * bet_amount;
                    // Don't continue - allow buying again in same snapshot if edge exists
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Check BUY conditions (can accumulate multiple positions)
        // ═══════════════════════════════════════════════════════════════

        // Check max trades
        if let Some(max) = config.max_trades_per_market {
            if trades_count >= max {
                continue;
            }
        }

        // Check cooldown
        if let Some(last_time) = last_entry_time {
            if snapshot.time_elapsed - last_time < config.cooldown_seconds {
                continue;
            }
        }

        // Determine which direction to buy (if any)
        let buy_down = snapshot.edge_down >= config.min_buy_edge && snapshot.spread_down <= config.max_spread;
        let buy_up = snapshot.edge_up >= config.min_buy_edge && snapshot.spread_up <= config.max_spread;

        let mut bought = false;

        // If we have existing positions, only buy in the SAME direction
        if !open_positions.is_empty() {
            let current_direction = open_positions[0].direction;
            match current_direction {
                Direction::Down if buy_down => {
                    open_positions.push(Position {
                        direction: Direction::Down,
                        entry_price: snapshot.price_down,
                        entry_time: snapshot.time_elapsed,
                    });
                    last_entry_time = Some(snapshot.time_elapsed);
                    trades_count += 1;
                    bought = true;
                }
                Direction::Up if buy_up => {
                    open_positions.push(Position {
                        direction: Direction::Up,
                        entry_price: snapshot.price_up,
                        entry_time: snapshot.time_elapsed,
                    });
                    last_entry_time = Some(snapshot.time_elapsed);
                    trades_count += 1;
                    bought = true;
                }
                _ => {} // No matching direction or no edge
            }
        } else {
            // No existing positions - can buy either direction (prefer DOWN)
            if buy_down {
                open_positions.push(Position {
                    direction: Direction::Down,
                    entry_price: snapshot.price_down,
                    entry_time: snapshot.time_elapsed,
                });
                last_entry_time = Some(snapshot.time_elapsed);
                trades_count += 1;
                bought = true;
            } else if buy_up {
                open_positions.push(Position {
                    direction: Direction::Up,
                    entry_price: snapshot.price_up,
                    entry_time: snapshot.time_elapsed,
                });
                last_entry_time = Some(snapshot.time_elapsed);
                trades_count += 1;
                bought = true;
            }
        }

        // Update capital if we bought
        if bought {
            total_bets_placed += bet_amount;
            current_capital += bet_amount;
            if current_capital > peak_capital {
                peak_capital = current_capital;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // STEP 3: Settle ALL remaining positions at expiration
    // ═══════════════════════════════════════════════════════════════
    for pos in open_positions {
        let won = match (pos.direction, market.outcome) {
            (Direction::Up, Outcome::UpWins) => true,
            (Direction::Down, Outcome::DownWins) => true,
            _ => false,
        };

        let pnl_pct = if won {
            (1.0 - pos.entry_price) / pos.entry_price * 100.0
        } else {
            -100.0
        };

        trades.push(Trade {
            direction: pos.direction,
            entry_price: pos.entry_price,
            entry_time: pos.entry_time,
            exit_price: if won { 1.0 } else { 0.0 },
            exit_time: 900,
            exit_reason: if won { "EXPIRATION_WIN".to_string() } else { "EXPIRATION_LOSS".to_string() },
            pnl_pct,
        });
    }

    let capital_info = MarketCapitalInfo {
        simple_capital: total_bets_placed,
        peak_capital,
    };

    (trades, capital_info)
}

fn run_scenario(markets: &[Market], config: &ScenarioConfig, bet_amount: f64) -> (ScenarioResult, Vec<MarketResult>) {
    let mut result = ScenarioResult {
        name: config.name.clone(),
        ..Default::default()
    };
    let mut market_results = Vec::new();

    for market in markets {
        let (trades, capital_info) = simulate_market(market, config, bet_amount);

        if trades.is_empty() {
            continue;
        }

        let mut mr = MarketResult {
            market_slug: market.slug.clone(),
            outcome: market.outcome,
            trades: trades.len() as u32,
            wins: 0,
            pnl: 0.0,
            sell_exits: 0,
            exp_wins: 0,
            exp_losses: 0,
            capital_invested: capital_info.simple_capital,
        };

        for trade in &trades {
            let pnl = trade.pnl(bet_amount);
            mr.pnl += pnl;
            result.total_pnl += pnl;
            result.total_trades += 1;

            if trade.is_win() {
                mr.wins += 1;
                result.wins += 1;
            } else {
                result.losses += 1;
            }

            match trade.exit_reason.as_str() {
                "SELL_EDGE" => { mr.sell_exits += 1; result.sell_edge_exits += 1; }
                "EXPIRATION_WIN" => { mr.exp_wins += 1; result.expiration_wins += 1; }
                "EXPIRATION_LOSS" => { mr.exp_losses += 1; result.expiration_losses += 1; }
                _ => {}
            }
        }

        // Update SIMPLE capital metrics (total bets placed)
        result.total_capital_invested += capital_info.simple_capital;
        result.capital_per_window_sum += capital_info.simple_capital;
        if capital_info.simple_capital > result.max_capital_per_window {
            result.max_capital_per_window = capital_info.simple_capital;
        }

        // Update REAL capital metrics (peak concurrent, accounting for sells)
        result.total_real_capital += capital_info.peak_capital;
        result.real_capital_per_window_sum += capital_info.peak_capital;
        if capital_info.peak_capital > result.max_real_capital_per_window {
            result.max_real_capital_per_window = capital_info.peak_capital;
        }

        result.markets_traded += 1;
        market_results.push(mr);
    }

    (result, market_results)
}

// ============================================================================
// Output Formatting
// ============================================================================

fn print_scenario_comparison(results: &[ScenarioResult], bet_amount: f64) {
    println!("\n{}", "=".repeat(200));
    println!("{:^200}", format!("SCENARIO COMPARISON (${:.0} bets)", bet_amount));
    println!("{}", "=".repeat(200));
    println!();

    // Header with both simple and real capital
    println!(
        "{:<30} {:>6} {:>6} {:>8} | {:>10} {:>8} {:>8} | {:>10} {:>8} {:>8} | {:>7} {:>7} {:>7}",
        "Scenario", "Trades", "Win%", "P&L",
        "SimpleCap", "Return%", "MaxCap/W",
        "RealCap", "RealRet%", "RealMax/W",
        "SellEx", "ExpWin", "ExpLoss"
    );
    println!("{}", "-".repeat(200));

    for r in results {
        println!(
            "{:<30} {:>6} {:>5.1}% {:>7.0}$ | {:>9.0}$ {:>7.1}% {:>7.0}$ | {:>9.0}$ {:>7.1}% {:>8.0}$ | {:>7} {:>7} {:>7}",
            r.name,
            r.total_trades,
            r.win_rate(),
            r.total_pnl,
            // Simple capital
            r.total_capital_invested,
            r.total_return_pct(),
            r.max_capital_per_window,
            // Real capital (peak concurrent)
            r.total_real_capital,
            r.real_return_pct(),
            r.max_real_capital_per_window,
            // Exit types
            r.sell_edge_exits,
            r.expiration_wins,
            r.expiration_losses,
        );
    }
    println!("{}", "-".repeat(200));
}

fn print_market_breakdown(market_results: &[MarketResult]) {
    println!("\n{}", "=".repeat(100));
    println!("{:^100}", "PER-MARKET BREAKDOWN");
    println!("{}", "=".repeat(100));
    println!();

    println!(
        "{:<40} {:>8} {:>7} {:>7} {:>10} {:>8} {:>8} {:>8}",
        "Market", "Outcome", "Trades", "Wins", "P&L", "SellEx", "ExpWin", "ExpLoss"
    );
    println!("{}", "-".repeat(100));

    let mut sorted = market_results.to_vec();
    sorted.sort_by(|a, b| b.pnl.partial_cmp(&a.pnl).unwrap());

    for mr in &sorted {
        let outcome_str = match mr.outcome { Outcome::UpWins => "UP", Outcome::DownWins => "DOWN" };
        let display_slug = mr.market_slug.split("btc-").last().unwrap_or(&mr.market_slug);

        println!(
            "{:<40} {:>8} {:>7} {:>7} {:>9.2}$ {:>8} {:>8} {:>8}",
            display_slug, outcome_str, mr.trades, mr.wins, mr.pnl, mr.sell_exits, mr.exp_wins, mr.exp_losses
        );
    }

    let total_pnl: f64 = sorted.iter().map(|m| m.pnl).sum();
    let total_trades: u32 = sorted.iter().map(|m| m.trades).sum();
    let total_wins: u32 = sorted.iter().map(|m| m.wins).sum();

    println!("{}", "-".repeat(100));
    println!(
        "{:<40} {:>8} {:>7} {:>7} {:>9.2}$",
        "TOTAL", "", total_trades, total_wins, total_pnl
    );
}

fn write_csv(results: &[ScenarioResult], path: &str) -> Result<()> {
    let mut file = File::create(path)?;

    writeln!(file, "scenario,trades,wins,losses,win_pct,total_pnl,simple_capital,simple_return_pct,simple_max_cap,real_capital,real_return_pct,real_max_cap,pnl_per_trade,sell_exits,exp_wins,exp_losses,markets")?;

    for r in results {
        writeln!(
            file,
            "{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.4},{},{},{},{}",
            r.name, r.total_trades, r.wins, r.losses, r.win_rate(),
            r.total_pnl,
            // Simple capital
            r.total_capital_invested, r.total_return_pct(), r.max_capital_per_window,
            // Real capital
            r.total_real_capital, r.real_return_pct(), r.max_real_capital_per_window,
            r.pnl_per_trade(), r.sell_edge_exits,
            r.expiration_wins, r.expiration_losses, r.markets_traded
        )?;
    }

    info!("Results written to {}", path);
    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("strategy_simulator=info".parse()?))
        .init();

    let args = Args::parse();

    println!();
    println!("{}", "=".repeat(80));
    println!("{:^80}", "BTC 15-MINUTE STRATEGY SIMULATOR v2");
    println!("{:^80}", "(with Buy/Sell Edge + Min Profit Analysis)");
    println!("{}", "=".repeat(80));
    println!();

    let config = DbConfig::default();
    info!("Connecting to database...");
    let client = connect_db(&config).await?;
    info!("Connected!");

    let markets = load_markets(&client, args.hours).await?;

    if markets.is_empty() {
        println!("No resolved markets found in the last {} hours", args.hours);
        return Ok(());
    }

    // Parse comparison options
    let buy_edges: Vec<f64> = args.compare_buy_edges
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![args.min_buy_edge]);

    let sell_edges: Vec<f64> = args.compare_sell_edges
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![args.min_sell_edge]);

    let min_profits: Vec<f64> = args.compare_min_profits
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![args.min_profit]);

    let cooldowns: Vec<i32> = args.compare_cooldowns
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![0]);

    let max_trades: Vec<Option<u32>> = args.compare_max_trades
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| {
            let x = x.trim();
            if x == "unlimited" || x == "0" { Some(None) } else { x.parse().ok().map(Some) }
        }).collect())
        .unwrap_or_else(|| vec![Some(1)]);

    // Build scenarios
    let mut scenarios: Vec<ScenarioConfig> = Vec::new();

    for &buy_edge in &buy_edges {
        for &sell_edge in &sell_edges {
            for &min_profit in &min_profits {
                for &cooldown in &cooldowns {
                    for &max_trade in &max_trades {
                        let name = format!(
                            "B{:.0}%_S{:.0}%_P{:.0}%_C{}s_M{}",
                            buy_edge * 100.0,
                            sell_edge * 100.0,
                            min_profit * 100.0,
                            cooldown,
                            max_trade.map(|m| m.to_string()).unwrap_or_else(|| "inf".to_string())
                        );

                        scenarios.push(ScenarioConfig {
                            name,
                            min_buy_edge: buy_edge,
                            min_sell_edge: sell_edge,
                            min_profit,
                            max_spread: args.max_spread,
                            cooldown_seconds: cooldown,
                            max_trades_per_market: max_trade,
                        });
                    }
                }
            }
        }
    }

    // Run simulations
    let mut results = Vec::new();
    let mut best_market_results: Option<Vec<MarketResult>> = None;
    let mut best_pnl = f64::MIN;

    for scenario in &scenarios {
        let (result, market_results) = run_scenario(&markets, scenario, args.bet_amount);

        if result.total_pnl > best_pnl {
            best_pnl = result.total_pnl;
            best_market_results = Some(market_results);
        }

        results.push(result);
    }

    // Print results
    println!("\nConfiguration:");
    println!("  Time window:     Last {} hours ({} markets)", args.hours, markets.len());
    println!("  Max spread:      {:.1}%", args.max_spread * 100.0);
    println!("  Bet amount:      ${:.2}", args.bet_amount);

    print_scenario_comparison(&results, args.bet_amount);

    // Find best scenarios
    if let Some(best) = results.iter().max_by(|a, b| a.total_pnl.partial_cmp(&b.total_pnl).unwrap()) {
        println!("\nBEST TOTAL P&L: {} (${:.2})", best.name, best.total_pnl);
    }
    if let Some(best) = results.iter().filter(|r| r.total_trades > 0).max_by(|a, b| a.pnl_per_trade().partial_cmp(&b.pnl_per_trade()).unwrap()) {
        println!("BEST P&L/TRADE: {} (${:.4}/trade)", best.name, best.pnl_per_trade());
    }

    if args.per_market {
        if let Some(ref mr) = best_market_results {
            print_market_breakdown(mr);
        }
    }

    if let Some(output_path) = &args.output {
        write_csv(&results, output_path)?;
    }

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "SIMULATION COMPLETE");
    println!("{}", "=".repeat(80));
    println!();

    Ok(())
}
