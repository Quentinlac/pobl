//! Arbitrage Backtester for BTC 15-Minute Markets
//!
//! Strategy: Buy BOTH UP and DOWN when combined ask < threshold
//! Since one side MUST win ($1 payout), this locks in profit if cost < $1
//!
//! Two exit strategies:
//! 1. HOLD TO EXPIRATION: Get $1 guaranteed, profit = $1 - entry_cost
//! 2. EARLY EXIT: If one side's bid rises enough, sell for profit
//!
//! Usage:
//!   btc-arbitrage-backtest --hours 24
//!   btc-arbitrage-backtest --hours 24 --max-ask-sum 1.02 --early-exit-profit 0.05

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_postgres::Client;
use tracing::info;
use tracing_subscriber::EnvFilter;

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "btc-arbitrage-backtest")]
#[command(about = "Backtest the both-sides arbitrage strategy on historical market data")]
struct Args {
    /// Number of hours of data to analyze
    #[arg(short = 'H', long, default_value = "24")]
    hours: u32,

    /// Maximum combined ask price to enter (e.g., 1.02 = max 2% cost over $1)
    #[arg(long, default_value = "1.02")]
    max_ask_sum: f64,

    /// Minimum combined ask to consider (filter out extreme cases)
    #[arg(long, default_value = "1.00")]
    min_ask_sum: f64,

    /// Minimum liquidity on BOTH sides to enter
    #[arg(long, default_value = "10.0")]
    min_liquidity: f64,

    /// Entry time window start (seconds into the 15-min window)
    #[arg(long, default_value = "30")]
    min_entry_time: i32,

    /// Entry time window end (seconds)
    #[arg(long, default_value = "600")]
    max_entry_time: i32,

    /// Enable early exit strategy
    #[arg(long, default_value = "true")]
    early_exit: bool,

    /// Minimum profit % to trigger early exit on one side (e.g., 0.10 = 10%)
    #[arg(long, default_value = "0.15")]
    early_exit_profit: f64,

    /// Bet amount per SIDE in USDC (total position = 2x this)
    #[arg(short = 'b', long, default_value = "15.0")]
    bet_amount: f64,

    /// Compare multiple max-ask-sum thresholds
    #[arg(long)]
    compare_max_sums: Option<String>,

    /// Compare multiple early-exit profit thresholds
    #[arg(long)]
    compare_exit_profits: Option<String>,

    /// Show per-market breakdown
    #[arg(short = 'm', long)]
    per_market: bool,
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
    // Prices (ask = cost to buy)
    ask_up: f64,
    ask_down: f64,
    // Bids (what we get when selling)
    bid_up: f64,
    bid_down: f64,
    // Liquidity
    size_up: f64,
    size_down: f64,
}

impl MarketSnapshot {
    fn ask_sum(&self) -> f64 {
        self.ask_up + self.ask_down
    }

    fn bid_sum(&self) -> f64 {
        self.bid_up + self.bid_down
    }

    fn min_liquidity(&self) -> f64 {
        self.size_up.min(self.size_down)
    }
}

#[derive(Debug, Clone)]
struct Market {
    slug: String,
    outcome: Outcome,
    snapshots: Vec<MarketSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    UpWins,
    DownWins,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::UpWins => write!(f, "UP"),
            Outcome::DownWins => write!(f, "DOWN"),
        }
    }
}

#[derive(Debug, Clone)]
struct ArbitragePosition {
    entry_ask_up: f64,
    entry_ask_down: f64,
    entry_time: i32,
    entry_cost: f64,  // Total cost = ask_up + ask_down
}

#[derive(Debug, Clone)]
struct Trade {
    entry_cost: f64,
    entry_time: i32,
    exit_value: f64,
    exit_time: i32,
    exit_reason: String,  // "EARLY_EXIT_UP", "EARLY_EXIT_DOWN", "EXPIRATION"
    pnl: f64,             // Absolute P&L per unit bet
    pnl_pct: f64,         // P&L as percentage of entry cost
}

#[derive(Debug, Clone)]
struct ScenarioConfig {
    name: String,
    max_ask_sum: f64,
    min_ask_sum: f64,
    min_liquidity: f64,
    min_entry_time: i32,
    max_entry_time: i32,
    early_exit: bool,
    early_exit_profit: f64,
}

#[derive(Debug, Clone, Default)]
struct ScenarioResult {
    name: String,
    total_trades: u32,
    early_exits_up: u32,
    early_exits_down: u32,
    expirations: u32,
    total_pnl: f64,
    total_invested: f64,
    markets_traded: u32,
    avg_entry_cost: f64,
    avg_hold_time: f64,
}

impl ScenarioResult {
    fn return_pct(&self) -> f64 {
        if self.total_invested == 0.0 { 0.0 } else { self.total_pnl / self.total_invested * 100.0 }
    }

    fn pnl_per_trade(&self) -> f64 {
        if self.total_trades == 0 { 0.0 } else { self.total_pnl / self.total_trades as f64 }
    }

    fn early_exit_rate(&self) -> f64 {
        if self.total_trades == 0 { 0.0 }
        else { (self.early_exits_up + self.early_exits_down) as f64 / self.total_trades as f64 * 100.0 }
    }
}

#[derive(Debug, Clone)]
struct MarketResult {
    market_slug: String,
    outcome: Outcome,
    entry_cost: f64,
    exit_value: f64,
    pnl: f64,
    exit_reason: String,
    hold_time: i32,
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

    // Load all snapshots
    let snapshots_query = r#"
        SELECT
            market_slug,
            time_elapsed,
            price_up,
            price_down,
            bid_up,
            bid_down,
            size_up,
            size_down
        FROM market_logs
        WHERE timestamp > $1
          AND time_elapsed <= 885
        ORDER BY market_slug, time_elapsed
    "#;

    let rows = client.query(snapshots_query, &[&cutoff]).await?;

    let mut markets_map: HashMap<String, Vec<MarketSnapshot>> = HashMap::new();

    for row in rows {
        let slug: String = row.get("market_slug");

        if !market_outcomes.contains_key(&slug) {
            continue;
        }

        let snapshot = MarketSnapshot {
            time_elapsed: row.get("time_elapsed"),
            ask_up: row.get::<_, Decimal>("price_up").to_f64().unwrap_or(0.5),
            ask_down: row.get::<_, Decimal>("price_down").to_f64().unwrap_or(0.5),
            bid_up: row.get::<_, Option<Decimal>>("bid_up").and_then(|d| d.to_f64()).unwrap_or(0.0),
            bid_down: row.get::<_, Option<Decimal>>("bid_down").and_then(|d| d.to_f64()).unwrap_or(0.0),
            size_up: row.get::<_, Decimal>("size_up").to_f64().unwrap_or(0.0),
            size_down: row.get::<_, Decimal>("size_down").to_f64().unwrap_or(0.0),
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

fn simulate_market(market: &Market, config: &ScenarioConfig, bet_amount: f64) -> Option<Trade> {
    let mut position: Option<ArbitragePosition> = None;

    for snapshot in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: If we have a position, check for early exit
        // ═══════════════════════════════════════════════════════════════
        if let Some(ref pos) = position {
            if config.early_exit {
                // Check if we can sell UP side for profit
                if snapshot.bid_up > 0.0 {
                    let up_profit_pct = (snapshot.bid_up - pos.entry_ask_up) / pos.entry_ask_up;
                    if up_profit_pct >= config.early_exit_profit {
                        // Sell UP, keep DOWN (will be worth $0 or $1 at expiration)
                        // But we're exiting early, so we calculate based on current bids
                        let exit_value = snapshot.bid_up + snapshot.bid_down;
                        let pnl = (exit_value - pos.entry_cost) * bet_amount;
                        let pnl_pct = (exit_value - pos.entry_cost) / pos.entry_cost * 100.0;

                        return Some(Trade {
                            entry_cost: pos.entry_cost,
                            entry_time: pos.entry_time,
                            exit_value,
                            exit_time: snapshot.time_elapsed,
                            exit_reason: "EARLY_EXIT_UP".to_string(),
                            pnl,
                            pnl_pct,
                        });
                    }
                }

                // Check if we can sell DOWN side for profit
                if snapshot.bid_down > 0.0 {
                    let down_profit_pct = (snapshot.bid_down - pos.entry_ask_down) / pos.entry_ask_down;
                    if down_profit_pct >= config.early_exit_profit {
                        let exit_value = snapshot.bid_up + snapshot.bid_down;
                        let pnl = (exit_value - pos.entry_cost) * bet_amount;
                        let pnl_pct = (exit_value - pos.entry_cost) / pos.entry_cost * 100.0;

                        return Some(Trade {
                            entry_cost: pos.entry_cost,
                            entry_time: pos.entry_time,
                            exit_value,
                            exit_time: snapshot.time_elapsed,
                            exit_reason: "EARLY_EXIT_DOWN".to_string(),
                            pnl,
                            pnl_pct,
                        });
                    }
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Check ENTRY conditions if no position
        // ═══════════════════════════════════════════════════════════════
        if position.is_none() {
            // Check time window
            if snapshot.time_elapsed < config.min_entry_time {
                continue;
            }
            if snapshot.time_elapsed > config.max_entry_time {
                continue;
            }

            // Check ask sum within bounds
            let ask_sum = snapshot.ask_sum();
            if ask_sum < config.min_ask_sum || ask_sum > config.max_ask_sum {
                continue;
            }

            // Check liquidity on both sides
            if snapshot.min_liquidity() < config.min_liquidity {
                continue;
            }

            // ENTER: Buy both UP and DOWN
            position = Some(ArbitragePosition {
                entry_ask_up: snapshot.ask_up,
                entry_ask_down: snapshot.ask_down,
                entry_time: snapshot.time_elapsed,
                entry_cost: ask_sum,
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // STEP 3: If still holding at expiration, settle
    // ═══════════════════════════════════════════════════════════════
    if let Some(pos) = position {
        // One side pays $1, the other $0
        let exit_value = 1.0;
        let pnl = (exit_value - pos.entry_cost) * bet_amount;
        let pnl_pct = (exit_value - pos.entry_cost) / pos.entry_cost * 100.0;

        return Some(Trade {
            entry_cost: pos.entry_cost,
            entry_time: pos.entry_time,
            exit_value,
            exit_time: 900,
            exit_reason: "EXPIRATION".to_string(),
            pnl,
            pnl_pct,
        });
    }

    None
}

fn run_scenario(markets: &[Market], config: &ScenarioConfig, bet_amount: f64) -> (ScenarioResult, Vec<MarketResult>) {
    let mut result = ScenarioResult {
        name: config.name.clone(),
        ..Default::default()
    };
    let mut market_results = Vec::new();
    let mut total_entry_cost = 0.0;
    let mut total_hold_time = 0;

    for market in markets {
        if let Some(trade) = simulate_market(market, config, bet_amount) {
            result.total_trades += 1;
            result.total_pnl += trade.pnl;
            result.total_invested += trade.entry_cost * bet_amount;
            total_entry_cost += trade.entry_cost;
            total_hold_time += trade.exit_time - trade.entry_time;

            match trade.exit_reason.as_str() {
                "EARLY_EXIT_UP" => result.early_exits_up += 1,
                "EARLY_EXIT_DOWN" => result.early_exits_down += 1,
                "EXPIRATION" => result.expirations += 1,
                _ => {}
            }

            market_results.push(MarketResult {
                market_slug: market.slug.clone(),
                outcome: market.outcome,
                entry_cost: trade.entry_cost,
                exit_value: trade.exit_value,
                pnl: trade.pnl,
                exit_reason: trade.exit_reason,
                hold_time: trade.exit_time - trade.entry_time,
            });

            result.markets_traded += 1;
        }
    }

    if result.total_trades > 0 {
        result.avg_entry_cost = total_entry_cost / result.total_trades as f64;
        result.avg_hold_time = total_hold_time as f64 / result.total_trades as f64;
    }

    (result, market_results)
}

// ============================================================================
// Output
// ============================================================================

fn print_scenario_comparison(results: &[ScenarioResult], bet_amount: f64) {
    println!("\n{}", "=".repeat(160));
    println!("{:^160}", format!("ARBITRAGE STRATEGY COMPARISON (${:.0} per side, ${:.0} total per position)", bet_amount, bet_amount * 2.0));
    println!("{}", "=".repeat(160));
    println!();

    println!(
        "{:<35} {:>7} {:>10} {:>9} | {:>10} {:>9} | {:>8} {:>8} {:>8} | {:>8}",
        "Scenario", "Trades", "Invested", "P&L",
        "Return%", "$/Trade",
        "EarlyUp", "EarlyDn", "Expiry",
        "EarlyEx%"
    );
    println!("{}", "-".repeat(160));

    for r in results {
        println!(
            "{:<35} {:>7} {:>9.0}$ {:>8.2}$ | {:>9.2}% {:>8.4}$ | {:>8} {:>8} {:>8} | {:>7.1}%",
            r.name,
            r.total_trades,
            r.total_invested,
            r.total_pnl,
            r.return_pct(),
            r.pnl_per_trade(),
            r.early_exits_up,
            r.early_exits_down,
            r.expirations,
            r.early_exit_rate(),
        );
    }
    println!("{}", "-".repeat(160));
}

fn print_market_breakdown(market_results: &[MarketResult]) {
    println!("\n{}", "=".repeat(110));
    println!("{:^110}", "PER-MARKET BREAKDOWN");
    println!("{}", "=".repeat(110));
    println!();

    println!(
        "{:<40} {:>8} {:>10} {:>10} {:>10} {:>15} {:>8}",
        "Market", "Outcome", "EntryCost", "ExitValue", "P&L", "ExitReason", "HoldTime"
    );
    println!("{}", "-".repeat(110));

    let mut sorted = market_results.to_vec();
    sorted.sort_by(|a, b| b.pnl.partial_cmp(&a.pnl).unwrap());

    for mr in &sorted {
        let display_slug = mr.market_slug.split("btc-").last().unwrap_or(&mr.market_slug);

        println!(
            "{:<40} {:>8} {:>9.4}$ {:>9.4}$ {:>9.4}$ {:>15} {:>7}s",
            display_slug, mr.outcome, mr.entry_cost, mr.exit_value, mr.pnl, mr.exit_reason, mr.hold_time
        );
    }

    let total_pnl: f64 = sorted.iter().map(|m| m.pnl).sum();
    println!("{}", "-".repeat(110));
    println!("{:<40} {:>8} {:>10} {:>10} {:>9.2}$", "TOTAL", "", "", "", total_pnl);
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("arbitrage_backtester=info".parse()?))
        .init();

    let args = Args::parse();

    println!();
    println!("{}", "=".repeat(80));
    println!("{:^80}", "BTC 15-MINUTE ARBITRAGE BACKTESTER");
    println!("{:^80}", "(Both-Sides Strategy Analysis)");
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
    let max_sums: Vec<f64> = args.compare_max_sums
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![args.max_ask_sum]);

    let exit_profits: Vec<f64> = args.compare_exit_profits
        .as_ref()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![args.early_exit_profit]);

    // Build scenarios
    let mut scenarios: Vec<ScenarioConfig> = Vec::new();

    // Add a NO early exit baseline
    for &max_sum in &max_sums {
        scenarios.push(ScenarioConfig {
            name: format!("MaxSum{:.0}%_NoEarlyExit", (max_sum - 1.0) * 100.0),
            max_ask_sum: max_sum,
            min_ask_sum: args.min_ask_sum,
            min_liquidity: args.min_liquidity,
            min_entry_time: args.min_entry_time,
            max_entry_time: args.max_entry_time,
            early_exit: false,
            early_exit_profit: 0.0,
        });
    }

    // Add early exit scenarios
    for &max_sum in &max_sums {
        for &exit_profit in &exit_profits {
            let name = format!(
                "MaxSum{:.0}%_Exit{:.0}%",
                (max_sum - 1.0) * 100.0,
                exit_profit * 100.0
            );

            scenarios.push(ScenarioConfig {
                name,
                max_ask_sum: max_sum,
                min_ask_sum: args.min_ask_sum,
                min_liquidity: args.min_liquidity,
                min_entry_time: args.min_entry_time,
                max_entry_time: args.max_entry_time,
                early_exit: true,
                early_exit_profit: exit_profit,
            });
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
    println!("  Time window:       Last {} hours ({} markets)", args.hours, markets.len());
    println!("  Min liquidity:     ${:.0} on both sides", args.min_liquidity);
    println!("  Entry time:        {}s - {}s", args.min_entry_time, args.max_entry_time);
    println!("  Bet per side:      ${:.2}", args.bet_amount);

    print_scenario_comparison(&results, args.bet_amount);

    // Find best scenarios
    if let Some(best) = results.iter().max_by(|a, b| a.total_pnl.partial_cmp(&b.total_pnl).unwrap()) {
        println!("\nBEST TOTAL P&L: {} (${:.2})", best.name, best.total_pnl);
    }
    if let Some(best) = results.iter().filter(|r| r.total_trades > 0).max_by(|a, b| a.return_pct().partial_cmp(&b.return_pct()).unwrap()) {
        println!("BEST RETURN %:  {} ({:.2}%)", best.name, best.return_pct());
    }

    if args.per_market {
        if let Some(ref mr) = best_market_results {
            print_market_breakdown(mr);
        }
    }

    // Key insight
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "KEY INSIGHT");
    println!("{}", "=".repeat(80));
    println!();
    println!("  This strategy buys BOTH UP and DOWN, guaranteeing $1 return at expiration.");
    println!("  Profit = $1.00 - entry_cost (typically $1.01-$1.02)");
    println!("  Early exit can increase returns if one side's price rises significantly.");
    println!();

    println!("{}", "=".repeat(80));
    println!("{:^80}", "BACKTEST COMPLETE");
    println!("{}", "=".repeat(80));
    println!();

    Ok(())
}
