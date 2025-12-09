//! Cheap Buy Scenario - Buy at $0.10, sell at $0.70
//!
//! Test: Buy when price hits $0.10 in first 3 minutes, sell if hits $0.70

use anyhow::Result;
use chrono::Utc;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_postgres::Client;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "cheap-buy-scenario")]
struct Args {
    #[arg(short = 'H', long, default_value = "48")]
    hours: u32,

    /// Entry price threshold (buy when price <= this)
    #[arg(long, default_value = "0.10")]
    entry_price: f64,

    /// Exit price target (sell when bid >= this)
    #[arg(long, default_value = "0.70")]
    exit_price: f64,

    /// Max entry time in seconds
    #[arg(long, default_value = "180")]
    max_entry_time: i32,

    /// Bet amount per position
    #[arg(short = 'b', long, default_value = "10.0")]
    bet_amount: f64,
}

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
    let connector = TlsConnector::builder().danger_accept_invalid_certs(true).build()?;
    let connector = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(&connection_string, connector).await?;
    tokio::spawn(async move { let _ = connection.await; });
    Ok(client)
}

#[derive(Debug, Clone)]
struct Snapshot {
    time_elapsed: i32,
    price_up: f64,
    price_down: f64,
    bid_up: f64,
    bid_down: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Outcome { UpWins, DownWins }

#[derive(Debug, Clone, Copy, PartialEq)]
enum Dir { Up, Down }

impl std::fmt::Display for Dir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Dir::Up => write!(f, "UP"), Dir::Down => write!(f, "DOWN") }
    }
}

struct Market {
    slug: String,
    outcome: Outcome,
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Clone)]
struct Trade {
    market: String,
    direction: Dir,
    entry_price: f64,
    entry_time: i32,
    exit_price: f64,
    exit_time: i32,
    exit_reason: String,
    pnl: f64,
    pnl_pct: f64,
}

fn simulate_market(
    market: &Market,
    entry_threshold: f64,
    exit_target: f64,
    max_entry_time: i32,
    bet_amount: f64,
) -> Vec<Trade> {
    let mut trades = Vec::new();
    let mut positions: Vec<(Dir, f64, i32)> = Vec::new(); // (dir, entry_price, entry_time)
    let mut has_up = false;
    let mut has_down = false;

    for snap in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════
        // CHECK EXIT: Sell if bid >= exit_target
        // ═══════════════════════════════════════════════════════════════
        let mut to_remove = Vec::new();
        for (i, &(dir, entry_price, entry_time)) in positions.iter().enumerate() {
            let bid = match dir { Dir::Up => snap.bid_up, Dir::Down => snap.bid_down };

            if bid >= exit_target {
                let pnl_pct = (bid - entry_price) / entry_price * 100.0;
                let pnl = bet_amount * (bid - entry_price) / entry_price;
                trades.push(Trade {
                    market: market.slug.clone(),
                    direction: dir,
                    entry_price,
                    entry_time,
                    exit_price: bid,
                    exit_time: snap.time_elapsed,
                    exit_reason: format!("SELL@{:.0}¢", bid * 100.0),
                    pnl,
                    pnl_pct,
                });
                to_remove.push(i);
                match dir { Dir::Up => has_up = false, Dir::Down => has_down = false }
            }
        }
        // Remove sold positions (reverse order to maintain indices)
        for i in to_remove.into_iter().rev() {
            positions.remove(i);
        }

        // ═══════════════════════════════════════════════════════════════
        // CHECK ENTRY: Buy if price <= entry_threshold in first N seconds
        // ═══════════════════════════════════════════════════════════════
        if snap.time_elapsed <= max_entry_time {
            // Check DOWN
            if !has_down && snap.price_down <= entry_threshold && snap.price_down > 0.0 {
                positions.push((Dir::Down, snap.price_down, snap.time_elapsed));
                has_down = true;
            }
            // Check UP
            if !has_up && snap.price_up <= entry_threshold && snap.price_up > 0.0 {
                positions.push((Dir::Up, snap.price_up, snap.time_elapsed));
                has_up = true;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // EXPIRATION: Settle remaining positions
    // ═══════════════════════════════════════════════════════════════
    for (dir, entry_price, entry_time) in positions {
        let won = match (dir, market.outcome) {
            (Dir::Up, Outcome::UpWins) | (Dir::Down, Outcome::DownWins) => true,
            _ => false,
        };
        let exit_price = if won { 1.0 } else { 0.0 };
        let pnl_pct = if won { (1.0 - entry_price) / entry_price * 100.0 } else { -100.0 };
        let pnl = if won { bet_amount * (1.0 - entry_price) / entry_price } else { -bet_amount };

        trades.push(Trade {
            market: market.slug.clone(),
            direction: dir,
            entry_price,
            entry_time,
            exit_price,
            exit_time: 900,
            exit_reason: if won { "EXP_WIN".to_string() } else { "EXP_LOSS".to_string() },
            pnl,
            pnl_pct,
        });
    }

    trades
}

async fn load_markets(client: &Client, hours: u32) -> Result<Vec<Market>> {
    let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);

    let outcomes_query = r#"
        SELECT market_slug,
               (ARRAY_AGG(price_up ORDER BY timestamp DESC))[1] as final_up,
               (ARRAY_AGG(price_delta ORDER BY timestamp DESC))[1] as final_delta
        FROM market_logs WHERE timestamp > $1
        GROUP BY market_slug HAVING MAX(time_elapsed) >= 800
    "#;
    let rows = client.query(outcomes_query, &[&cutoff]).await?;

    let mut outcomes: HashMap<String, Outcome> = HashMap::new();
    for row in rows {
        let slug: String = row.get("market_slug");
        let final_up: f64 = row.get::<_, Decimal>("final_up").to_f64().unwrap_or(0.5);
        let final_delta: f64 = row.get::<_, Decimal>("final_delta").to_f64().unwrap_or(0.0);
        let outcome = if final_up >= 0.95 || final_delta > 20.0 { Outcome::UpWins }
                      else if final_up <= 0.05 || final_delta < -20.0 { Outcome::DownWins }
                      else if final_delta > 0.0 { Outcome::UpWins }
                      else { Outcome::DownWins };
        outcomes.insert(slug, outcome);
    }

    let snap_query = r#"
        SELECT market_slug, time_elapsed, price_up, price_down, bid_up, bid_down
        FROM market_logs WHERE timestamp > $1 AND time_elapsed <= 885
        ORDER BY market_slug, time_elapsed
    "#;
    let rows = client.query(snap_query, &[&cutoff]).await?;

    let mut markets_map: HashMap<String, Vec<Snapshot>> = HashMap::new();
    for row in rows {
        let slug: String = row.get("market_slug");
        if !outcomes.contains_key(&slug) { continue; }
        let snap = Snapshot {
            time_elapsed: row.get("time_elapsed"),
            price_up: row.get::<_, Decimal>("price_up").to_f64().unwrap_or(0.5),
            price_down: row.get::<_, Decimal>("price_down").to_f64().unwrap_or(0.5),
            bid_up: row.get::<_, Option<Decimal>>("bid_up").and_then(|d| d.to_f64()).unwrap_or(0.0),
            bid_down: row.get::<_, Option<Decimal>>("bid_down").and_then(|d| d.to_f64()).unwrap_or(0.0),
        };
        markets_map.entry(slug).or_default().push(snap);
    }

    Ok(markets_map.into_iter()
        .filter_map(|(slug, snaps)| outcomes.get(&slug).map(|&o| Market { slug, outcome: o, snapshots: snaps }))
        .collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!();
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!("                    CHEAP BUY SCENARIO BACKTEST");
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!();
    println!("📋 SCENARIO:");
    println!("   • Buy when price ≤ ${:.2} ({}¢)", args.entry_price, (args.entry_price * 100.0) as i32);
    println!("   • Only in first {} seconds ({:.1} minutes)", args.max_entry_time, args.max_entry_time as f64 / 60.0);
    println!("   • Sell if bid ≥ ${:.2} ({}¢)", args.exit_price, (args.exit_price * 100.0) as i32);
    println!("   • Bet amount: ${:.0} per position", args.bet_amount);
    println!("   • Time window: Last {} hours", args.hours);
    println!();

    let client = connect_db(&DbConfig::default()).await?;
    let markets = load_markets(&client, args.hours).await?;
    println!("📊 Loaded {} resolved markets\n", markets.len());

    let mut all_trades = Vec::new();
    for market in &markets {
        let trades = simulate_market(
            market,
            args.entry_price,
            args.exit_price,
            args.max_entry_time,
            args.bet_amount,
        );
        all_trades.extend(trades);
    }

    if all_trades.is_empty() {
        println!("❌ No trades found with these criteria!");
        println!("   Try: --entry-price 0.15 or --max-entry-time 300");
        return Ok(());
    }

    // Summary stats
    let total_trades = all_trades.len();
    let wins = all_trades.iter().filter(|t| t.pnl > 0.0).count();
    let losses = all_trades.iter().filter(|t| t.pnl < 0.0).count();
    let total_pnl: f64 = all_trades.iter().map(|t| t.pnl).sum();
    let total_invested = total_trades as f64 * args.bet_amount;

    let sell_exits: Vec<_> = all_trades.iter().filter(|t| t.exit_reason.starts_with("SELL")).collect();
    let exp_wins: Vec<_> = all_trades.iter().filter(|t| t.exit_reason == "EXP_WIN").collect();
    let exp_losses: Vec<_> = all_trades.iter().filter(|t| t.exit_reason == "EXP_LOSS").collect();

    println!("┌─────────────────────────────────────────────────────────────────────────────┐");
    println!("│                              RESULTS SUMMARY                                │");
    println!("├─────────────────────────────────────────────────────────────────────────────┤");
    println!("│  Total Trades:     {:>5}                                                   │", total_trades);
    println!("│  Wins:             {:>5}  ({:.1}%)                                          │", wins, wins as f64 / total_trades as f64 * 100.0);
    println!("│  Losses:           {:>5}  ({:.1}%)                                          │", losses, losses as f64 / total_trades as f64 * 100.0);
    println!("├─────────────────────────────────────────────────────────────────────────────┤");
    println!("│  Total Invested:   ${:>8.2}                                               │", total_invested);
    println!("│  Total P&L:        ${:>8.2}  {}                                         │",
        total_pnl, if total_pnl >= 0.0 { "✅" } else { "❌" });
    println!("│  Return:           {:>8.1}%                                                │", total_pnl / total_invested * 100.0);
    println!("│  P&L per Trade:    ${:>8.2}                                               │", total_pnl / total_trades as f64);
    println!("├─────────────────────────────────────────────────────────────────────────────┤");
    println!("│  Exit Breakdown:                                                            │");
    println!("│    Sold at target: {:>5}  (P&L: ${:>8.2})                                  │",
        sell_exits.len(), sell_exits.iter().map(|t| t.pnl).sum::<f64>());
    println!("│    Expiration WIN: {:>5}  (P&L: ${:>8.2})                                  │",
        exp_wins.len(), exp_wins.iter().map(|t| t.pnl).sum::<f64>());
    println!("│    Expiration LOSS:{:>5}  (P&L: ${:>8.2})                                  │",
        exp_losses.len(), exp_losses.iter().map(|t| t.pnl).sum::<f64>());
    println!("└─────────────────────────────────────────────────────────────────────────────┘");

    // Show individual trades
    println!("\n📝 ALL TRADES:");
    println!("┌──────────────────────────────┬──────┬────────┬────────┬────────┬───────────┬──────────┐");
    println!("│ Market                       │ Side │ Entry  │ Exit   │ Time   │ Exit Type │    P&L   │");
    println!("├──────────────────────────────┼──────┼────────┼────────┼────────┼───────────┼──────────┤");

    for t in &all_trades {
        let short_market = t.market.replace("btc-updown-15m-", "");
        let emoji = if t.pnl > 0.0 { "✅" } else { "❌" };
        println!(
            "│ {:>28} │ {:>4} │ {:>5.0}¢ │ {:>5.0}¢ │ {:>4}s  │ {:>9} │ {} {:>6.2}$ │",
            short_market,
            t.direction,
            t.entry_price * 100.0,
            t.exit_price * 100.0,
            t.entry_time,
            t.exit_reason,
            emoji,
            t.pnl
        );
    }
    println!("└──────────────────────────────┴──────┴────────┴────────┴────────┴───────────┴──────────┘");

    // Analysis
    println!("\n💡 ANALYSIS:");

    if exp_losses.len() > 0 {
        let avg_loss_entry: f64 = exp_losses.iter().map(|t| t.entry_price).sum::<f64>() / exp_losses.len() as f64;
        println!("   • Average entry price of LOSSES: {:.0}¢", avg_loss_entry * 100.0);
    }

    if sell_exits.len() > 0 {
        let avg_hold_time: f64 = sell_exits.iter().map(|t| (t.exit_time - t.entry_time) as f64).sum::<f64>() / sell_exits.len() as f64;
        println!("   • Average hold time for SELLS: {:.0}s ({:.1} min)", avg_hold_time, avg_hold_time / 60.0);
    }

    let up_trades: Vec<_> = all_trades.iter().filter(|t| matches!(t.direction, Dir::Up)).collect();
    let down_trades: Vec<_> = all_trades.iter().filter(|t| matches!(t.direction, Dir::Down)).collect();

    println!("   • UP trades:   {} (P&L: ${:.2})", up_trades.len(), up_trades.iter().map(|t| t.pnl).sum::<f64>());
    println!("   • DOWN trades: {} (P&L: ${:.2})", down_trades.len(), down_trades.iter().map(|t| t.pnl).sum::<f64>());

    println!();
    Ok(())
}
