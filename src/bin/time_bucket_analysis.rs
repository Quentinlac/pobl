//! Time Bucket Analysis - Simulates bot behavior exactly
//!
//! Uses the EXACT same parameters as bot_config.yaml to predict P&L
//! This allows comparing theoretical vs actual trading results

use anyhow::Result;
use chrono::Utc;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_postgres::Client;

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

// ═══════════════════════════════════════════════════════════════════════════════
// BOT CONFIG - MUST MATCH bot_config.yaml EXACTLY
// ═══════════════════════════════════════════════════════════════════════════════
struct BotConfig {
    // Terminal strategy
    min_buy_edge: f64,           // terminal_strategy.min_edge
    min_sell_edge: f64,          // terminal_strategy.min_sell_edge
    min_profit_before_sell: f64, // terminal_strategy.min_profit_before_sell
    max_bet_usdc: f64,           // terminal_strategy.max_bet_usdc
    min_seconds_remaining: i32,  // terminal_strategy.min_seconds_remaining

    // Markets
    min_liquidity_usdc: f64,     // markets.min_liquidity_usdc
    max_spread_pct: f64,         // markets.max_spread_pct

    // Risk
    max_open_positions: u32,     // risk.max_open_positions (per direction)
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            // From terminal_strategy section
            min_buy_edge: 0.07,           // 7%
            min_sell_edge: 0.10,          // 10%
            min_profit_before_sell: 0.00, // 0%
            max_bet_usdc: 1.5,            // $1.50
            min_seconds_remaining: 15,    // 15 seconds

            // From markets section
            min_liquidity_usdc: 200.0,    // $200
            max_spread_pct: 0.30,         // 30%

            // From risk section
            max_open_positions: 20,       // 20 per direction
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
    edge_up: f64,
    edge_down: f64,
    edge_up_sell: f64,
    edge_down_sell: f64,
    size_up: f64,
    size_down: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Outcome { UpWins, DownWins }

#[derive(Debug, Clone, Copy, PartialEq)]
enum Dir { Up, Down }

struct Market {
    slug: String,
    outcome: Outcome,
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Clone)]
struct Position {
    direction: Dir,
    entry_price: f64,
    entry_time: i32,
    shares: f64,
    cost: f64,
}

#[derive(Debug, Default, Clone)]
struct BucketStats {
    trades: u32,
    wins: u32,
    losses: u32,
    pnl: f64,
    sell_exits: u32,
    exp_wins: u32,
    exp_losses: u32,
}

impl BucketStats {
    fn win_rate(&self) -> f64 {
        if self.trades == 0 { 0.0 } else { self.wins as f64 / self.trades as f64 * 100.0 }
    }
}

fn get_bucket(time_elapsed: i32) -> usize {
    // 0-150s = bucket 0 (0-2.5min)
    // 150-300s = bucket 1 (2.5-5min)
    // etc.
    (time_elapsed / 150).min(5) as usize
}

fn bucket_name(bucket: usize) -> &'static str {
    match bucket {
        0 => "0:00 - 2:30",
        1 => "2:30 - 5:00",
        2 => "5:00 - 7:30",
        3 => "7:30 - 10:00",
        4 => "10:00 - 12:30",
        5 => "12:30 - 15:00",
        _ => "Unknown",
    }
}

fn simulate_market(market: &Market, config: &BotConfig) -> Vec<(usize, f64, String)> {
    // Returns: Vec of (entry_bucket, pnl, exit_reason)
    let mut results = Vec::new();

    // Track positions PER DIRECTION (matching bot's per-direction limits)
    let mut up_positions: Vec<Position> = Vec::new();
    let mut down_positions: Vec<Position> = Vec::new();

    // Time cutoff: 900 - min_seconds_remaining
    let max_buy_time = 900 - config.min_seconds_remaining;

    for snap in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════════
        // CHECK SELLS FIRST (for existing positions)
        // ═══════════════════════════════════════════════════════════════════

        // Sell UP positions
        let mut sold_up_indices = Vec::new();
        for (idx, pos) in up_positions.iter().enumerate() {
            if snap.bid_up > 0.0 && snap.edge_up_sell >= config.min_sell_edge {
                let profit_pct = (snap.bid_up - pos.entry_price) / pos.entry_price;
                if profit_pct >= config.min_profit_before_sell {
                    let pnl = pos.cost * profit_pct;
                    let bucket = get_bucket(pos.entry_time);
                    results.push((bucket, pnl, "SELL".to_string()));
                    sold_up_indices.push(idx);
                }
            }
        }
        // Remove sold positions (in reverse order to maintain indices)
        for idx in sold_up_indices.into_iter().rev() {
            up_positions.remove(idx);
        }

        // Sell DOWN positions
        let mut sold_down_indices = Vec::new();
        for (idx, pos) in down_positions.iter().enumerate() {
            if snap.bid_down > 0.0 && snap.edge_down_sell >= config.min_sell_edge {
                let profit_pct = (snap.bid_down - pos.entry_price) / pos.entry_price;
                if profit_pct >= config.min_profit_before_sell {
                    let pnl = pos.cost * profit_pct;
                    let bucket = get_bucket(pos.entry_time);
                    results.push((bucket, pnl, "SELL".to_string()));
                    sold_down_indices.push(idx);
                }
            }
        }
        for idx in sold_down_indices.into_iter().rev() {
            down_positions.remove(idx);
        }

        // ═══════════════════════════════════════════════════════════════════
        // CHECK BUYS (if within time window)
        // ═══════════════════════════════════════════════════════════════════

        if snap.time_elapsed <= max_buy_time {
            // Check DOWN (if under position limit)
            if (down_positions.len() as u32) < config.max_open_positions {
                let spread = if snap.bid_down > 0.0 {
                    (snap.price_down - snap.bid_down) / snap.price_down
                } else {
                    1.0
                };

                // size_down is in SHARES, convert to USDC: shares * price
                let liquidity_down_usdc = snap.size_down * snap.price_down;
                if snap.edge_down >= config.min_buy_edge
                    && liquidity_down_usdc >= config.min_liquidity_usdc
                    && spread <= config.max_spread_pct
                {
                    let shares = config.max_bet_usdc / snap.price_down;
                    down_positions.push(Position {
                        direction: Dir::Down,
                        entry_price: snap.price_down,
                        entry_time: snap.time_elapsed,
                        shares,
                        cost: config.max_bet_usdc,
                    });
                }
            }

            // Check UP (if under position limit)
            if (up_positions.len() as u32) < config.max_open_positions {
                let spread = if snap.bid_up > 0.0 {
                    (snap.price_up - snap.bid_up) / snap.price_up
                } else {
                    1.0
                };

                // size_up is in SHARES, convert to USDC: shares * price
                let liquidity_up_usdc = snap.size_up * snap.price_up;
                if snap.edge_up >= config.min_buy_edge
                    && liquidity_up_usdc >= config.min_liquidity_usdc
                    && spread <= config.max_spread_pct
                {
                    let shares = config.max_bet_usdc / snap.price_up;
                    up_positions.push(Position {
                        direction: Dir::Up,
                        entry_price: snap.price_up,
                        entry_time: snap.time_elapsed,
                        shares,
                        cost: config.max_bet_usdc,
                    });
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SETTLE REMAINING POSITIONS AT EXPIRATION
    // ═══════════════════════════════════════════════════════════════════════

    for pos in up_positions.iter().chain(down_positions.iter()) {
        let won = match (pos.direction, market.outcome) {
            (Dir::Up, Outcome::UpWins) | (Dir::Down, Outcome::DownWins) => true,
            _ => false,
        };
        let pnl = if won {
            pos.cost * (1.0 - pos.entry_price) / pos.entry_price
        } else {
            -pos.cost
        };
        let bucket = get_bucket(pos.entry_time);
        let reason = if won { "EXP_WIN" } else { "EXP_LOSS" };
        results.push((bucket, pnl, reason.to_string()));
    }

    results
}

async fn load_markets(client: &Client, hours: u32) -> Result<Vec<Market>> {
    let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);

    // Get outcomes
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

    // Load snapshots (up to 885 seconds = 900 - 15)
    let snap_query = r#"
        SELECT market_slug, time_elapsed, price_up, price_down, bid_up, bid_down,
               edge_up, edge_down, edge_up_sell, edge_down_sell, size_up, size_down
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
            edge_up: row.get::<_, Option<Decimal>>("edge_up").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_down: row.get::<_, Option<Decimal>>("edge_down").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_up_sell: row.get::<_, Option<Decimal>>("edge_up_sell").and_then(|d| d.to_f64()).unwrap_or(0.0),
            edge_down_sell: row.get::<_, Option<Decimal>>("edge_down_sell").and_then(|d| d.to_f64()).unwrap_or(0.0),
            size_up: row.get::<_, Decimal>("size_up").to_f64().unwrap_or(0.0),
            size_down: row.get::<_, Decimal>("size_down").to_f64().unwrap_or(0.0),
        };
        markets_map.entry(slug).or_default().push(snap);
    }

    Ok(markets_map.into_iter()
        .filter_map(|(slug, snaps)| outcomes.get(&slug).map(|&o| Market { slug, outcome: o, snapshots: snaps }))
        .collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = BotConfig::default();

    println!();
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!("          TIME BUCKET ANALYSIS - Matching bot_config.yaml EXACTLY");
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!();
    println!("📋 CONFIG (from bot_config.yaml):");
    println!("   min_buy_edge:         {:.0}%", config.min_buy_edge * 100.0);
    println!("   min_sell_edge:        {:.0}%", config.min_sell_edge * 100.0);
    println!("   min_profit_before_sell: {:.0}%", config.min_profit_before_sell * 100.0);
    println!("   max_bet_usdc:         ${:.2}", config.max_bet_usdc);
    println!("   min_seconds_remaining: {}s", config.min_seconds_remaining);
    println!("   min_liquidity_usdc:   ${:.0}", config.min_liquidity_usdc);
    println!("   max_spread_pct:       {:.0}%", config.max_spread_pct * 100.0);
    println!("   max_open_positions:   {} per direction", config.max_open_positions);
    println!();

    let client = connect_db(&DbConfig::default()).await?;
    let markets = load_markets(&client, 24).await?;

    println!("📊 Loaded {} resolved markets from last 24 hours\n", markets.len());

    let mut buckets: [BucketStats; 6] = Default::default();
    let mut total_positions = 0u32;

    for market in &markets {
        let trades = simulate_market(market, &config);
        total_positions += trades.len() as u32;
        for (bucket, pnl, reason) in trades {
            buckets[bucket].trades += 1;
            buckets[bucket].pnl += pnl;
            if pnl > 0.0 {
                buckets[bucket].wins += 1;
            } else {
                buckets[bucket].losses += 1;
            }
            match reason.as_str() {
                "SELL" => buckets[bucket].sell_exits += 1,
                "EXP_WIN" => buckets[bucket].exp_wins += 1,
                "EXP_LOSS" => buckets[bucket].exp_losses += 1,
                _ => {}
            }
        }
    }

    println!("┌────────────────┬────────┬────────┬──────────┬──────────┬────────┬────────┬─────────┐");
    println!("│  Entry Time    │ Trades │  Win%  │   P&L    │  $/Trade │  Sell  │ ExpWin │ ExpLoss │");
    println!("├────────────────┼────────┼────────┼──────────┼──────────┼────────┼────────┼─────────┤");

    let mut total = BucketStats::default();

    for (i, b) in buckets.iter().enumerate() {
        let pnl_per_trade = if b.trades > 0 { b.pnl / b.trades as f64 } else { 0.0 };
        let emoji = if b.pnl > 0.0 { "✅" } else if b.pnl < 0.0 { "❌" } else { "➖" };

        println!(
            "│ {} {:>12} │ {:>6} │ {:>5.1}% │ {:>7.2}$ │ {:>7.3}$ │ {:>6} │ {:>6} │ {:>7} │",
            emoji,
            bucket_name(i),
            b.trades,
            b.win_rate(),
            b.pnl,
            pnl_per_trade,
            b.sell_exits,
            b.exp_wins,
            b.exp_losses
        );

        total.trades += b.trades;
        total.wins += b.wins;
        total.losses += b.losses;
        total.pnl += b.pnl;
        total.sell_exits += b.sell_exits;
        total.exp_wins += b.exp_wins;
        total.exp_losses += b.exp_losses;
    }

    println!("├────────────────┼────────┼────────┼──────────┼──────────┼────────┼────────┼─────────┤");
    let total_per_trade = if total.trades > 0 { total.pnl / total.trades as f64 } else { 0.0 };
    println!(
        "│ 🏆 TOTAL       │ {:>6} │ {:>5.1}% │ {:>7.2}$ │ {:>7.3}$ │ {:>6} │ {:>6} │ {:>7} │",
        total.trades,
        total.win_rate(),
        total.pnl,
        total_per_trade,
        total.sell_exits,
        total.exp_wins,
        total.exp_losses
    );
    println!("└────────────────┴────────┴────────┴──────────┴──────────┴────────┴────────┴─────────┘");

    println!("\n📈 SUMMARY:");
    println!("   Markets analyzed:  {}", markets.len());
    println!("   Total positions:   {}", total_positions);
    println!("   Avg per market:    {:.1}", total_positions as f64 / markets.len() as f64);
    println!("   Total P&L:         ${:.2}", total.pnl);
    println!("   Win rate:          {:.1}%", total.win_rate());

    // Analysis
    let best_bucket = buckets.iter().enumerate()
        .filter(|(_, b)| b.trades > 0)
        .max_by(|(_, a), (_, b)| a.pnl.partial_cmp(&b.pnl).unwrap());

    let worst_bucket = buckets.iter().enumerate()
        .filter(|(_, b)| b.trades > 0)
        .min_by(|(_, a), (_, b)| a.pnl.partial_cmp(&b.pnl).unwrap());

    if let Some((i, b)) = best_bucket {
        println!("\n   🥇 Best time to enter:  {} (P&L: ${:.2}, {:.1}% win rate)",
            bucket_name(i), b.pnl, b.win_rate());
    }

    if let Some((i, b)) = worst_bucket {
        println!("   🥉 Worst time to enter: {} (P&L: ${:.2}, {:.1}% win rate)",
            bucket_name(i), b.pnl, b.win_rate());
    }

    // Recommendations
    println!("\n💡 RECOMMENDATIONS:");
    for (i, b) in buckets.iter().enumerate() {
        if b.trades > 5 && b.pnl < 0.0 {
            println!("   ⚠️  Consider AVOIDING entries during {} (losing ${:.2})", bucket_name(i), -b.pnl);
        }
    }

    println!();
    Ok(())
}
