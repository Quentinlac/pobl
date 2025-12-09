//! Creative Strategies Backtester for BTC 15-Minute Markets
//!
//! Tests 20+ creative trading strategies to find optimal approaches
//!
//! Usage:
//!   btc-creative-strategies --hours 48
//!   btc-creative-strategies --hours 168 --per-market

use anyhow::Result;
use chrono::Utc;
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
#[command(name = "btc-creative-strategies")]
#[command(about = "Backtest 20+ creative trading strategies")]
struct Args {
    /// Number of hours of data to analyze
    #[arg(short = 'H', long, default_value = "48")]
    hours: u32,

    /// Bet amount in USDC
    #[arg(short = 'b', long, default_value = "15.0")]
    bet_amount: f64,

    /// Show per-market breakdown for best strategy
    #[arg(short = 'm', long)]
    per_market: bool,

    /// Minimum sell edge for exit
    #[arg(long, default_value = "0.08")]
    min_sell_edge: f64,
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
    price_delta: f64,
}

impl MarketSnapshot {
    fn spread_up(&self) -> f64 {
        if self.bid_up > 0.0 { self.price_up - self.bid_up } else { 1.0 }
    }
    fn spread_down(&self) -> f64 {
        if self.bid_down > 0.0 { self.price_down - self.bid_down } else { 1.0 }
    }
    fn liquidity_ratio(&self) -> f64 {
        if self.size_down > 0.0 { self.size_up / self.size_down } else { 100.0 }
    }
}

#[derive(Debug, Clone)]
struct Market {
    slug: String,
    outcome: Outcome,
    snapshots: Vec<MarketSnapshot>,
    initial_price_up: f64,
    initial_price_down: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    UpWins,
    DownWins,
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
    exit_price: f64,
    pnl_pct: f64,
    exit_reason: String,
}

// ============================================================================
// Strategy Definitions
// ============================================================================

#[derive(Debug, Clone)]
struct Strategy {
    name: String,
    description: String,
    // Entry conditions
    min_edge: f64,
    max_price: f64,           // Only buy if price < this
    min_price: f64,           // Only buy if price > this
    min_time: i32,
    max_time: i32,
    min_liquidity: f64,
    max_spread: f64,
    // Special conditions
    contrarian: bool,         // Buy opposite of expensive side
    liquidity_contrarian: bool, // Buy opposite of high-liquidity side
    require_balanced: bool,   // Only when prices near 50/50
    follow_delta: bool,       // Follow BTC price movement
    extreme_only: bool,       // Only extreme prices
    // Double-sided
    allow_both_sides: bool,   // Can accumulate both UP and DOWN
    opposite_trigger_drop: f64, // If position drops by this %, buy other side
    // Exit
    min_sell_edge: f64,
    min_profit_to_sell: f64,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: String::new(),
            min_edge: 0.05,
            max_price: 1.0,
            min_price: 0.0,
            min_time: 30,
            max_time: 660,
            min_liquidity: 5.0,
            max_spread: 0.30,
            contrarian: false,
            liquidity_contrarian: false,
            require_balanced: false,
            follow_delta: false,
            extreme_only: false,
            allow_both_sides: false,
            opposite_trigger_drop: 0.0,
            min_sell_edge: 0.08,
            min_profit_to_sell: 0.0,
        }
    }
}

fn get_strategies(min_sell_edge: f64) -> Vec<Strategy> {
    vec![
        // ═══════════════════════════════════════════════════════════════
        // BASELINE
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "01_BASELINE_5%".into(),
            description: "Standard 5% edge strategy (control)".into(),
            min_edge: 0.05,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "02_BASELINE_7%".into(),
            description: "Standard 7% edge strategy".into(),
            min_edge: 0.07,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // CHEAP SIDE STRATEGIES
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "03_CHEAP_HUNTER_25c".into(),
            description: "Buy when price < $0.25 with edge > 3%".into(),
            min_edge: 0.03,
            max_price: 0.25,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "04_UNDERDOG_SNIPER".into(),
            description: "Cheaper side < $0.35 with edge > 5%".into(),
            min_edge: 0.05,
            max_price: 0.35,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "05_EXTREME_CHEAP_15c".into(),
            description: "Ultra cheap < $0.15 with any edge > 2%".into(),
            min_edge: 0.02,
            max_price: 0.15,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // DOUBLE-SIDED STRATEGIES (User's idea!)
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "06_DOUBLE_ACCUM_49c".into(),
            description: "Buy both sides when < $0.49 with edge > 5%".into(),
            min_edge: 0.05,
            max_price: 0.49,
            allow_both_sides: true,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "07_DOUBLE_ACCUM_45c".into(),
            description: "Buy both sides when < $0.45 with edge > 5%".into(),
            min_edge: 0.05,
            max_price: 0.45,
            allow_both_sides: true,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "08_OPPOSITE_TRIGGER".into(),
            description: "Buy one side, if drops 25%+ buy other".into(),
            min_edge: 0.05,
            allow_both_sides: true,
            opposite_trigger_drop: 0.25,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // CONTRARIAN STRATEGIES
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "09_CONTRARIAN_75".into(),
            description: "When one side > $0.75, buy the OTHER side".into(),
            min_edge: 0.0,  // No edge required, pure contrarian
            contrarian: true,
            min_price: 0.75,  // The expensive side threshold
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "10_FADE_EXTREME_90".into(),
            description: "When one side > $0.90, fade it hard".into(),
            min_edge: 0.0,
            contrarian: true,
            min_price: 0.90,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "11_LIQUIDITY_CONTRA".into(),
            description: "Bet opposite of high-liquidity side (5:1)".into(),
            min_edge: 0.03,
            liquidity_contrarian: true,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // TIME-BASED STRATEGIES
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "12_EARLY_BIRD_120s".into(),
            description: "Only first 120s when edges largest".into(),
            min_edge: 0.05,
            max_time: 120,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "13_MID_WINDOW_180_400".into(),
            description: "Sweet spot: 180-400s".into(),
            min_edge: 0.05,
            min_time: 180,
            max_time: 400,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "14_LATE_GAME_500s".into(),
            description: "Late entry > 500s, outcome clearer".into(),
            min_edge: 0.03,
            min_time: 500,
            max_time: 700,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // COMBINED SIGNAL STRATEGIES
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "15_EDGE_STACK_10_40".into(),
            description: "Edge > 10% AND price < $0.40".into(),
            min_edge: 0.10,
            max_price: 0.40,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "16_COMBINED_7_45_30".into(),
            description: "Edge>7% AND price<$0.45 AND liq>30".into(),
            min_edge: 0.07,
            max_price: 0.45,
            min_liquidity: 30.0,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "17_HIGH_CONVICTION_12".into(),
            description: "Only trade edge > 12% (very selective)".into(),
            min_edge: 0.12,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "18_ULTRA_CONVICTION_15".into(),
            description: "Only trade edge > 15%".into(),
            min_edge: 0.15,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // SPECIAL CONDITION STRATEGIES
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "19_BALANCED_ENTRY".into(),
            description: "Only when prices near 50/50 (0.45-0.55)".into(),
            min_edge: 0.05,
            require_balanced: true,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "20_DELTA_FOLLOWER".into(),
            description: "Follow BTC price movement direction".into(),
            min_edge: 0.03,
            follow_delta: true,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "21_TIGHT_SPREAD_3pct".into(),
            description: "Only enter when spread < 3%".into(),
            min_edge: 0.05,
            max_spread: 0.03,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "22_HIGH_LIQ_100".into(),
            description: "Only high liquidity > $100".into(),
            min_edge: 0.05,
            min_liquidity: 100.0,
            min_sell_edge,
            ..Default::default()
        },

        // ═══════════════════════════════════════════════════════════════
        // HYBRID DOUBLE-SIDED + CONDITIONS
        // ═══════════════════════════════════════════════════════════════
        Strategy {
            name: "23_DOUBLE_EARLY_CHEAP".into(),
            description: "Both sides, < $0.45, first 300s".into(),
            min_edge: 0.05,
            max_price: 0.45,
            max_time: 300,
            allow_both_sides: true,
            min_sell_edge,
            ..Default::default()
        },

        Strategy {
            name: "24_DOUBLE_HIGH_EDGE".into(),
            description: "Both sides, edge > 8%, < $0.48".into(),
            min_edge: 0.08,
            max_price: 0.48,
            allow_both_sides: true,
            min_sell_edge,
            ..Default::default()
        },
    ]
}

// ============================================================================
// Data Loading
// ============================================================================

async fn load_markets(client: &Client, hours: u32) -> Result<Vec<Market>> {
    info!("Loading market data from last {} hours...", hours);

    let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);

    // Get resolved markets and outcomes
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
            market_slug, time_elapsed,
            price_up, price_down, bid_up, bid_down,
            edge_up, edge_down, edge_up_sell, edge_down_sell,
            size_up, size_down, price_delta
        FROM market_logs
        WHERE timestamp > $1 AND time_elapsed <= 885
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
            price_delta: row.get::<_, Decimal>("price_delta").to_f64().unwrap_or(0.0),
        };

        markets_map.entry(slug).or_default().push(snapshot);
    }

    let markets: Vec<Market> = markets_map
        .into_iter()
        .filter_map(|(slug, snapshots)| {
            if snapshots.is_empty() {
                return None;
            }
            let initial_up = snapshots.first().map(|s| s.price_up).unwrap_or(0.5);
            let initial_down = snapshots.first().map(|s| s.price_down).unwrap_or(0.5);
            market_outcomes.get(&slug).map(|&outcome| Market {
                slug,
                outcome,
                snapshots,
                initial_price_up: initial_up,
                initial_price_down: initial_down,
            })
        })
        .collect();

    info!("Loaded {} markets", markets.len());
    Ok(markets)
}

// ============================================================================
// Strategy Simulation
// ============================================================================

fn check_entry_conditions(snapshot: &MarketSnapshot, strategy: &Strategy, market: &Market) -> Option<Direction> {
    // Time filter
    if snapshot.time_elapsed < strategy.min_time || snapshot.time_elapsed > strategy.max_time {
        return None;
    }

    // ═══════════════════════════════════════════════════════════════
    // CONTRARIAN: Buy opposite of expensive side
    // ═══════════════════════════════════════════════════════════════
    if strategy.contrarian {
        if snapshot.price_up >= strategy.min_price {
            // UP is expensive, buy DOWN
            if snapshot.size_down >= strategy.min_liquidity && snapshot.spread_down() <= strategy.max_spread {
                return Some(Direction::Down);
            }
        }
        if snapshot.price_down >= strategy.min_price {
            // DOWN is expensive, buy UP
            if snapshot.size_up >= strategy.min_liquidity && snapshot.spread_up() <= strategy.max_spread {
                return Some(Direction::Up);
            }
        }
        return None;
    }

    // ═══════════════════════════════════════════════════════════════
    // LIQUIDITY CONTRARIAN: Bet opposite of high-liquidity side
    // ═══════════════════════════════════════════════════════════════
    if strategy.liquidity_contrarian {
        let ratio = snapshot.liquidity_ratio();
        if ratio > 5.0 {
            // UP has way more liquidity, bet DOWN
            if snapshot.edge_down >= strategy.min_edge && snapshot.size_down >= strategy.min_liquidity {
                return Some(Direction::Down);
            }
        } else if ratio < 0.2 {
            // DOWN has way more liquidity, bet UP
            if snapshot.edge_up >= strategy.min_edge && snapshot.size_up >= strategy.min_liquidity {
                return Some(Direction::Up);
            }
        }
        return None;
    }

    // ═══════════════════════════════════════════════════════════════
    // BALANCED ENTRY: Only when prices near 50/50
    // ═══════════════════════════════════════════════════════════════
    if strategy.require_balanced {
        if snapshot.price_up < 0.45 || snapshot.price_up > 0.55 {
            return None;
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // DELTA FOLLOWER: Follow BTC movement
    // ═══════════════════════════════════════════════════════════════
    if strategy.follow_delta {
        if snapshot.price_delta > 5.0 {
            // BTC going up
            if snapshot.edge_up >= strategy.min_edge && snapshot.price_up <= strategy.max_price {
                return Some(Direction::Up);
            }
        } else if snapshot.price_delta < -5.0 {
            // BTC going down
            if snapshot.edge_down >= strategy.min_edge && snapshot.price_down <= strategy.max_price {
                return Some(Direction::Down);
            }
        }
        return None;
    }

    // ═══════════════════════════════════════════════════════════════
    // STANDARD EDGE-BASED ENTRY (with price filters)
    // ═══════════════════════════════════════════════════════════════

    // Check DOWN
    let down_valid = snapshot.edge_down >= strategy.min_edge
        && snapshot.price_down >= strategy.min_price
        && snapshot.price_down <= strategy.max_price
        && snapshot.size_down >= strategy.min_liquidity
        && snapshot.spread_down() <= strategy.max_spread;

    // Check UP
    let up_valid = snapshot.edge_up >= strategy.min_edge
        && snapshot.price_up >= strategy.min_price
        && snapshot.price_up <= strategy.max_price
        && snapshot.size_up >= strategy.min_liquidity
        && snapshot.spread_up() <= strategy.max_spread;

    // Prefer DOWN (arbitrary tiebreaker)
    if down_valid {
        Some(Direction::Down)
    } else if up_valid {
        Some(Direction::Up)
    } else {
        None
    }
}

fn simulate_market(market: &Market, strategy: &Strategy, bet_amount: f64) -> Vec<Trade> {
    let mut trades: Vec<Trade> = Vec::new();
    let mut positions: Vec<Position> = Vec::new();
    let mut has_up = false;
    let mut has_down = false;

    for snapshot in &market.snapshots {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: Check SELL conditions for existing positions
        // ═══════════════════════════════════════════════════════════════
        let mut sold_directions: Vec<Direction> = Vec::new();

        for pos in &positions {
            let (sell_edge, bid_price) = match pos.direction {
                Direction::Up => (snapshot.edge_up_sell, snapshot.bid_up),
                Direction::Down => (snapshot.edge_down_sell, snapshot.bid_down),
            };

            if bid_price > 0.0 && sell_edge >= strategy.min_sell_edge {
                let profit_pct = (bid_price - pos.entry_price) / pos.entry_price;
                if profit_pct >= strategy.min_profit_to_sell {
                    let pnl_pct = profit_pct * 100.0;
                    trades.push(Trade {
                        direction: pos.direction,
                        entry_price: pos.entry_price,
                        exit_price: bid_price,
                        pnl_pct,
                        exit_reason: "SELL_EDGE".to_string(),
                    });
                    sold_directions.push(pos.direction);
                }
            }
        }

        // Remove sold positions
        for dir in &sold_directions {
            positions.retain(|p| p.direction != *dir);
            match dir {
                Direction::Up => has_up = false,
                Direction::Down => has_down = false,
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Check OPPOSITE TRIGGER (if position dropped significantly)
        // ═══════════════════════════════════════════════════════════════
        let mut trigger_opposite: Option<(Direction, f64)> = None;
        if strategy.opposite_trigger_drop > 0.0 && !positions.is_empty() {
            for pos in &positions {
                let current_bid = match pos.direction {
                    Direction::Up => snapshot.bid_up,
                    Direction::Down => snapshot.bid_down,
                };
                if current_bid > 0.0 {
                    let drop_pct = (pos.entry_price - current_bid) / pos.entry_price;
                    if drop_pct >= strategy.opposite_trigger_drop {
                        // Trigger opposite side entry
                        let opposite = match pos.direction {
                            Direction::Up => Direction::Down,
                            Direction::Down => Direction::Up,
                        };
                        let (opp_price, opp_edge, opp_size) = match opposite {
                            Direction::Up => (snapshot.price_up, snapshot.edge_up, snapshot.size_up),
                            Direction::Down => (snapshot.price_down, snapshot.edge_down, snapshot.size_down),
                        };
                        let opp_has = match opposite {
                            Direction::Up => has_up,
                            Direction::Down => has_down,
                        };
                        if !opp_has && opp_edge > 0.0 && opp_size >= strategy.min_liquidity {
                            trigger_opposite = Some((opposite, opp_price));
                        }
                    }
                }
            }
        }

        // Apply opposite trigger outside the borrow
        if let Some((opposite, opp_price)) = trigger_opposite {
            positions.push(Position {
                direction: opposite,
                entry_price: opp_price,
                entry_time: snapshot.time_elapsed,
            });
            match opposite {
                Direction::Up => has_up = true,
                Direction::Down => has_down = true,
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 3: Check BUY conditions
        // ═══════════════════════════════════════════════════════════════
        if let Some(direction) = check_entry_conditions(snapshot, strategy, market) {
            let already_has = match direction {
                Direction::Up => has_up,
                Direction::Down => has_down,
            };

            // For single-sided: only one position total
            // For allow_both_sides: can have one UP and one DOWN
            let can_enter = if strategy.allow_both_sides {
                !already_has
            } else {
                positions.is_empty()
            };

            if can_enter {
                let entry_price = match direction {
                    Direction::Up => snapshot.price_up,
                    Direction::Down => snapshot.price_down,
                };
                positions.push(Position {
                    direction,
                    entry_price,
                    entry_time: snapshot.time_elapsed,
                });
                match direction {
                    Direction::Up => has_up = true,
                    Direction::Down => has_down = true,
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // STEP 4: Settle remaining positions at expiration
    // ═══════════════════════════════════════════════════════════════
    for pos in positions {
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
            exit_price: if won { 1.0 } else { 0.0 },
            pnl_pct,
            exit_reason: if won { "EXP_WIN".to_string() } else { "EXP_LOSS".to_string() },
        });
    }

    trades
}

#[derive(Debug, Clone, Default)]
struct StrategyResult {
    name: String,
    description: String,
    total_trades: u32,
    wins: u32,
    losses: u32,
    total_pnl: f64,
    total_invested: f64,
    sell_exits: u32,
    exp_wins: u32,
    exp_losses: u32,
    markets_with_trades: u32,
    // For double-sided tracking
    both_sides_markets: u32,
}

impl StrategyResult {
    fn win_rate(&self) -> f64 {
        if self.total_trades == 0 { 0.0 } else { self.wins as f64 / self.total_trades as f64 * 100.0 }
    }
    fn return_pct(&self) -> f64 {
        if self.total_invested == 0.0 { 0.0 } else { self.total_pnl / self.total_invested * 100.0 }
    }
    fn pnl_per_trade(&self) -> f64 {
        if self.total_trades == 0 { 0.0 } else { self.total_pnl / self.total_trades as f64 }
    }
}

fn run_strategy(markets: &[Market], strategy: &Strategy, bet_amount: f64) -> StrategyResult {
    let mut result = StrategyResult {
        name: strategy.name.clone(),
        description: strategy.description.clone(),
        ..Default::default()
    };

    for market in markets {
        let trades = simulate_market(market, strategy, bet_amount);

        if trades.is_empty() {
            continue;
        }

        result.markets_with_trades += 1;

        // Check if this market had both sides
        let has_up = trades.iter().any(|t| matches!(t.direction, Direction::Up));
        let has_down = trades.iter().any(|t| matches!(t.direction, Direction::Down));
        if has_up && has_down {
            result.both_sides_markets += 1;
        }

        for trade in trades {
            result.total_trades += 1;
            let pnl = bet_amount * trade.pnl_pct / 100.0;
            result.total_pnl += pnl;
            result.total_invested += bet_amount;

            if trade.pnl_pct > 0.0 {
                result.wins += 1;
            } else {
                result.losses += 1;
            }

            match trade.exit_reason.as_str() {
                "SELL_EDGE" => result.sell_exits += 1,
                "EXP_WIN" => result.exp_wins += 1,
                "EXP_LOSS" => result.exp_losses += 1,
                _ => {}
            }
        }
    }

    result
}

// ============================================================================
// Output
// ============================================================================

fn print_results(results: &mut [StrategyResult], bet_amount: f64) {
    // Sort by total P&L descending
    results.sort_by(|a, b| b.total_pnl.partial_cmp(&a.total_pnl).unwrap());

    println!("\n{}", "═".repeat(180));
    println!("{:^180}", format!("CREATIVE STRATEGIES COMPARISON (${:.0} bets)", bet_amount));
    println!("{}", "═".repeat(180));
    println!();

    println!(
        "{:<28} {:>7} {:>7} {:>9} {:>10} {:>8} | {:>6} {:>6} {:>6} | {:>6} {:>8}",
        "Strategy", "Trades", "Win%", "Return%", "P&L", "$/Trade",
        "Sells", "ExpW", "ExpL",
        "Mkts", "BothSide"
    );
    println!("{}", "─".repeat(180));

    for (i, r) in results.iter().enumerate() {
        let rank = if i < 3 { ["🥇", "🥈", "🥉"][i] } else { "  " };

        println!(
            "{} {:<25} {:>7} {:>6.1}% {:>8.1}% {:>9.0}$ {:>7.2}$ | {:>6} {:>6} {:>6} | {:>6} {:>8}",
            rank,
            r.name,
            r.total_trades,
            r.win_rate(),
            r.return_pct(),
            r.total_pnl,
            r.pnl_per_trade(),
            r.sell_exits,
            r.exp_wins,
            r.exp_losses,
            r.markets_with_trades,
            r.both_sides_markets,
        );
    }

    println!("{}", "─".repeat(180));

    // Print descriptions for top 5
    println!("\n📊 TOP 5 STRATEGIES EXPLAINED:");
    for (i, r) in results.iter().take(5).enumerate() {
        println!("  {}. {} - {}", i + 1, r.name, r.description);
    }

    // Print worst 3 for comparison
    println!("\n⚠️ WORST 3 STRATEGIES:");
    for r in results.iter().rev().take(3) {
        println!("  ❌ {} - {} (Return: {:.1}%)", r.name, r.description, r.return_pct());
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("creative_strategies=info".parse()?))
        .init();

    let args = Args::parse();

    println!();
    println!("{}", "═".repeat(80));
    println!("{:^80}", "🎯 CREATIVE STRATEGIES BACKTESTER 🎯");
    println!("{:^80}", "Testing 24 Different Trading Approaches");
    println!("{}", "═".repeat(80));
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

    let up_wins = markets.iter().filter(|m| m.outcome == Outcome::UpWins).count();
    let down_wins = markets.iter().filter(|m| m.outcome == Outcome::DownWins).count();

    println!("📈 Data Summary:");
    println!("   • Time window:  Last {} hours", args.hours);
    println!("   • Markets:      {} total ({} UP wins, {} DOWN wins)", markets.len(), up_wins, down_wins);
    println!("   • Bet amount:   ${:.2}", args.bet_amount);
    println!("   • Sell edge:    {:.0}%", args.min_sell_edge * 100.0);

    let strategies = get_strategies(args.min_sell_edge);
    println!("   • Strategies:   {} to test", strategies.len());

    // Run all strategies
    let mut results: Vec<StrategyResult> = strategies
        .iter()
        .map(|s| run_strategy(&markets, s, args.bet_amount))
        .collect();

    print_results(&mut results, args.bet_amount);

    // Key insights
    println!("\n{}", "═".repeat(80));
    println!("{:^80}", "💡 KEY INSIGHTS");
    println!("{}", "═".repeat(80));

    let best = &results[0];
    let baseline = results.iter().find(|r| r.name.contains("BASELINE_5")).unwrap_or(&results[0]);

    println!();
    println!("  🏆 BEST STRATEGY: {}", best.name);
    println!("     • Return: {:.1}% vs Baseline {:.1}%", best.return_pct(), baseline.return_pct());
    println!("     • P&L: ${:.0} from {} trades", best.total_pnl, best.total_trades);

    if best.both_sides_markets > 0 {
        println!("     • Double-sided markets: {} ({:.1}% of traded markets)",
            best.both_sides_markets,
            best.both_sides_markets as f64 / best.markets_with_trades as f64 * 100.0);
    }

    // Find best double-sided strategy
    if let Some(best_double) = results.iter().find(|r| r.both_sides_markets > 0) {
        println!();
        println!("  🔄 BEST DOUBLE-SIDED: {}", best_double.name);
        println!("     • Return: {:.1}%, Both-sides in {} markets", best_double.return_pct(), best_double.both_sides_markets);
    }

    println!();
    println!("{}", "═".repeat(80));
    println!("{:^80}", "BACKTEST COMPLETE");
    println!("{}", "═".repeat(80));
    println!();

    Ok(())
}
