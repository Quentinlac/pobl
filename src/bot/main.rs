//! BTC 15-Minute Polymarket Trading Bot
//!
//! This bot monitors Polymarket BTC 15-minute UP/DOWN markets and places bets
//! when it detects mispriced opportunities based on historical probability analysis.

// Include modules from the library
#[path = "../models.rs"]
mod models;
#[path = "../edge.rs"]
mod edge;
#[path = "../stats.rs"]
mod stats;
#[path = "../chainlink.rs"]
mod chainlink;

mod binance;
mod config;
mod db;
mod executor;
mod polymarket;
mod redis_state;
mod strategy;
mod websocket;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use config::BotConfig;
use db::{ExecutionRecord, MarketOutcome, TradeAttempt, TradeDb, TradeRecord};
use models::{FirstPassageMatrix, PriceCrossingMatrix, ProbabilityMatrix};
use redis_state::{RedisState, RedisPosition};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strategy::{BetDirection, StrategyContext};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use websocket::MarketState;

/// BTC 15-Minute Polymarket Trading Bot
#[derive(Parser, Debug)]
#[command(name = "btc-bot")]
#[command(about = "Automated trading bot for Polymarket BTC 15-minute markets")]
struct Args {
    /// Place a single $1 test order to verify signing works (ignores spread)
    #[arg(long)]
    test_order: bool,

    /// Amount in USDC for test order (default: 1.0)
    #[arg(long, default_value = "1.0")]
    test_amount: f64,

    /// Direction for test order: "up" or "down" (default: "up")
    #[arg(long, default_value = "up")]
    test_direction: String,
}

/// An open position that we're tracking for potential exit
#[derive(Debug, Clone)]
struct OpenPosition {
    position_id: String,        // UUID to link buy/sell in database
    token_id: String,
    direction: BetDirection,
    entry_price: f64,           // e.g., 0.55 (55 cents)
    shares: f64,                // number of shares bought
    entry_time_bucket: u8,      // time bucket when we bought
    entry_delta_bucket: i8,     // BTC delta bucket when we bought
    exit_target: f64,           // target price to sell at (e.g., 0.70)
    window_start: DateTime<Utc>,
    sell_pending: bool,         // true if we tried to sell but FOK failed
    strategy_type: String,      // "TERMINAL" or "EXIT"
    entry_seconds_elapsed: u32, // seconds elapsed in window when position was opened
}

/// Generate a unique position ID (simple timestamp + random)
fn generate_position_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let random: u64 = rand::random();
    format!("{:016x}-{:016x}", now as u64, random)
}

/// Load probability matrix from local file
fn load_matrix_from_file() -> Result<ProbabilityMatrix> {
    let matrix_path = std::env::var("MATRIX_PATH")
        .unwrap_or_else(|_| "output/matrix.json".to_string());
    let matrix_path = PathBuf::from(&matrix_path);

    if !matrix_path.exists() {
        anyhow::bail!(
            "Probability matrix not found: {}. Run 'cargo run -- build' first.",
            matrix_path.display()
        );
    }

    info!("Loading probability matrix from: {}", matrix_path.display());
    let matrix_json = std::fs::read_to_string(&matrix_path)
        .context("Failed to read matrix file")?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&matrix_json)
        .context("Failed to parse matrix JSON")?;

    Ok(matrix)
}

/// Load first-passage matrix from local file
fn load_first_passage_matrix() -> Result<FirstPassageMatrix> {
    let fp_path = std::env::var("FIRST_PASSAGE_MATRIX_PATH")
        .unwrap_or_else(|_| "output/first_passage_matrix.json".to_string());
    let fp_path = PathBuf::from(&fp_path);

    if !fp_path.exists() {
        anyhow::bail!(
            "First-passage matrix not found: {}. Run 'cargo run -- build' first.",
            fp_path.display()
        );
    }

    info!("Loading first-passage matrix from: {}", fp_path.display());
    let fp_json = std::fs::read_to_string(&fp_path)
        .context("Failed to read first-passage matrix file")?;
    let fp_matrix: FirstPassageMatrix = serde_json::from_str(&fp_json)
        .context("Failed to parse first-passage matrix JSON")?;

    Ok(fp_matrix)
}

/// Load price crossing matrix from local file
fn load_crossing_matrix() -> Result<PriceCrossingMatrix> {
    let pc_path = std::env::var("CROSSING_MATRIX_PATH")
        .unwrap_or_else(|_| "output/price_crossing_matrix.json".to_string());
    let pc_path = PathBuf::from(&pc_path);

    if !pc_path.exists() {
        anyhow::bail!(
            "Price crossing matrix not found: {}. Run 'cargo run -- build' first.",
            pc_path.display()
        );
    }

    info!("Loading price crossing matrix from: {}", pc_path.display());
    let pc_json = std::fs::read_to_string(&pc_path)
        .context("Failed to read crossing matrix file")?;
    let crossing_matrix: PriceCrossingMatrix = serde_json::from_str(&pc_json)
        .context("Failed to parse crossing matrix JSON")?;

    Ok(crossing_matrix)
}

/// Bot state tracking
struct BotState {
    bankroll: f64,
    consecutive_losses: u32,
    consecutive_wins: u32,
    terminal_bets_this_window: u32,  // Bets from terminal strategy
    exit_bets_this_window: u32,      // Bets from exit strategy
    daily_pnl: f64,
    open_positions: Vec<OpenPosition>,  // Track actual positions
    current_window_start: Option<DateTime<Utc>>,
    last_terminal_bet_time: Option<DateTime<Utc>>,  // Separate cooldown for terminal
    last_exit_bet_time: Option<DateTime<Utc>>,      // Separate cooldown for exit
    last_log_time: Option<std::time::Instant>,
    market_fetch_logged: bool,
    bet_pending: bool,  // Flag to prevent race condition - true while order is being executed
}

impl BotState {
    fn new(initial_bankroll: f64) -> Self {
        Self {
            bankroll: initial_bankroll,
            consecutive_losses: 0,
            consecutive_wins: 0,
            terminal_bets_this_window: 0,
            exit_bets_this_window: 0,
            daily_pnl: 0.0,
            open_positions: Vec::new(),
            current_window_start: None,
            last_terminal_bet_time: None,
            last_exit_bet_time: None,
            last_log_time: None,
            market_fetch_logged: false,
            bet_pending: false,
        }
    }

    fn set_bet_pending(&mut self, pending: bool) {
        self.bet_pending = pending;
        if pending {
            debug!("Bet pending - blocking new orders until this one resolves");
        }
    }

    fn is_bet_pending(&self) -> bool {
        self.bet_pending
    }

    fn total_bets_this_window(&self) -> u32 {
        self.terminal_bets_this_window + self.exit_bets_this_window
    }

    fn position_count(&self) -> u32 {
        self.open_positions.len() as u32
    }

    fn has_pending_sells(&self) -> bool {
        self.open_positions.iter().any(|p| p.sell_pending)
    }

    fn mark_sell_pending(&mut self, index: usize) {
        if let Some(pos) = self.open_positions.get_mut(index) {
            pos.sell_pending = true;
            warn!("Position {} marked as SELL PENDING (will retry next cycle)", index);
        }
    }

    fn add_position(&mut self, position: OpenPosition) {
        self.open_positions.push(position);
    }

    fn remove_position(&mut self, index: usize) -> Option<OpenPosition> {
        if index < self.open_positions.len() {
            Some(self.open_positions.remove(index))
        } else {
            None
        }
    }

    fn clear_positions_for_window(&mut self, window_start: DateTime<Utc>) {
        // Remove positions from previous windows (they settled)
        self.open_positions.retain(|p| p.window_start == window_start);
    }

    fn should_log(&mut self, cooldown_seconds: u32) -> bool {
        let now = std::time::Instant::now();
        match self.last_log_time {
            Some(last) if now.duration_since(last).as_secs() < cooldown_seconds as u64 => false,
            _ => {
                self.last_log_time = Some(now);
                true
            }
        }
    }

    fn seconds_since_terminal_bet(&self) -> Option<u32> {
        self.last_terminal_bet_time.map(|t| {
            (chrono::Utc::now() - t).num_seconds().max(0) as u32
        })
    }

    fn seconds_since_exit_bet(&self) -> Option<u32> {
        self.last_exit_bet_time.map(|t| {
            (chrono::Utc::now() - t).num_seconds().max(0) as u32
        })
    }

    fn on_new_window(&mut self, window_start: DateTime<Utc>, outcome: Option<&str>) {
        if self.current_window_start != Some(window_start) {
            // Log settlement for each unsold position BEFORE clearing
            if !self.open_positions.is_empty() {
                let outcome_str = outcome.unwrap_or("UNKNOWN");
                info!("╔══════════════════════════════════════════════════════════════╗");
                info!("║  WINDOW SETTLEMENT - {} unsold position(s)                    ", self.open_positions.len());
                info!("╚══════════════════════════════════════════════════════════════╝");
                info!("  Market outcome: {}", outcome_str);

                let mut total_cost = 0.0;
                let mut total_payout = 0.0;

                for pos in &self.open_positions {
                    let position_won = match (outcome_str, pos.direction) {
                        ("UP", strategy::BetDirection::Up) => true,
                        ("DOWN", strategy::BetDirection::Down) => true,
                        _ => false,
                    };

                    let cost = pos.shares * pos.entry_price;
                    let payout = if position_won { pos.shares } else { 0.0 };
                    let profit = payout - cost;

                    total_cost += cost;
                    total_payout += payout;

                    let status = if position_won { "✓ WON" } else { "✗ LOST" };
                    info!(
                        "  {} {:?}: {:.2} shares @ {:.0}¢ → {} | cost=${:.2}, payout=${:.2}, P&L=${:+.2}",
                        status,
                        pos.direction,
                        pos.shares,
                        pos.entry_price * 100.0,
                        if pos.exit_target < 1.0 {
                            format!("target {:.0}¢ NOT HIT", pos.exit_target * 100.0)
                        } else {
                            "TERMINAL".to_string()
                        },
                        cost,
                        payout,
                        profit
                    );
                }

                let total_profit = total_payout - total_cost;
                info!("  ─────────────────────────────────────────────────────────────");
                info!("  SETTLEMENT TOTAL: cost=${:.2}, payout=${:.2}, P&L=${:+.2}",
                    total_cost, total_payout, total_profit);

                // Update state
                self.daily_pnl += total_profit;
                self.bankroll += total_profit;
            }

            info!("═══ New 15-minute window: {} ═══", window_start.format("%H:%M:%S UTC"));
            // Close positions from the previous window (they resolved when window ended)
            self.clear_positions_for_window(window_start);
            self.current_window_start = Some(window_start);
            self.terminal_bets_this_window = 0;
            self.exit_bets_this_window = 0;
            self.market_fetch_logged = false;
        }
    }

    fn on_bet_placed(&mut self, strategy_type: &str) {
        let now = Utc::now();
        if strategy_type == "TERMINAL" {
            self.terminal_bets_this_window += 1;
            self.last_terminal_bet_time = Some(now);
        } else {
            self.exit_bets_this_window += 1;
            self.last_exit_bet_time = Some(now);
        }
    }

    fn on_position_sold(&mut self, profit: f64) {
        self.daily_pnl += profit;
        self.bankroll += profit;
        if profit > 0.0 {
            self.consecutive_wins += 1;
            self.consecutive_losses = 0;
            info!("Exit profit: ${:.4}", profit);
        } else {
            self.consecutive_losses += 1;
            self.consecutive_wins = 0;
            warn!("Exit loss: ${:.4}", profit);
        }
    }

    #[allow(dead_code)]
    fn on_win(&mut self, amount: f64, config: &BotConfig) {
        self.daily_pnl += amount;
        self.bankroll += amount;
        self.consecutive_wins += 1;
        self.consecutive_losses = 0;

        if self.consecutive_wins >= config.risk.consecutive_wins_to_reset {
            info!("Resetting loss reduction after {} consecutive wins", self.consecutive_wins);
        }
    }

    #[allow(dead_code)]
    fn on_loss(&mut self, amount: f64) {
        self.daily_pnl -= amount;
        self.bankroll -= amount;
        self.consecutive_losses += 1;
        self.consecutive_wins = 0;

        warn!(
            "Loss recorded: ${:.2}, consecutive losses: {}",
            amount, self.consecutive_losses
        );
    }
}

/// Run a single test order to verify signing and order execution
async fn run_test_order(amount: f64, direction: &str) -> Result<()> {
    info!("═══════════════════════════════════════════════════════════════");
    info!("TEST ORDER MODE");
    info!("  Amount:    ${:.2}", amount);
    info!("  Direction: {}", direction.to_uppercase());
    info!("═══════════════════════════════════════════════════════════════");

    // Validate direction
    let is_up = match direction.to_lowercase().as_str() {
        "up" => true,
        "down" => false,
        _ => {
            error!("Invalid direction: {}. Use 'up' or 'down'", direction);
            return Err(anyhow::anyhow!("Invalid direction"));
        }
    };

    // Initialize executor
    let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
        .context("POLYMARKET_PRIVATE_KEY not set - required for test order")?;

    info!("Initializing order executor...");
    let mut exec = executor::Executor::new(&private_key, None).await
        .context("Failed to initialize executor")?;
    info!("Executor ready: {}", exec.wallet_address());

    // Fetch current market
    info!("Fetching current BTC 15-minute market...");
    let polymarket = polymarket::PolymarketClient::new(10000)?;
    let market = polymarket.get_current_btc_15m_market().await
        .context("Failed to fetch market")?;

    info!("Market found: {}", market.slug);
    info!("  UP token:   {}", market.up_token_id);
    info!("  DOWN token: {}", market.down_token_id);

    // Get order book for the chosen token
    let token_id = if is_up { &market.up_token_id } else { &market.down_token_id };
    let book = polymarket.get_order_book(token_id).await
        .context("Failed to get order book")?;
    let quote = polymarket.get_price_quote(&book)?;

    info!("Order book:");
    info!("  Best bid: {:.2}¢", quote.best_bid * 100.0);
    info!("  Best ask: {:.2}¢", quote.best_ask * 100.0);
    info!("  Spread:   {:.2}%", quote.spread_pct * 100.0);

    // Use the ask price (we're buying)
    let price = quote.best_ask;
    if price <= 0.0 || price >= 1.0 {
        error!("Invalid ask price: {}. Market may have no liquidity.", price);
        return Err(anyhow::anyhow!("No liquidity"));
    }

    info!("");
    info!("Placing TEST ORDER:");
    info!("  Token: {} ({})", if is_up { "UP" } else { "DOWN" }, &token_id[..20]);
    info!("  Amount: ${:.2}", amount);
    info!("  Price: {:.2}¢", price * 100.0);
    info!("");

    // Place the order
    match exec.market_buy(token_id, price, amount).await {
        Ok(response) => {
            if response.success {
                info!("╔══════════════════════════════════════════════════════════════╗");
                info!("║  ✓ TEST ORDER SUCCESSFUL!                                    ║");
                info!("╚══════════════════════════════════════════════════════════════╝");
                info!("  Order ID: {:?}", response.order_id);
            } else {
                error!("╔══════════════════════════════════════════════════════════════╗");
                error!("║  ✗ TEST ORDER REJECTED                                       ║");
                error!("╚══════════════════════════════════════════════════════════════╝");
                error!("  Error: {:?}", response.error_msg);
            }
        }
        Err(e) => {
            error!("╔══════════════════════════════════════════════════════════════╗");
            error!("║  ✗ TEST ORDER FAILED                                         ║");
            error!("╚══════════════════════════════════════════════════════════════╝");
            error!("  Error: {}", e);
            return Err(e);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Early debug output (before tracing setup)
    eprintln!("[btc-bot] Starting...");

    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    eprintln!("[btc-bot] Parsing args...");
    let args = Args::parse();

    // Initialize logging
    eprintln!("[btc-bot] Initializing logging...");
    let log_filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info,btc_bot=debug".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&log_filter))
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    eprintln!("[btc-bot] Logging initialized OK");
    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║       BTC 15-MINUTE POLYMARKET TRADING BOT                   ║");
    info!("╚══════════════════════════════════════════════════════════════╝");

    // Handle test order mode
    if args.test_order {
        return run_test_order(args.test_amount, &args.test_direction).await;
    }

    // Load configuration
    let config_path = std::env::var("BOT_CONFIG_PATH")
        .unwrap_or_else(|_| "config/bot_config.yaml".to_string());
    let config_path = PathBuf::from(&config_path);

    let config = if config_path.exists() {
        info!("Loading config from: {}", config_path.display());
        BotConfig::load_with_env(&config_path)
            .context("Failed to load configuration")?
    } else {
        warn!("Config file not found, using defaults: {}", config_path.display());
        BotConfig::default()
    };

    info!("Configuration loaded:");
    info!("  Polling interval: {}ms", config.polling.interval_ms);
    info!("  Min edge (strong): {:.1}%", config.edge.min_edge_strong * 100.0);
    info!("  Kelly fraction: {:.0}%", config.betting.kelly_fraction * 100.0);
    info!("  Max bet: ${:.2} or {:.0}% of bankroll", config.betting.max_bet_usdc, config.betting.max_bet_pct * 100.0);

    // Connect to database for trade tracking
    let trade_db = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            info!("Connecting to trade database...");
            match TradeDb::connect(&url).await {
                Ok(db) => {
                    // Run migrations
                    if let Err(e) = db.run_migrations().await {
                        warn!("Failed to run migrations: {}", e);
                    }
                    Some(db)
                }
                Err(e) => {
                    warn!("Failed to connect to database: {}", e);
                    warn!("Running without trade tracking");
                    None
                }
            }
        }
        Err(_) => {
            info!("DATABASE_URL not set - trade tracking disabled");
            None
        }
    };

    // Connect to Redis for multi-pod coordination
    let redis_state = match RedisState::connect().await {
        Ok(rs) => {
            info!("Redis connected for shared state");
            Some(rs)
        }
        Err(e) => {
            warn!("Failed to connect to Redis: {}", e);
            warn!("Running without multi-pod coordination - ensure only 1 pod!");
            None
        }
    };

    // Load probability matrix (try DB first, fall back to file, retry if not found)
    let matrix: ProbabilityMatrix = loop {
        let result = match std::env::var("DATABASE_URL") {
            Ok(ref url) => {
                info!("Loading probability matrix from database...");
                match db::load_matrix_from_db(url).await {
                    Ok(Some((m, info))) => {
                        info!(
                            "Matrix loaded from DB: snapshot #{}, {} windows, updated {}",
                            info.id, info.total_windows, info.created_at.format("%Y-%m-%d %H:%M UTC")
                        );
                        Some(m)
                    }
                    Ok(None) => {
                        warn!("No matrix found in database, trying file fallback...");
                        load_matrix_from_file().ok()
                    }
                    Err(e) => {
                        warn!("Failed to load matrix from DB: {}, trying file fallback...", e);
                        load_matrix_from_file().ok()
                    }
                }
            }
            Err(_) => {
                info!("DATABASE_URL not set, loading matrix from file...");
                load_matrix_from_file().ok()
            }
        };

        match result {
            Some(m) => break m,
            None => {
                error!("═══════════════════════════════════════════════════════════════");
                error!("No probability matrix available!");
                error!("Please run the matrix builder first:");
                error!("  cargo run -- build");
                error!("Or deploy the matrix-builder cron job on Qovery");
                error!("Retrying in 60 seconds...");
                error!("═══════════════════════════════════════════════════════════════");
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    };
    info!("Matrix ready: {} windows analyzed", matrix.total_windows);

    // Load first-passage matrix for exit strategy (legacy)
    let fp_matrix: Option<FirstPassageMatrix> = if config.exit_strategy.enabled {
        match load_first_passage_matrix() {
            Ok(fp) => {
                info!("First-passage matrix loaded: {} observations", fp.total_observations);
                Some(fp)
            }
            Err(e) => {
                warn!("Failed to load first-passage matrix: {}", e);
                None
            }
        }
    } else {
        info!("Exit strategy disabled in config");
        None
    };

    // Load crossing matrix for dynamic exit targeting (preferred over fp_matrix)
    let crossing_matrix: Option<PriceCrossingMatrix> = if config.exit_strategy.enabled {
        match load_crossing_matrix() {
            Ok(cm) => {
                info!("Crossing matrix loaded: {} trajectories (DYNAMIC TARGETING ENABLED)", cm.total_trajectories);
                Some(cm)
            }
            Err(e) => {
                warn!("Failed to load crossing matrix: {}", e);
                warn!("Falling back to first-passage matrix for exit strategy");
                None
            }
        }
    } else {
        None
    };

    // Get initial bankroll from environment
    let initial_bankroll: f64 = std::env::var("BOT_BANKROLL")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .unwrap_or(1000.0);
    info!("Starting bankroll: ${:.2}", initial_bankroll);

    // Initialize REST clients (still needed for market discovery and order execution)
    let polymarket = polymarket::PolymarketClient::new(config.polling.request_timeout_ms)?;
    let binance = binance::BinanceClient::new(config.polling.request_timeout_ms)?;

    // Initialize order executor (if credentials are available)
    let mut order_executor = match std::env::var("POLYMARKET_PRIVATE_KEY") {
        Ok(private_key) => {
            info!("Initializing order executor...");
            match executor::Executor::new(&private_key, None).await {
                Ok(exec) => {
                    info!("Order executor ready: {}", exec.wallet_address());
                    Some(exec)
                }
                Err(e) => {
                    warn!("Failed to initialize executor: {}", e);
                    warn!("Running in DRY-RUN mode (no orders will be placed)");
                    None
                }
            }
        }
        Err(_) => {
            warn!("POLYMARKET_PRIVATE_KEY not set - running in DRY-RUN mode");
            None
        }
    };

    // Fetch current BTC 15m market (tokens change every 15 minutes)
    info!("Fetching current BTC 15-minute market...");
    let mut current_market = match polymarket.get_current_btc_15m_market().await {
        Ok(market) => {
            info!("Market found: {}", market.slug);
            info!("  UP token:   {}", market.up_token_id);
            info!("  DOWN token: {}", market.down_token_id);
            Some(market)
        }
        Err(e) => {
            warn!("Failed to fetch BTC 15m market: {}", e);
            warn!("Running in DRY-RUN mode until market is available");
            None
        }
    };

    // Initialize bot state
    let mut state = BotState::new(initial_bankroll);

    // Set up graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received, stopping bot...");
        r.store(false, Ordering::SeqCst);
    });

    // Initialize shared market state for WebSocket data
    let market_state = Arc::new(RwLock::new(MarketState::default()));

    // Set initial market tokens if we have them
    if let Some(ref market) = current_market {
        let mut ms = market_state.write().await;
        ms.market_slug = market.slug.clone();
        ms.up_token_id = market.up_token_id.clone();
        ms.down_token_id = market.down_token_id.clone();
    }

    // Spawn WebSocket tasks
    let binance_state = market_state.clone();
    let binance_running = running.clone();
    tokio::spawn(async move {
        if let Err(e) = websocket::binance_ws_task(binance_state, binance_running).await {
            error!("Binance WebSocket task failed: {}", e);
        }
    });

    let poly_state = market_state.clone();
    let poly_running = running.clone();
    info!(">>> Spawning Polymarket WebSocket task...");
    tokio::spawn(async move {
        info!(">>> Polymarket WebSocket task spawned and running");
        if let Err(e) = websocket::polymarket_ws_task(poly_state, poly_running).await {
            error!("Polymarket WebSocket task failed: {}", e);
        }
        info!(">>> Polymarket WebSocket task ended");
    });

    info!("");
    info!("Bot started with REAL-TIME WebSocket data!");
    info!("Press Ctrl+C to stop");
    info!("");

    // Main loop
    let poll_interval = Duration::from_millis(config.polling.interval_ms);
    let mut window_open_price: Option<f64> = None;
    let mut last_window_outcome: Option<String> = None; // Track outcome for settlement logging

    while running.load(Ordering::SeqCst) {
        let loop_start = std::time::Instant::now();

        // Get current window info
        let window_start = binance::get_current_window_start();
        let seconds_elapsed = binance::get_seconds_elapsed();
        let seconds_remaining = binance::get_seconds_remaining();

        // Check for new window (pass last outcome for settlement logging)
        state.on_new_window(window_start, last_window_outcome.as_deref());
        last_window_outcome = None; // Clear after use

        // Reset open price on new window (REST API - only once per window)
        if state.current_window_start == Some(window_start) && window_open_price.is_none() {
            match binance.get_window_open_price(window_start).await {
                Ok(price) => {
                    window_open_price = Some(price);
                    // Also update shared state
                    {
                        let mut ms = market_state.write().await;
                        ms.window_open_price = price;
                        ms.window_start = Some(window_start);
                    }
                    info!("Window open price: ${:.2}", price);
                }
                Err(e) => {
                    warn!("Failed to get window open price: {}", e);
                }
            }
        }

        // Get current BTC price from WebSocket (real-time!)
        let btc_price = {
            let ms = market_state.read().await;
            ms.btc_price
        };

        if btc_price == 0.0 {
            debug!("Waiting for WebSocket BTC price...");
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Calculate price delta
        let price_delta = match window_open_price {
            Some(open) => btc_price - open,
            None => {
                debug!("Waiting for window open price...");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if config.logging.log_price_checks {
            debug!(
                "BTC: ${:.2} | Delta: ${:+.2} | Time: {}s / {}s remaining",
                btc_price, price_delta, seconds_elapsed, seconds_remaining
            );
        }

        // Get current market tokens (refresh if needed via REST)
        let market = match &current_market {
            Some(m) => m.clone(),
            None => {
                // Try to fetch market (only log once per window to avoid spam)
                if !state.market_fetch_logged {
                    info!("Waiting for market: btc-updown-15m-*");
                    state.market_fetch_logged = true;
                }
                match polymarket.get_current_btc_15m_market().await {
                    Ok(m) => {
                        info!("✓ Market found: {} | UP={:.8}... DOWN={:.8}...",
                            m.slug,
                            &m.up_token_id[..16.min(m.up_token_id.len())],
                            &m.down_token_id[..16.min(m.down_token_id.len())]);
                        // Update shared state for WebSocket subscriptions
                        {
                            let mut ms = market_state.write().await;
                            ms.market_slug = m.slug.clone();
                            ms.up_token_id = m.up_token_id.clone();
                            ms.down_token_id = m.down_token_id.clone();
                            // Reset order book to force fresh data from WebSocket
                            ms.up_best_ask = 0.0;
                            ms.up_best_bid = 0.0;
                            ms.down_best_ask = 0.0;
                            ms.down_best_bid = 0.0;
                            info!("WebSocket state initialized with market tokens");
                        }
                        current_market = Some(m.clone());
                        m
                    }
                    Err(e) => {
                        // Still no market - show dry-run status periodically
                        if state.should_log(config.cooldown.log_cooldown_seconds) {
                            let time_bucket = (seconds_elapsed / 30).min(29) as u8;
                            let delta_bucket = models::delta_to_bucket(
                                rust_decimal::Decimal::try_from(price_delta).unwrap_or_default()
                            );
                            let cell = matrix.get(time_bucket, delta_bucket);

                            info!(
                                "[NO-MARKET] BTC ${:.0} | Δ${:+.0} | t={}s | P(UP)={:.0}% n={} | {}",
                                btc_price, price_delta, seconds_elapsed,
                                cell.p_up * 100.0, cell.total(), e
                            );
                        }
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                }
            }
        };

        // Get order book prices from WebSocket (real-time!)
        let (up_quote, down_quote) = {
            let ms = market_state.read().await;

            // Debug: log raw MarketState values every 10 iterations
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count % 10 == 0 {
                info!("DEBUG MarketState: up_bid={:.4} up_ask={:.4} down_bid={:.4} down_ask={:.4} tokens_set={}",
                      ms.up_best_bid, ms.up_best_ask, ms.down_best_bid, ms.down_best_ask,
                      !ms.up_token_id.is_empty() && !ms.down_token_id.is_empty());
            }

            if ms.up_best_ask == 0.0 || ms.down_best_ask == 0.0 {
                debug!("Waiting for WebSocket order book data...");
                drop(ms);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            (ms.up_quote(), ms.down_quote())
        };

        // Dummy book variables for compatibility (not used but referenced elsewhere)
        let up_book = polymarket::OrderBook {
            market: market.slug.clone(),
            asset_id: market.up_token_id.clone(),
            bids: vec![],
            asks: vec![],
            timestamp: String::new(),
        };
        let down_book = polymarket::OrderBook {
            market: market.slug.clone(),
            asset_id: market.down_token_id.clone(),
            bids: vec![],
            asks: vec![],
            timestamp: String::new(),
        };
        let _ = (&up_book, &down_book); // Suppress unused warnings - kept for future use if needed
        // Note: We no longer use REST for order books - all data comes from WebSocket!

        // Get matrix cell info for calculations
        let time_bucket = (seconds_elapsed / 30).min(29) as u8;
        let delta_bucket = models::delta_to_bucket(
            rust_decimal::Decimal::try_from(price_delta).unwrap_or_default()
        );

        // ═══════════════════════════════════════════════════════════════
        // CHECK SELL EDGE FOR TERMINAL POSITIONS
        // Sell when (bid - our_probability) / bid >= min_sell_edge
        // Same logic as buying, but for selling - market overvalues our position
        // ═══════════════════════════════════════════════════════════════
        if config.terminal_strategy.enabled && !state.open_positions.is_empty() {
            // Get our probability estimates using the matrix
            let cell = matrix.get(time_bucket, delta_bucket);
            let our_p_up = cell.p_up_wilson_lower;
            let our_p_down = 1.0 - cell.p_up_wilson_upper;

            struct SellEdgeAction {
                idx: usize,
                position: OpenPosition,
                current_bid: f64,
                our_prob: f64,
                sell_edge: f64,
            }
            let mut sell_edge_actions: Vec<SellEdgeAction> = Vec::new();

            for (idx, position) in state.open_positions.iter().enumerate() {
                // Only check TERMINAL positions from current window
                if position.window_start != window_start {
                    continue;
                }
                if position.strategy_type != "TERMINAL" {
                    continue;
                }

                // Get current bid and our probability for this position's token
                let (current_bid, our_prob) = match position.direction {
                    BetDirection::Up => (up_quote.best_bid, our_p_up),
                    BetDirection::Down => (down_quote.best_bid, our_p_down),
                };

                // Calculate sell edge: (bid - our_prob) / bid
                // Positive = market paying more than we think it's worth = good to sell
                if current_bid > 0.01 {
                    let sell_edge = (current_bid - our_prob) / current_bid;
                    let profit_pct = (current_bid - position.entry_price) / position.entry_price;

                    // Must meet BOTH conditions:
                    // 1. Sell edge >= min_sell_edge (market overvalues position)
                    // 2. Profit >= min_profit_before_sell (we're not selling at a loss)
                    if sell_edge >= config.terminal_strategy.min_sell_edge
                        && profit_pct >= config.terminal_strategy.min_profit_before_sell
                    {
                        sell_edge_actions.push(SellEdgeAction {
                            idx,
                            position: position.clone(),
                            current_bid,
                            our_prob,
                            sell_edge,
                        });
                    }
                }
            }

            // Execute sell edge sells
            let mut indices_to_remove: Vec<usize> = Vec::new();
            for action in sell_edge_actions {
                let position = &action.position;
                let current_bid = action.current_bid;
                let profit_pct = (current_bid - position.entry_price) / position.entry_price;

                if let Some(ref mut exec) = order_executor {
                    info!("═══════════════════════════════════════════════════════════════");
                    info!("SELL EDGE TRIGGERED - TERMINAL POSITION!");
                    info!("  Direction:    {:?}", position.direction);
                    info!("  Entry price:  {:.2}¢", position.entry_price * 100.0);
                    info!("  Exit price:   {:.2}¢ (bid)", current_bid * 100.0);
                    info!("  Our prob:     {:.1}%", action.our_prob * 100.0);
                    info!("  Sell edge:    +{:.1}% (min: {:.1}%)", action.sell_edge * 100.0, config.terminal_strategy.min_sell_edge * 100.0);
                    info!("  Profit:       {:+.1}% (min: {:.1}%)", profit_pct * 100.0, config.terminal_strategy.min_profit_before_sell * 100.0);
                    info!("  Shares:       {:.2}", position.shares);
                    info!("═══════════════════════════════════════════════════════════════");

                    // Get bid liquidity for this position's token
                    let bid_liquidity = match position.direction {
                        BetDirection::Up => up_quote.bid_liquidity,
                        BetDirection::Down => down_quote.bid_liquidity,
                    };

                    // Create sell execution record
                    let sell_execution = ExecutionRecord {
                        position_id: position.position_id.clone(),
                        side: "SELL".to_string(),
                        market_slug: Some(market.slug.clone()),
                        token_id: position.token_id.clone(),
                        direction: format!("{:?}", position.direction).to_uppercase(),
                        window_start: position.window_start,
                        order_type: "FOK".to_string(),
                        requested_price: current_bid,
                        requested_amount: position.shares,
                        requested_shares: Some(position.shares),
                        filled_price: None,
                        filled_amount: None,
                        filled_shares: None,
                        status: "PENDING".to_string(),
                        error_message: None,
                        order_id: None,
                        time_elapsed_s: Some(seconds_elapsed as i32),
                        btc_price: Some(btc_price),
                        btc_delta: Some(price_delta),
                        edge_pct: None,
                        our_probability: Some(action.our_prob),
                        market_probability: Some(current_bid),
                        best_ask: match position.direction {
                            BetDirection::Up => Some(up_quote.best_ask),
                            BetDirection::Down => Some(down_quote.best_ask),
                        },
                        best_bid: Some(current_bid),
                        ask_liquidity: match position.direction {
                            BetDirection::Up => Some(up_quote.ask_liquidity),
                            BetDirection::Down => Some(down_quote.ask_liquidity),
                        },
                        bid_liquidity: Some(bid_liquidity),
                        sell_edge_pct: Some(action.sell_edge),
                        profit_pct: Some(profit_pct),
                        entry_price: Some(position.entry_price),
                    };

                    // Insert pending execution record
                    let sell_exec_id = if let Some(ref db) = trade_db {
                        match db.insert_execution(&sell_execution).await {
                            Ok(id) => Some(id),
                            Err(e) => {
                                warn!("Failed to insert sell execution record: {}", e);
                                None
                            }
                        }
                    } else {
                        None
                    };

                    match exec.fok_sell_with_liquidity(
                        &position.token_id,
                        current_bid,
                        position.shares,
                        bid_liquidity,
                    ).await {
                        Ok((response, actual_shares, actual_usdc)) => {
                            if response.success {
                                info!("✓ Sell edge FOK FILLED! ID: {:?}", response.order_id);
                                info!("  Filled: {:.4} shares for ${:.2}", actual_shares, actual_usdc);

                                // Update execution record
                                if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                    if let Err(e) = db.update_execution_status(
                                        id,
                                        "FILLED",
                                        Some(current_bid),
                                        Some(actual_shares),
                                        Some(actual_shares),
                                        response.order_id.as_deref(),
                                        None,
                                    ).await {
                                        warn!("Failed to update sell execution status: {}", e);
                                    }
                                }

                                let profit = (current_bid - position.entry_price) * actual_shares;
                                state.on_position_sold(profit);
                                indices_to_remove.push(action.idx);
                            } else {
                                warn!("Sell edge FOK rejected: {:?} - will retry next cycle", response.error_msg);

                                // Update execution record with rejection
                                if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                    if let Err(e) = db.update_execution_status(
                                        id,
                                        "CANCELLED",
                                        None, None, None,
                                        response.order_id.as_deref(),
                                        response.error_msg.as_deref(),
                                    ).await {
                                        warn!("Failed to update sell execution status: {}", e);
                                    }
                                }

                                state.mark_sell_pending(action.idx);
                            }
                        }
                        Err(e) => {
                            error!("Sell edge execution error: {} - will retry next cycle", e);

                            // Update execution record with error
                            if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                if let Err(e2) = db.update_execution_status(
                                    id,
                                    "FAILED",
                                    None, None, None, None,
                                    Some(&e.to_string()),
                                ).await {
                                    warn!("Failed to update sell execution status: {}", e2);
                                }
                            }

                            state.mark_sell_pending(action.idx);
                        }
                    }
                } else {
                    info!("[DRY-RUN] SELL EDGE: {:?} edge={:+.1}%>={:.1}% profit={:+.1}%>={:.1}% | sell {:.2} shares at {:.2}¢",
                        position.direction,
                        action.sell_edge * 100.0, config.terminal_strategy.min_sell_edge * 100.0,
                        profit_pct * 100.0, config.terminal_strategy.min_profit_before_sell * 100.0,
                        position.shares, current_bid * 100.0);
                    let profit = (current_bid - position.entry_price) * position.shares;
                    state.on_position_sold(profit);
                    indices_to_remove.push(action.idx);
                }
            }

            // Remove sold positions (in reverse order to maintain indices)
            indices_to_remove.sort();
            for idx in indices_to_remove.into_iter().rev() {
                state.remove_position(idx);
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // CHECK EXIT CONDITIONS FOR OPEN POSITIONS (EXIT STRATEGY)
        // Use crossing matrix (dynamic targeting) if available, else fp_matrix
        // ═══════════════════════════════════════════════════════════════
        if crossing_matrix.is_some() || fp_matrix.is_some() {
            // First, collect all sell actions (to avoid borrow checker issues)
            struct SellAction {
                idx: usize,
                position: OpenPosition,
                current_bid: f64,
            }
            let mut sell_actions: Vec<SellAction> = Vec::new();

            for (idx, position) in state.open_positions.iter().enumerate() {
                // Only check positions from current window
                if position.window_start != window_start {
                    continue;
                }

                // Get current bid for this position's token
                let current_bid = match position.direction {
                    BetDirection::Up => up_quote.best_bid,
                    BetDirection::Down => down_quote.best_bid,
                };

                // Check if exit target is hit - prefer crossing matrix for dynamic targeting
                let exit_decision = if let Some(ref cm) = crossing_matrix {
                    // Use crossing-based dynamic exit (recalculates target each tick)
                    strategy::decide_exit_crossing(
                        &config,
                        cm,
                        &matrix,
                        time_bucket,
                        delta_bucket,
                        position.direction,
                        position.entry_price,
                        current_bid,
                        position.exit_target,
                    )
                } else if let Some(ref fp) = fp_matrix {
                    // Fallback to first-passage based exit (legacy)
                    strategy::decide_exit(
                        &config,
                        fp,
                        &matrix,
                        time_bucket,
                        delta_bucket,
                        position.direction,
                        position.entry_price,
                        current_bid,
                        position.exit_target,
                    )
                } else {
                    strategy::ExitDecision::no_exit("No exit matrix available".to_string())
                };

                if exit_decision.should_exit {
                    sell_actions.push(SellAction {
                        idx,
                        position: position.clone(),
                        current_bid,
                    });
                }
            }

            // Now execute sells and update state
            let mut indices_to_remove: Vec<usize> = Vec::new();
            for action in sell_actions {
                let position = &action.position;
                let current_bid = action.current_bid;

                // Execute sell
                if let Some(ref mut exec) = order_executor {
                    info!("═══════════════════════════════════════════════════════════════");
                    info!("EXIT SIGNAL - SELLING POSITION!");
                    info!("  Direction:    {:?}", position.direction);
                    info!("  Entry price:  {:.2}¢", position.entry_price * 100.0);
                    info!("  Exit price:   {:.2}¢", current_bid * 100.0);
                    info!("  Target was:   {:.2}¢", position.exit_target * 100.0);
                    info!("  Shares:       {:.2}", position.shares);
                    info!("  Profit/share: {:.2}¢", (current_bid - position.entry_price) * 100.0);
                    info!("═══════════════════════════════════════════════════════════════");

                    // Get bid liquidity for this position's token
                    let bid_liquidity = match position.direction {
                        BetDirection::Up => up_quote.bid_liquidity,
                        BetDirection::Down => down_quote.bid_liquidity,
                    };

                    let profit_pct = (current_bid - position.entry_price) / position.entry_price;

                    // Create sell execution record
                    let sell_execution = ExecutionRecord {
                        position_id: position.position_id.clone(),
                        side: "SELL".to_string(),
                        market_slug: Some(market.slug.clone()),
                        token_id: position.token_id.clone(),
                        direction: format!("{:?}", position.direction).to_uppercase(),
                        window_start: position.window_start,
                        order_type: "FOK".to_string(),
                        requested_price: current_bid,
                        requested_amount: position.shares,
                        requested_shares: Some(position.shares),
                        filled_price: None,
                        filled_amount: None,
                        filled_shares: None,
                        status: "PENDING".to_string(),
                        error_message: None,
                        order_id: None,
                        time_elapsed_s: Some(seconds_elapsed as i32),
                        btc_price: Some(btc_price),
                        btc_delta: Some(price_delta),
                        edge_pct: None,
                        our_probability: None,
                        market_probability: Some(current_bid),
                        best_ask: match position.direction {
                            BetDirection::Up => Some(up_quote.best_ask),
                            BetDirection::Down => Some(down_quote.best_ask),
                        },
                        best_bid: Some(current_bid),
                        ask_liquidity: match position.direction {
                            BetDirection::Up => Some(up_quote.ask_liquidity),
                            BetDirection::Down => Some(down_quote.ask_liquidity),
                        },
                        bid_liquidity: Some(bid_liquidity),
                        sell_edge_pct: None,
                        profit_pct: Some(profit_pct),
                        entry_price: Some(position.entry_price),
                    };

                    // Insert pending execution record
                    let sell_exec_id = if let Some(ref db) = trade_db {
                        match db.insert_execution(&sell_execution).await {
                            Ok(id) => Some(id),
                            Err(e) => {
                                warn!("Failed to insert sell execution record: {}", e);
                                None
                            }
                        }
                    } else {
                        None
                    };

                    match exec.fok_sell_with_liquidity(
                        &position.token_id,
                        current_bid,
                        position.shares,
                        bid_liquidity,
                    ).await {
                        Ok((response, actual_shares, actual_usdc)) => {
                            if response.success {
                                info!("✓ Exit FOK FILLED! ID: {:?}", response.order_id);
                                info!("  Filled: {:.4} shares for ${:.2}", actual_shares, actual_usdc);

                                // Update execution record
                                if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                    if let Err(e) = db.update_execution_status(
                                        id,
                                        "FILLED",
                                        Some(current_bid),
                                        Some(actual_shares),
                                        Some(actual_shares),
                                        response.order_id.as_deref(),
                                        None,
                                    ).await {
                                        warn!("Failed to update sell execution status: {}", e);
                                    }
                                }

                                let profit = (current_bid - position.entry_price) * actual_shares;
                                state.on_position_sold(profit);
                                indices_to_remove.push(action.idx);
                            } else {
                                warn!("Exit FOK rejected: {:?} - will retry next cycle", response.error_msg);

                                // Update execution record with rejection
                                if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                    if let Err(e) = db.update_execution_status(
                                        id,
                                        "CANCELLED",
                                        None, None, None,
                                        response.order_id.as_deref(),
                                        response.error_msg.as_deref(),
                                    ).await {
                                        warn!("Failed to update sell execution status: {}", e);
                                    }
                                }

                                state.mark_sell_pending(action.idx);
                            }
                        }
                        Err(e) => {
                            error!("Exit sell execution error: {} - will retry next cycle", e);

                            // Update execution record with error
                            if let (Some(id), Some(ref db)) = (sell_exec_id, &trade_db) {
                                if let Err(e2) = db.update_execution_status(
                                    id,
                                    "FAILED",
                                    None, None, None, None,
                                    Some(&e.to_string()),
                                ).await {
                                    warn!("Failed to update sell execution status: {}", e2);
                                }
                            }

                            state.mark_sell_pending(action.idx);
                        }
                    }
                } else {
                    info!("[DRY-RUN] Would sell {:?} position: {:.2} shares at {:.2}¢",
                        position.direction, position.shares, current_bid * 100.0);
                    let profit = (current_bid - position.entry_price) * position.shares;
                    state.on_position_sold(profit);
                    indices_to_remove.push(action.idx);
                }
            }

            // Remove sold positions (in reverse order to maintain indices)
            indices_to_remove.sort();
            for idx in indices_to_remove.into_iter().rev() {
                state.remove_position(idx);
            }
        }

        // ═══════════════════════════════════════════════════════════════
        // CHECK BUY CONDITIONS
        // ═══════════════════════════════════════════════════════════════

        // Make buy decision
        let ctx = StrategyContext {
            config: &config,
            matrix: &matrix,
            fp_matrix: fp_matrix.as_ref(),
            crossing_matrix: crossing_matrix.as_ref(),
            bankroll: state.bankroll,
            consecutive_losses: state.consecutive_losses,
            terminal_bets_this_window: state.terminal_bets_this_window,
            exit_bets_this_window: state.exit_bets_this_window,
            daily_pnl: state.daily_pnl,
            open_positions: state.position_count(),
        };

        let decision = ctx.decide(seconds_elapsed, price_delta, &up_quote, &down_quote);

        // Get matrix cell info for logging
        let cell = matrix.get(time_bucket, delta_bucket);

        // Log status periodically (respecting cooldown)
        if state.should_log(config.cooldown.log_cooldown_seconds) {
            // Concise one-liner with all key info
            let edge_str = if decision.edge > 0.0 {
                format!("+{:.1}%", decision.edge * 100.0)
            } else {
                format!("{:.1}%", decision.edge * 100.0)
            };

            info!(
                "BTC ${:.0} Δ${:+.0} | t={}s | UP:{:.0}¢/{:.0}¢ DOWN:{:.0}¢/{:.0}¢ | P(UP)={:.0}% n={} | edge={} | {}",
                btc_price,
                price_delta,
                seconds_elapsed,
                up_quote.best_bid * 100.0, up_quote.best_ask * 100.0,
                down_quote.best_bid * 100.0, down_quote.best_ask * 100.0,
                cell.p_up * 100.0,
                cell.total(),
                edge_str,
                if decision.should_bet { "→ BET!" } else { &decision.reason }
            );
        }

        // Check if we have pending sells - don't buy new positions until sold
        let has_pending_sells = state.has_pending_sells();
        if has_pending_sells && decision.should_bet {
            info!("⏸ SKIP BUY: have pending sells to retry first");
        }

        // Check if we're at max open positions
        let at_max_positions = state.position_count() >= config.risk.max_open_positions;
        if at_max_positions && decision.should_bet {
            info!("⏸ SKIP BUY: at max positions ({}/{})",
                state.position_count(), config.risk.max_open_positions);
        }

        // Check strategy-specific cooldown before betting (skip if pending sells)
        let in_cooldown = if decision.should_bet && !has_pending_sells {
            let (last_bet_secs, cooldown_required) = if decision.strategy_type == "TERMINAL" {
                (state.seconds_since_terminal_bet(), config.terminal_strategy.cooldown_seconds)
            } else {
                (state.seconds_since_exit_bet(), config.exit_strategy.cooldown_seconds)
            };

            match last_bet_secs {
                Some(secs) if secs < cooldown_required => {
                    info!("⏸ SKIP BUY: [{}] cooldown {}s/{} required",
                        decision.strategy_type, secs, cooldown_required);
                    true
                }
                _ => false,
            }
        } else {
            false
        };

        // Check if a bet is already pending (prevents race condition)
        if state.is_bet_pending() && decision.should_bet {
            info!("⏸ SKIP BUY: order already pending");
        }

        if decision.should_bet && !in_cooldown && !has_pending_sells && !at_max_positions && !state.is_bet_pending() {
            let direction = decision.direction.unwrap();

            // Set pending flag IMMEDIATELY to prevent race condition
            state.set_bet_pending(true);

            info!("═══════════════════════════════════════════════════════════════");
            info!("BET SIGNAL DETECTED!");
            info!("  Direction:    {:?}", direction);
            info!("  Edge:         {:.2}%", decision.edge * 100.0);
            info!("  Our P:        {:.2}%", decision.our_probability * 100.0);
            info!("  Market P:     {:.2}%", decision.market_probability * 100.0);
            info!("  Bet Amount:   ${:.2}", decision.bet_amount);
            info!("  Confidence:   {:?}", decision.confidence);
            info!("  Time:         {}s elapsed, {}s remaining", seconds_elapsed, seconds_remaining);
            info!("  Price Delta:  ${:+.2}", price_delta);
            info!("═══════════════════════════════════════════════════════════════");

            // Record trade to database
            if let Some(ref db) = trade_db {
                let trade = TradeRecord {
                    market_id: Some(market.condition_id.clone()),
                    window_start,
                    direction: format!("{:?}", direction).to_uppercase(),
                    amount_usdc: decision.bet_amount,
                    entry_price: decision.market_probability,
                    shares: Some(decision.bet_amount / decision.market_probability),
                    time_elapsed_s: seconds_elapsed as i32,
                    price_delta,
                    edge_pct: decision.edge,
                    our_probability: decision.our_probability,
                    market_probability: decision.market_probability,
                    confidence_level: format!("{:?}", decision.confidence),
                    kelly_fraction: Some(config.betting.kelly_fraction),
                    tx_hash: None, // TODO: Set after order execution
                    order_id: None, // TODO: Set after order execution
                };

                if let Err(e) = db.insert_trade(&trade).await {
                    warn!("Failed to record trade: {}", e);
                }
            }

            // Execute the bet via Polymarket CLOB API (FOK order)
            // First, try to acquire Redis lock to prevent duplicate orders across pods
            let lock_id = generate_position_id(); // Unique lock ID for this attempt
            let got_lock = if let Some(ref rs) = redis_state {
                match rs.try_acquire_trade_lock(&lock_id, 5000).await { // 5 second TTL
                    Ok(true) => {
                        debug!("Acquired Redis trade lock");
                        true
                    }
                    Ok(false) => {
                        info!("Another pod is placing an order, skipping");
                        state.set_bet_pending(false);
                        continue;
                    }
                    Err(e) => {
                        warn!("Redis lock error: {}, proceeding anyway", e);
                        true // Proceed if Redis fails
                    }
                }
            } else {
                true // No Redis, proceed
            };

            if let Some(ref mut exec) = order_executor {
                let token_id = match direction {
                    strategy::BetDirection::Up => &market.up_token_id,
                    strategy::BetDirection::Down => &market.down_token_id,
                };

                // Use best_ask for BUY orders (what we actually have to pay)
                // Add 1 cent slippage tolerance to handle price movement during API latency
                const BUY_SLIPPAGE: f64 = 0.01; // 1 cent slippage tolerance
                let (best_ask, ask_liquidity) = match direction {
                    strategy::BetDirection::Up => (up_quote.best_ask, up_quote.ask_liquidity),
                    strategy::BetDirection::Down => (down_quote.best_ask, down_quote.ask_liquidity),
                };
                let execution_price = (best_ask + BUY_SLIPPAGE).min(0.99); // Cap at 99¢

                // Generate position ID before placing order
                let position_id = generate_position_id();

                info!("Placing FOK {} order: ${:.2} at {:.2}¢ (ask={:.2}¢ + {:.0}¢ slippage) (liquidity: {:.2} shares) [{}]",
                    format!("{:?}", direction).to_uppercase(),
                    decision.bet_amount,
                    execution_price * 100.0,
                    best_ask * 100.0,
                    BUY_SLIPPAGE * 100.0,
                    ask_liquidity,
                    &position_id[..8]
                );

                // Create execution record for DB
                let buy_execution = ExecutionRecord {
                    position_id: position_id.clone(),
                    side: "BUY".to_string(),
                    market_slug: Some(market.slug.clone()),
                    token_id: token_id.clone(),
                    direction: format!("{:?}", direction).to_uppercase(),
                    window_start,
                    order_type: "FOK".to_string(),
                    requested_price: execution_price, // Price with slippage
                    requested_amount: decision.bet_amount,
                    requested_shares: Some(decision.bet_amount / execution_price),
                    filled_price: None,
                    filled_amount: None,
                    filled_shares: None,
                    status: "PENDING".to_string(),
                    error_message: None,
                    order_id: None,
                    time_elapsed_s: Some(seconds_elapsed as i32),
                    btc_price: Some(btc_price),
                    btc_delta: Some(price_delta),
                    edge_pct: Some(decision.edge),
                    our_probability: Some(decision.our_probability),
                    market_probability: Some(best_ask), // Original ask (no slippage)
                    best_ask: Some(best_ask), // Original ask (no slippage)
                    best_bid: match direction {
                        strategy::BetDirection::Up => Some(up_quote.best_bid),
                        strategy::BetDirection::Down => Some(down_quote.best_bid),
                    },
                    ask_liquidity: Some(ask_liquidity),
                    bid_liquidity: match direction {
                        strategy::BetDirection::Up => Some(up_quote.bid_liquidity),
                        strategy::BetDirection::Down => Some(down_quote.bid_liquidity),
                    },
                    sell_edge_pct: None,
                    profit_pct: None,
                    entry_price: None,
                };

                // Insert pending execution record
                let exec_id = if let Some(ref db) = trade_db {
                    match db.insert_execution(&buy_execution).await {
                        Ok(id) => Some(id),
                        Err(e) => {
                            warn!("Failed to insert execution record: {}", e);
                            None
                        }
                    }
                } else {
                    None
                };

                // Execute FOK order with liquidity check
                match exec.fok_buy_with_liquidity(
                    token_id,
                    execution_price,
                    decision.bet_amount,
                    ask_liquidity,
                ).await {
                    Ok((response, actual_usdc, actual_shares)) => {
                        // FOK fills immediately and completely, or fails
                        if response.success {
                            info!("✓ FOK Order FILLED! ID: {:?}", response.order_id);
                            info!("  Filled: ${:.2} for {:.4} shares at {:.2}¢",
                                actual_usdc, actual_shares, execution_price * 100.0);

                            // Log trade attempt (success)
                            if let Some(ref db) = trade_db {
                                let attempt = TradeAttempt {
                                    market_slug: Some(market.slug.clone()),
                                    token_id: token_id.clone(),
                                    direction: format!("{:?}", direction).to_uppercase(),
                                    side: "BUY".to_string(),
                                    strategy_type: Some(decision.strategy_type.clone()),
                                    order_type: "FOK".to_string(),
                                    our_probability: Some(decision.our_probability),
                                    market_price: best_ask,
                                    edge: Some(decision.edge),
                                    bet_amount_usdc: actual_usdc,
                                    shares: actual_shares,
                                    slippage_price: Some(execution_price),
                                    btc_price: Some(btc_price),
                                    price_delta: Some(price_delta),
                                    time_elapsed_secs: Some(seconds_elapsed as i32),
                                    time_remaining_secs: Some(seconds_remaining as i32),
                                    success: true,
                                    error_message: None,
                                    order_id: response.order_id.clone(),
                                    time_bucket: Some(time_bucket as i32),
                                    delta_bucket: Some(delta_bucket as i32),
                                };
                                if let Err(e) = db.insert_trade_attempt(&attempt).await {
                                    warn!("Failed to log trade attempt: {}", e);
                                }
                            }

                            // Update execution record with fill info
                            if let (Some(id), Some(ref db)) = (exec_id, &trade_db) {
                                if let Err(e) = db.update_execution_status(
                                    id,
                                    "FILLED",
                                    Some(execution_price),
                                    Some(actual_usdc),
                                    Some(actual_shares),
                                    response.order_id.as_deref(),
                                    None,
                                ).await {
                                    warn!("Failed to update execution status: {}", e);
                                }
                            }

                            // Trigger cooldown
                            state.on_bet_placed(&decision.strategy_type);

                            // Track position - use best_ask as entry price (not slippage-adjusted)
                            // FOK fills at best available price, typically the ask
                            let exit_target = decision.exit_target.unwrap_or(1.0);
                            state.add_position(OpenPosition {
                                position_id: position_id.clone(),
                                token_id: token_id.clone(),
                                direction,
                                entry_price: best_ask, // Use actual ask, not max price with slippage
                                shares: actual_shares,
                                entry_time_bucket: time_bucket,
                                entry_delta_bucket: delta_bucket,
                                exit_target,
                                window_start,
                                sell_pending: false,
                                strategy_type: decision.strategy_type.clone(),
                                entry_seconds_elapsed: seconds_elapsed,
                            });

                            // Sync position to Redis for multi-pod awareness
                            if let Some(ref rs) = redis_state {
                                let redis_pos = RedisPosition {
                                    position_id: position_id.clone(),
                                    token_id: token_id.clone(),
                                    direction: format!("{:?}", direction),
                                    entry_price: best_ask,
                                    shares: actual_shares,
                                    entry_time_bucket: time_bucket,
                                    entry_delta_bucket: delta_bucket,
                                    exit_target,
                                    window_start_ts: window_start.timestamp(),
                                    sell_pending: false,
                                    strategy_type: decision.strategy_type.clone(),
                                    entry_seconds_elapsed: seconds_elapsed,
                                };
                                if let Err(e) = rs.add_position(redis_pos).await {
                                    warn!("Failed to sync position to Redis: {}", e);
                                }
                                // Increment bet counter in Redis
                                if let Err(e) = rs.increment_bet_count(window_start.timestamp(), &decision.strategy_type).await {
                                    warn!("Failed to increment Redis bet counter: {}", e);
                                }
                                // Release trade lock
                                if let Err(e) = rs.release_trade_lock(&lock_id).await {
                                    warn!("Failed to release Redis lock: {}", e);
                                }
                            }

                            if exit_target < 1.0 {
                                info!("  [EXIT] Position {} tracked: {:.2} shares, exit at {:.0}¢",
                                    &position_id[..8], actual_shares, exit_target * 100.0);
                            } else {
                                info!("  [TERMINAL] Position {} tracked: {:.2} shares, hold to settlement",
                                    &position_id[..8], actual_shares);
                            }
                            state.set_bet_pending(false);
                        } else {
                            // FOK rejected (not filled)
                            warn!("✗ FOK Order rejected: {:?}", response.error_msg);

                            // Log trade attempt (rejected)
                            if let Some(ref db) = trade_db {
                                let attempt = TradeAttempt {
                                    market_slug: Some(market.slug.clone()),
                                    token_id: token_id.clone(),
                                    direction: format!("{:?}", direction).to_uppercase(),
                                    side: "BUY".to_string(),
                                    strategy_type: Some(decision.strategy_type.clone()),
                                    order_type: "FOK".to_string(),
                                    our_probability: Some(decision.our_probability),
                                    market_price: best_ask,
                                    edge: Some(decision.edge),
                                    bet_amount_usdc: decision.bet_amount,
                                    shares: decision.bet_amount / execution_price,
                                    slippage_price: Some(execution_price),
                                    btc_price: Some(btc_price),
                                    price_delta: Some(price_delta),
                                    time_elapsed_secs: Some(seconds_elapsed as i32),
                                    time_remaining_secs: Some(seconds_remaining as i32),
                                    success: false,
                                    error_message: response.error_msg.clone(),
                                    order_id: response.order_id.clone(),
                                    time_bucket: Some(time_bucket as i32),
                                    delta_bucket: Some(delta_bucket as i32),
                                };
                                if let Err(e) = db.insert_trade_attempt(&attempt).await {
                                    warn!("Failed to log trade attempt: {}", e);
                                }
                            }

                            // Update execution record with failure
                            if let (Some(id), Some(ref db)) = (exec_id, &trade_db) {
                                if let Err(e) = db.update_execution_status(
                                    id,
                                    "CANCELLED",
                                    None, None, None,
                                    response.order_id.as_deref(),
                                    response.error_msg.as_deref(),
                                ).await {
                                    warn!("Failed to update execution status: {}", e);
                                }
                            }

                            // Release Redis lock on failure
                            if let Some(ref rs) = redis_state {
                                let _ = rs.release_trade_lock(&lock_id).await;
                            }

                            // No cooldown for rejected FOK orders - can retry immediately
                            state.set_bet_pending(false);
                        }
                    }
                    Err(e) => {
                        error!("FOK order execution failed: {}", e);

                        // Release Redis lock on error
                        if let Some(ref rs) = redis_state {
                            let _ = rs.release_trade_lock(&lock_id).await;
                        }

                        // Log trade attempt (error)
                        if let Some(ref db) = trade_db {
                            let attempt = TradeAttempt {
                                market_slug: Some(market.slug.clone()),
                                token_id: token_id.clone(),
                                direction: format!("{:?}", direction).to_uppercase(),
                                side: "BUY".to_string(),
                                strategy_type: Some(decision.strategy_type.clone()),
                                order_type: "FOK".to_string(),
                                our_probability: Some(decision.our_probability),
                                market_price: best_ask,
                                edge: Some(decision.edge),
                                bet_amount_usdc: decision.bet_amount,
                                shares: decision.bet_amount / execution_price,
                                slippage_price: Some(execution_price),
                                btc_price: Some(btc_price),
                                price_delta: Some(price_delta),
                                time_elapsed_secs: Some(seconds_elapsed as i32),
                                time_remaining_secs: Some(seconds_remaining as i32),
                                success: false,
                                error_message: Some(e.to_string()),
                                order_id: None,
                                time_bucket: Some(time_bucket as i32),
                                delta_bucket: Some(delta_bucket as i32),
                            };
                            if let Err(e2) = db.insert_trade_attempt(&attempt).await {
                                warn!("Failed to log trade attempt: {}", e2);
                            }
                        }

                        // Update execution record with error
                        if let (Some(id), Some(ref db)) = (exec_id, &trade_db) {
                            if let Err(e2) = db.update_execution_status(
                                id,
                                "FAILED",
                                None, None, None, None,
                                Some(&e.to_string()),
                            ).await {
                                warn!("Failed to update execution status: {}", e2);
                            }
                        }

                        state.set_bet_pending(false);
                    }
                }
            } else {
                info!("[DRY-RUN] Would place {:?} [{}] order: ${:.2}",
                    direction, decision.strategy_type, decision.bet_amount);
                state.on_bet_placed(&decision.strategy_type);

                // Track position in dry-run mode too
                let execution_price = match direction {
                    BetDirection::Up => up_quote.best_ask,
                    BetDirection::Down => down_quote.best_ask,
                };
                let shares = decision.bet_amount / execution_price;
                let token_id = match direction {
                    BetDirection::Up => market.up_token_id.clone(),
                    BetDirection::Down => market.down_token_id.clone(),
                };

                // Use exit target from decision
                let exit_target = decision.exit_target.unwrap_or(1.0);

                let position_id = generate_position_id();
                state.add_position(OpenPosition {
                    position_id: position_id.clone(),
                    token_id,
                    direction,
                    entry_price: execution_price,
                    shares,
                    entry_time_bucket: time_bucket,
                    entry_delta_bucket: delta_bucket,
                    exit_target,
                    window_start,
                    sell_pending: false,
                    strategy_type: decision.strategy_type.clone(),
                    entry_seconds_elapsed: seconds_elapsed,
                });

                if exit_target < 1.0 {
                    info!("  [DRY-RUN EXIT] Position {}: {:.2} shares at {:.0}¢, exit at {:.0}¢",
                        &position_id[..8], shares, execution_price * 100.0, exit_target * 100.0);
                } else {
                    info!("  [DRY-RUN TERMINAL] Position {}: {:.2} shares at {:.0}¢, hold to settlement",
                        &position_id[..8], shares, execution_price * 100.0);
                }
                state.set_bet_pending(false);
            }
        } else if config.logging.log_skipped_opportunities {
            debug!("No bet: {}", decision.reason);
        }

        // Sleep for remaining interval time
        let elapsed = loop_start.elapsed();
        if elapsed < poll_interval {
            tokio::time::sleep(poll_interval - elapsed).await;
        }

        // Reset open price on window change and record outcome
        let new_window_start = binance::get_current_window_start();
        if new_window_start != window_start {
            // Window ended - calculate and store outcome for settlement logging
            if let Some(open_price) = window_open_price {
                let close_price = btc_price;
                let price_change = close_price - open_price;
                let outcome = if close_price >= open_price { "UP" } else { "DOWN" };

                // Store outcome for next iteration's settlement logging
                last_window_outcome = Some(outcome.to_string());

                info!(
                    "Window {} ended: {} (${:+.2})",
                    window_start.format("%H:%M"),
                    outcome,
                    price_change
                );

                // Record to database if available
                if let Some(ref db) = trade_db {
                    let market_outcome = MarketOutcome {
                        market_id: current_market.as_ref().map(|m| m.condition_id.clone()),
                        window_start,
                        window_end: new_window_start,
                        btc_open_price: open_price,
                        btc_close_price: close_price,
                        btc_high_price: None,
                        btc_low_price: None,
                        outcome: outcome.to_string(),
                        price_change,
                        price_change_pct: price_change / open_price * 100.0,
                    };

                    if let Err(e) = db.insert_outcome(&market_outcome).await {
                        warn!("Failed to record market outcome: {}", e);
                    }

                    // Calculate trade results for any completed trades
                    if let Err(e) = db.calculate_trade_results().await {
                        warn!("Failed to calculate trade results: {}", e);
                    }
                }
            }

            window_open_price = None;

            // Fetch new market for the new window
            match polymarket.get_current_btc_15m_market().await {
                Ok(m) => {
                    info!("New market: {}", m.slug);
                    // Update WebSocket shared state with new tokens (CRITICAL!)
                    {
                        let mut ms = market_state.write().await;
                        ms.market_slug = m.slug.clone();
                        ms.up_token_id = m.up_token_id.clone();
                        ms.down_token_id = m.down_token_id.clone();
                        // Reset order book prices for new market
                        ms.up_best_ask = 0.0;
                        ms.up_best_bid = 0.0;
                        ms.down_best_ask = 0.0;
                        ms.down_best_bid = 0.0;
                        info!("Updated WebSocket state with new tokens");
                    }
                    current_market = Some(m);
                }
                Err(e) => {
                    warn!("Failed to fetch new market: {}", e);
                    current_market = None;
                }
            }
        }
    }

    // Final stats from database
    if let Some(ref db) = trade_db {
        match db.get_overall_stats().await {
            Ok(stats) => {
                info!("");
                info!("═══════════════════════════════════════════════════════════════");
                info!("SESSION STATS (from database)");
                info!("  Total trades:   {}", stats.total_trades);
                info!("  Wins:           {}", stats.wins);
                info!("  Win rate:       {:.1}%", if stats.total_trades > 0 { stats.wins as f64 / stats.total_trades as f64 * 100.0 } else { 0.0 });
                info!("  Total wagered:  ${:.2}", stats.total_wagered);
                info!("  Net P&L:        ${:+.2}", stats.net_pnl);
                info!("═══════════════════════════════════════════════════════════════");
            }
            Err(e) => warn!("Failed to get stats: {}", e),
        }
    }

    info!("");
    info!("Bot stopped.");
    info!("Final bankroll: ${:.2}", state.bankroll);
    info!("Daily P&L: ${:+.2}", state.daily_pnl);

    Ok(())
}
