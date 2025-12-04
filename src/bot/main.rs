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

mod binance;
mod config;
mod db;
mod executor;
mod polymarket;
mod strategy;

use anyhow::{Context, Result};
use clap::Parser;
use config::BotConfig;
use db::{MarketOutcome, TradeDb, TradeRecord};
use models::ProbabilityMatrix;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strategy::StrategyContext;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

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

/// Bot state tracking
struct BotState {
    bankroll: f64,
    consecutive_losses: u32,
    consecutive_wins: u32,
    bets_this_window: u32,
    daily_pnl: f64,
    open_positions: u32,
    current_window_start: Option<chrono::DateTime<chrono::Utc>>,
    last_bet_time: Option<chrono::DateTime<chrono::Utc>>,
    last_log_time: Option<std::time::Instant>,
    market_fetch_logged: bool,
}

impl BotState {
    fn new(initial_bankroll: f64) -> Self {
        Self {
            bankroll: initial_bankroll,
            consecutive_losses: 0,
            consecutive_wins: 0,
            bets_this_window: 0,
            daily_pnl: 0.0,
            open_positions: 0,
            current_window_start: None,
            last_bet_time: None,
            last_log_time: None,
            market_fetch_logged: false,
        }
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

    fn seconds_since_last_bet(&self) -> Option<u32> {
        self.last_bet_time.map(|t| {
            (chrono::Utc::now() - t).num_seconds().max(0) as u32
        })
    }

    fn on_new_window(&mut self, window_start: chrono::DateTime<chrono::Utc>) {
        if self.current_window_start != Some(window_start) {
            info!("═══ New 15-minute window: {} ═══", window_start.format("%H:%M:%S UTC"));
            // Close positions from the previous window (they resolved when window ended)
            self.open_positions = self.open_positions.saturating_sub(self.bets_this_window);
            self.current_window_start = Some(window_start);
            self.bets_this_window = 0;
            self.market_fetch_logged = false;
        }
    }

    fn on_bet_placed(&mut self) {
        self.bets_this_window += 1;
        self.open_positions += 1;
        self.last_bet_time = Some(chrono::Utc::now());
    }

    fn on_win(&mut self, amount: f64, config: &BotConfig) {
        self.daily_pnl += amount;
        self.bankroll += amount;
        self.consecutive_wins += 1;
        self.consecutive_losses = 0;
        self.open_positions = self.open_positions.saturating_sub(1);

        if self.consecutive_wins >= config.risk.consecutive_wins_to_reset {
            info!("Resetting loss reduction after {} consecutive wins", self.consecutive_wins);
        }
    }

    fn on_loss(&mut self, amount: f64) {
        self.daily_pnl -= amount;
        self.bankroll -= amount;
        self.consecutive_losses += 1;
        self.consecutive_wins = 0;
        self.open_positions = self.open_positions.saturating_sub(1);

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
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging
    let log_filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info,btc_bot=debug".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&log_filter))
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

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

    // Get initial bankroll from environment
    let initial_bankroll: f64 = std::env::var("BOT_BANKROLL")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .unwrap_or(1000.0);
    info!("Starting bankroll: ${:.2}", initial_bankroll);

    // Initialize clients
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

    // Initialize state
    let mut state = BotState::new(initial_bankroll);

    // Set up graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received, stopping bot...");
        r.store(false, Ordering::SeqCst);
    });

    info!("");
    info!("Bot started! Polling every {}ms", config.polling.interval_ms);
    info!("Press Ctrl+C to stop");
    info!("");

    // Main loop
    let poll_interval = Duration::from_millis(config.polling.interval_ms);
    let mut window_open_price: Option<f64> = None;

    while running.load(Ordering::SeqCst) {
        let loop_start = std::time::Instant::now();

        // Get current window info
        let window_start = binance::get_current_window_start();
        let seconds_elapsed = binance::get_seconds_elapsed();
        let seconds_remaining = binance::get_seconds_remaining();

        // Check for new window
        state.on_new_window(window_start);

        // Reset open price on new window
        if state.current_window_start == Some(window_start) && window_open_price.is_none() {
            match binance.get_window_open_price(window_start).await {
                Ok(price) => {
                    window_open_price = Some(price);
                    info!("Window open price: ${:.2}", price);
                }
                Err(e) => {
                    warn!("Failed to get window open price: {}", e);
                }
            }
        }

        // Get current BTC price
        let current_price = match binance.get_btc_price().await {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to get BTC price: {}", e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        // Calculate price delta
        let price_delta = match window_open_price {
            Some(open) => current_price.price - open,
            None => {
                debug!("Waiting for window open price...");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if config.logging.log_price_checks {
            debug!(
                "BTC: ${:.2} | Delta: ${:+.2} | Time: {}s / {}s remaining",
                current_price.price, price_delta, seconds_elapsed, seconds_remaining
            );
        }

        // Get current market tokens (refresh if needed)
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
                                current_price.price, price_delta, seconds_elapsed,
                                cell.p_up * 100.0, cell.total(), e
                            );
                        }
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                }
            }
        };

        // Get order books
        let (up_book, down_book) = match polymarket.get_both_books(&market.up_token_id, &market.down_token_id).await {
            Ok(books) => books,
            Err(e) => {
                warn!("Failed to get order books: {}", e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        let up_quote = match polymarket.get_price_quote(&up_book) {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to parse UP order book: {}", e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        let down_quote = match polymarket.get_price_quote(&down_book) {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to parse DOWN order book: {}", e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        // Make decision
        let ctx = StrategyContext {
            config: &config,
            matrix: &matrix,
            bankroll: state.bankroll,
            consecutive_losses: state.consecutive_losses,
            bets_this_window: state.bets_this_window,
            daily_pnl: state.daily_pnl,
            open_positions: state.open_positions,
        };

        let decision = ctx.decide(seconds_elapsed, price_delta, &up_quote, &down_quote);

        // Get matrix cell info for logging
        let time_bucket = (seconds_elapsed / 30).min(29) as u8;
        let delta_bucket = models::delta_to_bucket(
            rust_decimal::Decimal::try_from(price_delta).unwrap_or_default()
        );
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
                current_price.price,
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

        // Check cooldown before betting
        let in_cooldown = match state.seconds_since_last_bet() {
            Some(secs) if secs < config.cooldown.min_seconds_between_bets => {
                if decision.should_bet {
                    debug!("Cooldown: {}s since last bet, need {}s", secs, config.cooldown.min_seconds_between_bets);
                }
                true
            }
            _ => false,
        };

        if decision.should_bet && !in_cooldown {
            let direction = decision.direction.unwrap();
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

            // Execute the bet via Polymarket CLOB API
            if let Some(ref mut exec) = order_executor {
                let token_id = match direction {
                    strategy::BetDirection::Up => &market.up_token_id,
                    strategy::BetDirection::Down => &market.down_token_id,
                };

                // Use best_ask for BUY orders (what we actually have to pay)
                let execution_price = match direction {
                    strategy::BetDirection::Up => up_quote.best_ask,
                    strategy::BetDirection::Down => down_quote.best_ask,
                };

                info!("Placing {} order: ${:.2} at {:.2}¢ (ask price)",
                    format!("{:?}", direction).to_uppercase(),
                    decision.bet_amount,
                    execution_price * 100.0
                );

                match exec.market_buy(
                    token_id,
                    execution_price,
                    decision.bet_amount,
                ).await {
                    Ok(response) => {
                        if response.success {
                            info!("✓ Order submitted! ID: {:?}", response.order_id);
                            state.on_bet_placed();

                            // Update trade record with order ID
                            if let Some(ref db) = trade_db {
                                if let Some(order_id) = &response.order_id {
                                    // TODO: Update the trade record with order_id
                                    debug!("Trade recorded with order ID: {}", order_id);
                                }
                            }

                            // Wait 1 second then check status and cancel if still open
                            if let Some(order_id) = &response.order_id {
                                let order_id_clone = order_id.clone();
                                tokio::time::sleep(Duration::from_secs(1)).await;

                                // Check order status first
                                match exec.get_order(&order_id_clone).await {
                                    Ok(order_details) => {
                                        use executor::OrderStatus;
                                        match order_details.status {
                                            OrderStatus::Filled => {
                                                info!("✓ Order FILLED: {} (matched: {})",
                                                    order_id_clone, order_details.size_matched);
                                            }
                                            OrderStatus::Open => {
                                                // Still open after 1s, cancel it
                                                match exec.cancel_order(&order_id_clone).await {
                                                    Ok(_) => {
                                                        info!("✗ Order cancelled (unfilled after 1s): {}", order_id_clone);
                                                    }
                                                    Err(e) => {
                                                        warn!("Failed to cancel order: {}", e);
                                                    }
                                                }
                                            }
                                            OrderStatus::Cancelled => {
                                                info!("Order already cancelled: {}", order_id_clone);
                                            }
                                            OrderStatus::Unknown => {
                                                warn!("Unknown order status for {}", order_id_clone);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // Can't get status, try to cancel anyway
                                        warn!("Failed to get order status: {}, attempting cancel", e);
                                        let _ = exec.cancel_order(&order_id_clone).await;
                                    }
                                }
                            }
                        } else {
                            warn!("Order rejected: {:?}", response.error_msg);
                        }
                    }
                    Err(e) => {
                        error!("Order execution failed: {}", e);
                    }
                }
            } else {
                info!("[DRY-RUN] Would place {:?} order: ${:.2}", direction, decision.bet_amount);
                state.on_bet_placed();
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
            // Window ended - record outcome
            if let (Some(ref db), Some(open_price)) = (&trade_db, window_open_price) {
                let close_price = current_price.price;
                let price_change = close_price - open_price;
                let outcome = if close_price >= open_price { "UP" } else { "DOWN" };

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

                info!(
                    "Window {} ended: {} (${:+.2})",
                    window_start.format("%H:%M"),
                    outcome,
                    price_change
                );
            }

            window_open_price = None;

            // Fetch new market for the new window
            match polymarket.get_current_btc_15m_market().await {
                Ok(m) => {
                    info!("New market: {}", m.slug);
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
