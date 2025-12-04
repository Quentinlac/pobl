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
mod polymarket;
mod strategy;

use anyhow::{Context, Result};
use config::BotConfig;
use models::ProbabilityMatrix;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strategy::StrategyContext;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

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
        }
    }

    fn on_new_window(&mut self, window_start: chrono::DateTime<chrono::Utc>) {
        if self.current_window_start != Some(window_start) {
            info!("New 15-minute window started: {}", window_start);
            self.current_window_start = Some(window_start);
            self.bets_this_window = 0;
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

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

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

    // Load probability matrix
    let matrix_path = std::env::var("MATRIX_PATH")
        .unwrap_or_else(|_| "output/matrix.json".to_string());
    let matrix_path = PathBuf::from(&matrix_path);

    if !matrix_path.exists() {
        error!("Probability matrix not found: {}", matrix_path.display());
        error!("Run 'cargo run -- build' first to generate the matrix");
        return Ok(());
    }

    info!("Loading probability matrix from: {}", matrix_path.display());
    let matrix_json = std::fs::read_to_string(&matrix_path)
        .context("Failed to read matrix file")?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&matrix_json)
        .context("Failed to parse matrix JSON")?;
    info!("Matrix loaded: {} windows analyzed", matrix.total_windows);

    // Get initial bankroll from environment
    let initial_bankroll: f64 = std::env::var("BOT_BANKROLL")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .unwrap_or(1000.0);
    info!("Starting bankroll: ${:.2}", initial_bankroll);

    // Check required environment variables
    let up_token = std::env::var("POLYMARKET_BTC_UP_TOKEN")
        .unwrap_or_else(|_| config.markets.btc_up_token_id.clone());
    let down_token = std::env::var("POLYMARKET_BTC_DOWN_TOKEN")
        .unwrap_or_else(|_| config.markets.btc_down_token_id.clone());

    if up_token.is_empty() || down_token.is_empty() {
        warn!("Market token IDs not configured!");
        warn!("Set POLYMARKET_BTC_UP_TOKEN and POLYMARKET_BTC_DOWN_TOKEN");
        warn!("Running in DRY-RUN mode (no actual bets)");
    }

    // Initialize clients
    let polymarket = polymarket::PolymarketClient::new(config.polling.request_timeout_ms)?;
    let binance = binance::BinanceClient::new(config.polling.request_timeout_ms)?;

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

        // Skip if tokens not configured (dry run mode)
        if up_token.is_empty() || down_token.is_empty() {
            // Still show what we would do
            let time_bucket = (seconds_elapsed / 30).min(29) as u8;
            let delta_bucket = models::delta_to_bucket(
                rust_decimal::Decimal::try_from(price_delta).unwrap_or_default()
            );
            let cell = matrix.get(time_bucket, delta_bucket);

            if seconds_elapsed >= config.timing.min_seconds_elapsed
                && seconds_remaining >= config.timing.min_seconds_remaining
                && cell.total() >= config.timing.min_samples_in_bucket
            {
                debug!(
                    "[DRY-RUN] Cell[t={}, d={}]: P(UP)={:.1}%, n={}, {:?}",
                    time_bucket, delta_bucket,
                    cell.p_up * 100.0, cell.total(), cell.confidence_level
                );
            }

            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // Get order books
        let (up_book, down_book) = match polymarket.get_both_books(&up_token, &down_token).await {
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

        if config.logging.log_order_book {
            debug!(
                "Order books - UP: bid={:.3} ask={:.3} | DOWN: bid={:.3} ask={:.3}",
                up_quote.best_bid, up_quote.best_ask,
                down_quote.best_bid, down_quote.best_ask
            );
        }

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

        if decision.should_bet {
            info!("═══════════════════════════════════════════════════════════════");
            info!("BET SIGNAL DETECTED!");
            info!("  Direction:    {:?}", decision.direction.unwrap());
            info!("  Edge:         {:.2}%", decision.edge * 100.0);
            info!("  Our P:        {:.2}%", decision.our_probability * 100.0);
            info!("  Market P:     {:.2}%", decision.market_probability * 100.0);
            info!("  Bet Amount:   ${:.2}", decision.bet_amount);
            info!("  Confidence:   {:?}", decision.confidence);
            info!("  Time:         {}s elapsed, {}s remaining", seconds_elapsed, seconds_remaining);
            info!("  Price Delta:  ${:+.2}", price_delta);
            info!("═══════════════════════════════════════════════════════════════");

            // TODO: Execute the bet via Polymarket CLOB API
            // For now, just log and track
            state.on_bet_placed();

            // Simulate: In production, you would call the order execution here
            // let result = executor.place_order(decision).await?;
        } else if config.logging.log_skipped_opportunities {
            debug!("No bet: {}", decision.reason);
        }

        // Sleep for remaining interval time
        let elapsed = loop_start.elapsed();
        if elapsed < poll_interval {
            tokio::time::sleep(poll_interval - elapsed).await;
        }

        // Reset open price on window change
        let new_window_start = binance::get_current_window_start();
        if new_window_start != window_start {
            window_open_price = None;
        }
    }

    info!("");
    info!("Bot stopped.");
    info!("Final bankroll: ${:.2}", state.bankroll);
    info!("Daily P&L: ${:+.2}", state.daily_pnl);

    Ok(())
}
