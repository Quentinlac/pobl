//! BTC 15-Minute Market Data Logger
//!
//! Continuously logs order book data and calculated edges every 200ms
//! to PostgreSQL for analysis.

// Include modules from the library
#[path = "../models.rs"]
mod models;
#[path = "../stats.rs"]
mod stats;

#[path = "../bot/binance.rs"]
mod binance;
#[path = "../bot/polymarket.rs"]
mod polymarket;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use models::{delta_to_bucket, ProbabilityMatrix};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::Client;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Database configuration (same as main bot)
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

/// Connect to PostgreSQL
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
            error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

/// Run migrations for market_logs table
async fn run_migrations(client: &Client) -> Result<()> {
    let migration = include_str!("../../migrations/003_market_logs.sql");
    client.batch_execute(migration).await?;
    info!("Database migrations complete");
    Ok(())
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

/// Market log entry to insert
struct MarketLogEntry {
    timestamp: DateTime<Utc>,
    market_slug: String,
    up_token_id: String,
    down_token_id: String,
    price_up: Decimal,
    price_down: Decimal,
    size_up: Decimal,
    size_down: Decimal,
    edge_up: Option<Decimal>,
    edge_down: Option<Decimal>,
    btc_price: Decimal,
    time_elapsed: i32,
    price_delta: Decimal,
    error_message: Option<String>,
}

/// Insert a market log entry into the database
async fn insert_log(client: &Client, entry: &MarketLogEntry) -> Result<()> {
    client
        .execute(
            r#"
            INSERT INTO market_logs (
                timestamp, market_slug, up_token_id, down_token_id,
                price_up, price_down, size_up, size_down,
                edge_up, edge_down, btc_price, time_elapsed, price_delta, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
            &[
                &entry.timestamp,
                &entry.market_slug,
                &entry.up_token_id,
                &entry.down_token_id,
                &entry.price_up,
                &entry.price_down,
                &entry.size_up,
                &entry.size_down,
                &entry.edge_up,
                &entry.edge_down,
                &entry.btc_price,
                &entry.time_elapsed,
                &entry.price_delta,
                &entry.error_message,
            ],
        )
        .await?;

    Ok(())
}

/// Calculate edges from the probability matrix
fn calculate_edges(
    matrix: &ProbabilityMatrix,
    time_elapsed: u32,
    price_delta: f64,
    market_price_up: f64,
    market_price_down: f64,
) -> (Option<f64>, Option<f64>) {
    // Get matrix cell for current situation (15-second intervals)
    let time_bucket = (time_elapsed / 15).min(59) as u8;
    let delta_bucket = delta_to_bucket(
        Decimal::try_from(price_delta).unwrap_or_default()
    );

    let cell = matrix.get(time_bucket, delta_bucket);

    // Check sample size - must match bot's timing.min_samples_in_bucket (30)
    if cell.total() < 30 {
        return (None, None);
    }

    // Use Wilson lower bound for conservative probability estimate
    let our_p_up = cell.p_up_wilson_lower;
    let our_p_down = 1.0 - cell.p_up_wilson_upper;

    // Calculate edges
    let edge_up = if market_price_up > 0.01 {
        Some((our_p_up - market_price_up) / market_price_up)
    } else {
        None
    };

    let edge_down = if market_price_down > 0.01 {
        Some((our_p_down - market_price_down) / market_price_down)
    } else {
        None
    };

    (edge_up, edge_down)
}

/// State for tracking current window
struct WindowState {
    window_start: DateTime<Utc>,
    window_open_price: f64,
    market_slug: String,
    up_token_id: String,
    down_token_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,btc_logger=debug")),
        )
        .init();

    info!("Starting BTC 15-Minute Market Logger");

    // Set up shutdown signal
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        r.store(false, Ordering::SeqCst);
    });

    // Connect to database
    let db_config = DbConfig::default();
    let db_client = connect_db(&db_config).await
        .context("Failed to connect to database")?;
    info!("Connected to PostgreSQL");

    // Run migrations
    run_migrations(&db_client).await?;

    // Load probability matrix
    let matrix = load_matrix_from_file()?;
    info!("Loaded probability matrix with {} windows", matrix.total_windows);

    // Create API clients
    let binance_client = binance::BinanceClient::new(5000)
        .context("Failed to create Binance client")?;
    let polymarket_client = polymarket::PolymarketClient::new(5000)
        .context("Failed to create Polymarket client")?;

    info!("API clients initialized");

    // Track current window state
    let mut window_state: Option<WindowState> = None;
    let mut log_count: u64 = 0;
    let mut error_count: u64 = 0;

    // Main logging loop - 200ms interval
    let interval = Duration::from_millis(200);

    while running.load(Ordering::SeqCst) {
        let loop_start = std::time::Instant::now();
        let timestamp = Utc::now();

        // Check if we need to refresh window state (new 15-minute window)
        let current_window_start = binance::get_current_window_start();
        let needs_refresh = window_state
            .as_ref()
            .map(|s| s.window_start != current_window_start)
            .unwrap_or(true);

        if needs_refresh {
            info!("Fetching new window data for {:?}", current_window_start);

            // Fetch market info
            match polymarket_client.get_current_btc_15m_market().await {
                Ok(market) => {
                    // Fetch window open price
                    match binance_client.get_window_open_price(current_window_start).await {
                        Ok(open_price) => {
                            window_state = Some(WindowState {
                                window_start: current_window_start,
                                window_open_price: open_price,
                                market_slug: market.slug,
                                up_token_id: market.up_token_id,
                                down_token_id: market.down_token_id,
                            });
                            info!(
                                "New window: slug={}, open_price={:.2}",
                                window_state.as_ref().unwrap().market_slug,
                                open_price
                            );
                        }
                        Err(e) => {
                            warn!("Failed to get window open price: {}", e);
                            // Log error entry
                            let entry = MarketLogEntry {
                                timestamp,
                                market_slug: market.slug,
                                up_token_id: market.up_token_id,
                                down_token_id: market.down_token_id,
                                price_up: Decimal::ZERO,
                                price_down: Decimal::ZERO,
                                size_up: Decimal::ZERO,
                                size_down: Decimal::ZERO,
                                edge_up: None,
                                edge_down: None,
                                btc_price: Decimal::ZERO,
                                time_elapsed: 0,
                                price_delta: Decimal::ZERO,
                                error_message: Some(format!("Failed to get open price: {}", e)),
                            };
                            if let Err(e) = insert_log(&db_client, &entry).await {
                                error!("Failed to insert error log: {}", e);
                            }
                            error_count += 1;
                            tokio::time::sleep(interval).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get market info: {}", e);
                    // Log error entry with minimal data
                    let entry = MarketLogEntry {
                        timestamp,
                        market_slug: "unknown".to_string(),
                        up_token_id: "unknown".to_string(),
                        down_token_id: "unknown".to_string(),
                        price_up: Decimal::ZERO,
                        price_down: Decimal::ZERO,
                        size_up: Decimal::ZERO,
                        size_down: Decimal::ZERO,
                        edge_up: None,
                        edge_down: None,
                        btc_price: Decimal::ZERO,
                        time_elapsed: 0,
                        price_delta: Decimal::ZERO,
                        error_message: Some(format!("Failed to get market: {}", e)),
                    };
                    if let Err(e) = insert_log(&db_client, &entry).await {
                        error!("Failed to insert error log: {}", e);
                    }
                    error_count += 1;
                    tokio::time::sleep(interval).await;
                    continue;
                }
            }
        }

        // Get current window state
        let state = window_state.as_ref().unwrap();

        // Fetch data concurrently
        let (btc_result, up_book_result, down_book_result) = tokio::join!(
            binance_client.get_btc_price(),
            polymarket_client.get_order_book(&state.up_token_id),
            polymarket_client.get_order_book(&state.down_token_id)
        );

        // Process results
        let mut error_message: Option<String> = None;
        let mut btc_price_f64 = 0.0_f64;
        let mut price_up_f64 = 0.0_f64;
        let mut price_down_f64 = 0.0_f64;
        let mut size_up_f64 = 0.0_f64;
        let mut size_down_f64 = 0.0_f64;

        // BTC price
        match btc_result {
            Ok(price) => btc_price_f64 = price.price,
            Err(e) => {
                error_message = Some(format!("BTC price error: {}", e));
            }
        }

        // UP order book
        match up_book_result {
            Ok(book) => {
                // Best ask = lowest ask price (price to buy)
                if let Some(best_ask) = book.asks.first() {
                    price_up_f64 = best_ask.price.parse().unwrap_or(0.0);
                    size_up_f64 = best_ask.size.parse().unwrap_or(0.0);
                }
            }
            Err(e) => {
                let msg = format!("UP book error: {}", e);
                error_message = Some(match error_message {
                    Some(existing) => format!("{}; {}", existing, msg),
                    None => msg,
                });
            }
        }

        // DOWN order book
        match down_book_result {
            Ok(book) => {
                // Best ask = lowest ask price (price to buy)
                if let Some(best_ask) = book.asks.first() {
                    price_down_f64 = best_ask.price.parse().unwrap_or(0.0);
                    size_down_f64 = best_ask.size.parse().unwrap_or(0.0);
                }
            }
            Err(e) => {
                let msg = format!("DOWN book error: {}", e);
                error_message = Some(match error_message {
                    Some(existing) => format!("{}; {}", existing, msg),
                    None => msg,
                });
            }
        }

        // Calculate derived values
        let time_elapsed = binance::get_seconds_elapsed() as i32;
        let price_delta_f64 = btc_price_f64 - state.window_open_price;

        // Calculate edges (only if we have valid data)
        let (edge_up_f64, edge_down_f64) = if btc_price_f64 > 0.0 && price_up_f64 > 0.0 && price_down_f64 > 0.0 {
            calculate_edges(&matrix, time_elapsed as u32, price_delta_f64, price_up_f64, price_down_f64)
        } else {
            (None, None)
        };

        // Convert to Decimal for database
        let f64_to_dec = |v: f64| Decimal::try_from(v).unwrap_or_default();

        // Create log entry
        let entry = MarketLogEntry {
            timestamp,
            market_slug: state.market_slug.clone(),
            up_token_id: state.up_token_id.clone(),
            down_token_id: state.down_token_id.clone(),
            price_up: f64_to_dec(price_up_f64),
            price_down: f64_to_dec(price_down_f64),
            size_up: f64_to_dec(size_up_f64),
            size_down: f64_to_dec(size_down_f64),
            edge_up: edge_up_f64.map(f64_to_dec),
            edge_down: edge_down_f64.map(f64_to_dec),
            btc_price: f64_to_dec(btc_price_f64),
            time_elapsed,
            price_delta: f64_to_dec(price_delta_f64),
            error_message: error_message.clone(),
        };

        // Insert into database
        match insert_log(&db_client, &entry).await {
            Ok(_) => {
                log_count += 1;
                if error_message.is_some() {
                    error_count += 1;
                }
            }
            Err(e) => {
                error!("Failed to insert log: {}", e);
                error_count += 1;
            }
        }

        // Periodic status log (every 100 entries = ~20 seconds)
        if log_count % 100 == 0 {
            info!(
                "Logged {} entries ({} errors) | BTC=${:.0} | UP={:.4} ({:.0}) DOWN={:.4} ({:.0}) | Edge: UP={:+.2}% DOWN={:+.2}%",
                log_count,
                error_count,
                btc_price_f64,
                price_up_f64,
                size_up_f64,
                price_down_f64,
                size_down_f64,
                edge_up_f64.unwrap_or(0.0) * 100.0,
                edge_down_f64.unwrap_or(0.0) * 100.0
            );
        }

        // Sleep for remaining interval time
        let elapsed = loop_start.elapsed();
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }

    info!("Logger shutdown. Total entries: {}, errors: {}", log_count, error_count);
    Ok(())
}
