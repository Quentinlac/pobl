mod chainlink;
mod db;
mod edge;
mod models;
mod output;
mod processor;
mod stats;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::db::DbConfig;
use crate::models::{FirstPassageMatrix, ProbabilityMatrix};

#[derive(Parser)]
#[command(name = "btc-probability-matrix")]
#[command(about = "Analyze Binance BTC prices for Polymarket betting edge detection")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build the probability matrix from historical data
    Build {
        /// Output directory for generated files
        #[arg(short, long, default_value = "output")]
        output_dir: PathBuf,
    },

    /// Query the probability for a specific situation
    Query {
        /// Time elapsed in seconds since window start (0-899)
        #[arg(short, long)]
        time_elapsed: u32,

        /// Current price delta from window open (in dollars)
        #[arg(short, long)]
        price_delta: f64,

        /// Polymarket price for UP outcome (e.g., 0.45 for 45 cents)
        #[arg(short = 'm', long)]
        market_price: f64,

        /// Your bankroll in dollars (for Kelly sizing)
        #[arg(short, long, default_value = "1000")]
        bankroll: f64,
    },

    /// Show summary statistics
    Stats {
        /// Path to saved matrix JSON
        #[arg(short, long)]
        matrix_path: Option<PathBuf>,
    },

    /// Query first-passage probabilities (for exit strategy)
    FirstPassage {
        /// Time elapsed in seconds since window start (0-899)
        #[arg(short, long)]
        time_elapsed: u32,

        /// Current price delta from window open (in dollars)
        #[arg(short, long)]
        price_delta: f64,

        /// Your current position direction (up or down)
        #[arg(short = 'd', long)]
        direction: String,

        /// Your entry price (e.g., 0.30 for 30 cents)
        #[arg(short, long)]
        entry_price: f64,
    },

    /// Fetch Chainlink BTC/USD price data
    Chainlink {
        #[command(subcommand)]
        action: ChainlinkAction,
    },
}

#[derive(Subcommand)]
enum ChainlinkAction {
    /// Fetch historical data from Chainlink oracle
    Fetch {
        /// Number of days of history to fetch (default: 180 = 6 months)
        #[arg(short, long, default_value = "180")]
        days: u32,
    },

    /// Fetch only new data since last update
    Update,

    /// Show statistics about stored Chainlink data
    Stats,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Build { output_dir } => {
            build_matrix(output_dir).await?;
        }
        Commands::Query {
            time_elapsed,
            price_delta,
            market_price,
            bankroll,
        } => {
            query_probability(time_elapsed, price_delta, market_price, bankroll).await?;
        }
        Commands::Stats { matrix_path } => {
            show_stats(matrix_path).await?;
        }
        Commands::FirstPassage {
            time_elapsed,
            price_delta,
            direction,
            entry_price,
        } => {
            query_first_passage(time_elapsed, price_delta, &direction, entry_price).await?;
        }
        Commands::Chainlink { action } => {
            handle_chainlink(action).await?;
        }
    }

    Ok(())
}

async fn build_matrix(output_dir: PathBuf) -> Result<()> {
    println!("🔌 Connecting to database...");
    let config = DbConfig::default();
    let client = db::connect(&config).await?;

    // Run migrations for matrix snapshots table
    println!("📋 Running migrations...");
    db::run_matrix_migrations(&client).await?;

    // Get data range info
    let (start, end) = db::get_data_range(&client).await?;
    let count = db::get_price_count(&client).await?;
    println!("📊 Data range: {} to {}", start, end);
    println!("📊 Total records: {}", count);

    println!("\n📥 Fetching price data (this may take a moment)...");
    let prices = db::fetch_all_prices(&client).await?;
    println!("✅ Fetched {} price points", prices.len());

    println!("\n🔄 Processing into 15-minute windows...");
    let windows = processor::process_into_windows(&prices);
    println!("✅ Created {} windows", windows.len());

    println!("\n📈 Building probability matrix...");
    let mut matrix = ProbabilityMatrix::new();
    processor::populate_matrix(&windows, &mut matrix);

    // Compute statistics for all cells
    println!("📊 Computing statistical significance...");
    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
            let cell = matrix.get_mut(time_bucket, delta_bucket);
            stats::compute_cell_stats(cell);
        }
    }

    // Build First-Passage Matrix
    println!("\n📈 Building first-passage matrix...");
    let mut fp_matrix = FirstPassageMatrix::new();
    processor::populate_first_passage_matrix(&windows, &mut fp_matrix);

    // Compute first-passage statistics
    println!("📊 Computing first-passage statistics...");
    stats::compute_first_passage_matrix_stats(&mut fp_matrix);

    // Create output directory
    std::fs::create_dir_all(&output_dir)?;

    // Export Terminal Probability Matrix files
    let json_path = output_dir.join("matrix.json");
    let csv_path = output_dir.join("matrix.csv");
    let report_path = output_dir.join("report.txt");

    println!("\n💾 Exporting terminal probability results...");

    output::export_to_json(&matrix, &json_path)?;
    println!("  ✅ JSON: {}", json_path.display());

    output::export_to_csv(&matrix, &csv_path)?;
    println!("  ✅ CSV: {}", csv_path.display());

    let report = output::generate_report(&matrix);
    std::fs::write(&report_path, &report)?;
    println!("  ✅ Report: {}", report_path.display());

    // Export First-Passage Matrix files
    let fp_json_path = output_dir.join("first_passage_matrix.json");
    let fp_csv_path = output_dir.join("first_passage_matrix.csv");
    let fp_report_path = output_dir.join("first_passage_report.txt");

    println!("\n💾 Exporting first-passage results...");

    output::export_first_passage_to_json(&fp_matrix, &fp_json_path)?;
    println!("  ✅ JSON: {}", fp_json_path.display());

    output::export_first_passage_to_csv(&fp_matrix, &fp_csv_path)?;
    println!("  ✅ CSV: {}", fp_csv_path.display());

    let fp_report = output::generate_first_passage_report(&fp_matrix);
    std::fs::write(&fp_report_path, &fp_report)?;
    println!("  ✅ Report: {}", fp_report_path.display());

    // Build Price Reach Matrix (uses terminal matrix)
    println!("\n📈 Building price reach matrix...");
    let price_reach_matrix = processor::build_price_reach_matrix(&windows, &matrix);

    // Export Price Reach Matrix files
    let pr_json_path = output_dir.join("price_reach_matrix.json");
    let pr_csv_path = output_dir.join("price_reach_matrix.csv");
    let pr_report_path = output_dir.join("price_reach_report.txt");

    println!("\n💾 Exporting price reach results...");

    output::export_price_reach_to_json(&price_reach_matrix, &pr_json_path)?;
    println!("  ✅ JSON: {}", pr_json_path.display());

    output::export_price_reach_to_csv(&price_reach_matrix, &pr_csv_path)?;
    println!("  ✅ CSV: {}", pr_csv_path.display());

    let pr_report = output::generate_price_reach_report(&price_reach_matrix);
    std::fs::write(&pr_report_path, &pr_report)?;
    println!("  ✅ Report: {}", pr_report_path.display());

    // Build Price Crossing Matrix (uses terminal matrix)
    println!("\n📈 Building price crossing matrix...");
    let crossing_matrix = processor::build_price_crossing_matrix(&windows, &matrix);

    // Export Price Crossing Matrix files
    let pc_json_path = output_dir.join("price_crossing_matrix.json");
    let pc_csv_path = output_dir.join("price_crossing_matrix.csv");
    let pc_report_path = output_dir.join("price_crossing_report.txt");

    println!("\n💾 Exporting price crossing results...");

    output::export_price_crossing_to_json(&crossing_matrix, &pc_json_path)?;
    println!("  ✅ JSON: {}", pc_json_path.display());

    output::export_price_crossing_to_csv(&crossing_matrix, &pc_csv_path)?;
    println!("  ✅ CSV: {}", pc_csv_path.display());

    let pc_report = output::generate_price_crossing_report(&crossing_matrix);
    std::fs::write(&pc_report_path, &pc_report)?;
    println!("  ✅ Report: {}", pc_report_path.display());

    // Print summaries
    println!("\n{}", report);
    output::print_matrix_summary(&matrix);

    println!("\n{}", fp_report);

    println!("\n{}", pr_report);

    println!("\n{}", pc_report);

    // Save to database for bot access
    println!("\n💾 Saving matrix to database...");
    let snapshot_id = db::save_matrix(&client, &matrix).await?;
    println!("  ✅ Saved as snapshot #{}", snapshot_id);

    println!("\n✅ Build complete!");

    Ok(())
}

async fn query_probability(
    time_elapsed: u32,
    price_delta: f64,
    market_price: f64,
    bankroll: f64,
) -> Result<()> {
    // Load matrix from file
    let matrix_path = PathBuf::from("output/matrix.json");

    if !matrix_path.exists() {
        println!("❌ Matrix not found. Run 'build' first to generate the probability matrix.");
        println!("   cargo run -- build");
        return Ok(());
    }

    let json = std::fs::read_to_string(&matrix_path)?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&json)?;

    // Convert time to bucket (15-second intervals)
    let time_bucket = (time_elapsed / 15).min(59) as u8;
    let delta_bucket = models::delta_to_bucket(rust_decimal::Decimal::try_from(price_delta)?);

    let cell = matrix.get(time_bucket, delta_bucket);

    // Get recommendation
    let rec = edge::get_recommendation(cell, market_price, bankroll);

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║               BET RECOMMENDATION ANALYSIS                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("📍 SITUATION");
    println!("─────────────────────────────────────────");
    println!("  Time elapsed:     {} seconds (bucket {})", time_elapsed, time_bucket);
    println!("  Price delta:      ${:.2} (bucket {})", price_delta, delta_bucket);
    println!("  Market price UP:  {:.1}¢ ({:.1}% implied)", market_price * 100.0, market_price * 100.0);
    println!("  Bankroll:         ${:.2}", bankroll);

    println!("\n📊 HISTORICAL DATA");
    println!("─────────────────────────────────────────");
    println!("  Sample count:     {} observations", cell.total());
    println!("  Confidence:       {:?}", cell.confidence_level);
    println!("  Raw P(UP):        {:.1}%", cell.p_up * 100.0);
    println!("  Raw P(DOWN):      {:.1}%", cell.p_down * 100.0);
    println!("  Wilson CI (UP):   {:.1}% - {:.1}%", cell.p_up_wilson_lower * 100.0, cell.p_up_wilson_upper * 100.0);

    println!("\n🎯 RECOMMENDATION");
    println!("─────────────────────────────────────────");

    if rec.should_bet {
        let direction = rec.direction.unwrap();
        println!("  ✅ BET {:?}", direction);
        println!("  Edge:             {:.1}%", rec.edge * 100.0);
        println!("  Our probability:  {:.1}% (conservative)", rec.our_probability * 100.0);
        println!("  Kelly fraction:   {:.2}%", rec.kelly_fraction * 100.0);
        println!("  Suggested bet:    ${:.2}", rec.bet_amount);

        // Calculate EV
        let ev = edge::calculate_expected_value(rec.our_probability, market_price, rec.bet_amount);
        println!("  Expected value:   ${:.2}", ev);
    } else {
        println!("  ❌ NO BET");
        println!("  Reason: Edge ({:.1}%) below threshold or insufficient data", rec.edge * 100.0);
    }

    println!("\n═══════════════════════════════════════════════════════════════\n");

    Ok(())
}

async fn show_stats(matrix_path: Option<PathBuf>) -> Result<()> {
    let path = matrix_path.unwrap_or_else(|| PathBuf::from("output/matrix.json"));

    if !path.exists() {
        println!("❌ Matrix not found at {}. Run 'build' first.", path.display());
        return Ok(());
    }

    let json = std::fs::read_to_string(&path)?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&json)?;

    let report = output::generate_report(&matrix);
    println!("{}", report);

    output::print_matrix_summary(&matrix);

    Ok(())
}

async fn query_first_passage(
    time_elapsed: u32,
    price_delta: f64,
    direction: &str,
    entry_price: f64,
) -> Result<()> {
    // Load first-passage matrix
    let fp_path = PathBuf::from("output/first_passage_matrix.json");
    let matrix_path = PathBuf::from("output/matrix.json");

    if !fp_path.exists() {
        println!("❌ First-passage matrix not found. Run 'build' first to generate it.");
        return Ok(());
    }

    let fp_json = std::fs::read_to_string(&fp_path)?;
    let fp_matrix: FirstPassageMatrix = serde_json::from_str(&fp_json)?;

    let matrix_json = std::fs::read_to_string(&matrix_path)?;
    let matrix: ProbabilityMatrix = serde_json::from_str(&matrix_json)?;

    // Convert inputs (15-second intervals)
    let time_bucket = (time_elapsed / 15).min(59) as u8;
    let delta_bucket = models::delta_to_bucket(rust_decimal::Decimal::try_from(price_delta)?);
    let is_up = direction.to_lowercase() == "up";

    let state = fp_matrix.get(time_bucket, delta_bucket);
    let terminal_cell = matrix.get(time_bucket, delta_bucket);

    let remaining_secs = 900 - time_elapsed;

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║            FIRST-PASSAGE EXIT STRATEGY ANALYSIS              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("📍 CURRENT STATE");
    println!("─────────────────────────────────────────");
    println!("  Time elapsed:     {}:{:02} ({}:{:02} remaining)",
        time_elapsed / 60, time_elapsed % 60,
        remaining_secs / 60, remaining_secs % 60);
    println!("  Price delta:      ${:.2} (bucket: {})", price_delta, models::bucket_to_label(delta_bucket));
    println!("  Position:         {} at {:.0}¢", direction.to_uppercase(), entry_price * 100.0);
    println!("  Terminal P(UP):   {:.1}%", terminal_cell.p_up * 100.0);

    println!("\n🎯 EXIT TARGET ANALYSIS ({})", if is_up { "holding UP" } else { "holding DOWN" });
    println!("─────────────────────────────────────────");
    println!("{:>12} {:>12} {:>12} {:>12} {:>12}", "Target", "P(reach)", "Gain", "EV(exit)", "vs Hold");

    // Calculate EV of holding to settlement
    let p_win = if is_up { terminal_cell.p_up } else { terminal_cell.p_down };
    let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

    // Analyze exit targets
    let targets: Vec<f64> = if is_up {
        vec![0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
    } else {
        vec![0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
    };

    let mut best_ev = hold_ev;
    let mut best_target: Option<f64> = None;

    for target_price in &targets {
        if *target_price <= entry_price {
            continue; // No point exiting at a loss
        }

        // Map target price to delta bucket (new 34-bucket system: -17 to +16)
        let target_delta_bucket = if is_up {
            // Higher P(UP) = more positive delta
            match (*target_price * 100.0) as i32 {
                0..=35 => -11,   // ~-$140
                36..=40 => -8,   // ~-$70
                41..=45 => -6,   // ~-$40
                46..=50 => -1,   // ~-$5
                51..=55 => 1,    // ~+$10
                56..=60 => 4,    // ~+$30
                61..=65 => 6,    // ~+$50
                66..=70 => 7,    // ~+$70
                71..=75 => 9,    // ~+$100
                76..=80 => 10,   // ~+$130
                81..=85 => 12,   // ~+$180
                86..=90 => 14,   // ~+$250
                91..=95 => 15,   // ~+$280
                _ => 16,         // >+$300
            }
        } else {
            // Higher P(DOWN) = more negative delta
            match ((1.0 - target_price) * 100.0) as i32 {
                0..=35 => 11,
                36..=40 => 8,
                41..=45 => 6,
                46..=50 => 1,
                51..=55 => -1,
                56..=60 => -4,
                61..=65 => -6,
                66..=70 => -7,
                71..=75 => -9,
                76..=80 => -10,
                81..=85 => -12,
                86..=90 => -14,
                91..=95 => -15,
                _ => -16,
            }
        };

        let p_reach = if is_up {
            state.get_up_target(target_delta_bucket).p_reach
        } else {
            state.get_down_target(target_delta_bucket).p_reach
        };

        let gain = target_price - entry_price;
        let loss = entry_price;

        // EV of exit strategy: P(reach) × gain - P(not reach and lose) × loss
        // Simplified: assume if we don't reach target, we hold to settlement
        let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;

        let vs_hold = exit_ev - hold_ev;

        let marker = if exit_ev > best_ev { " ← BEST" } else { "" };
        if exit_ev > best_ev {
            best_ev = exit_ev;
            best_target = Some(*target_price);
        }

        println!(
            "{:>11.0}¢ {:>11.1}% {:>11.1}¢ {:>11.2}¢ {:>+11.2}¢{}",
            target_price * 100.0,
            p_reach * 100.0,
            gain * 100.0,
            exit_ev * 100.0,
            vs_hold * 100.0,
            marker
        );
    }

    println!("\n📊 RECOMMENDATION");
    println!("─────────────────────────────────────────");
    println!("  EV(hold to settlement): {:.2}¢", hold_ev * 100.0);

    if let Some(target) = best_target {
        if best_ev > hold_ev + 0.01 {
            println!("  ✅ SET EXIT TARGET at {:.0}¢", target * 100.0);
            println!("  EV improvement: +{:.2}¢ per contract", (best_ev - hold_ev) * 100.0);
        } else {
            println!("  ➡️  HOLD to settlement (exit targets don't improve EV significantly)");
        }
    } else {
        println!("  ➡️  HOLD to settlement (no profitable exit targets found)");
    }

    println!("\n═══════════════════════════════════════════════════════════════\n");

    Ok(())
}

async fn handle_chainlink(action: ChainlinkAction) -> Result<()> {
    match action {
        ChainlinkAction::Fetch { days } => {
            chainlink_fetch(days).await?;
        }
        ChainlinkAction::Update => {
            chainlink_update().await?;
        }
        ChainlinkAction::Stats => {
            chainlink_stats().await?;
        }
    }
    Ok(())
}

async fn chainlink_fetch(days: u32) -> Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("        CHAINLINK BTC/USD HISTORICAL DATA FETCH");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("📡 Connecting to Polygon RPC...");
    let client = chainlink::ChainlinkClient::new().await?;

    println!("\n📥 Fetching {} days of historical data...", days);
    let rounds = client.fetch_historical(days).await?;

    if rounds.is_empty() {
        println!("❌ No data fetched!");
        return Ok(());
    }

    println!(
        "\n📊 Data range: {} to {}",
        rounds.first().unwrap().updated_at,
        rounds.last().unwrap().updated_at
    );
    println!(
        "📊 Price range: ${:.2} to ${:.2}",
        rounds.iter().map(|r| r.price).min().unwrap(),
        rounds.iter().map(|r| r.price).max().unwrap()
    );

    println!("\n🔌 Connecting to database...");
    let config = DbConfig::default();
    let db_client = db::connect(&config).await?;

    println!("💾 Inserting {} records...", rounds.len());
    let count = chainlink::insert_chainlink_prices(&db_client, &rounds).await?;
    println!("✅ Inserted {} records", count);

    let stats = chainlink::get_chainlink_stats(&db_client).await?;
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("                    DATABASE STATS");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Total rows:   {:>12}", stats.total_rows);
    if let (Some(earliest), Some(latest)) = (stats.earliest, stats.latest) {
        println!("  Date range:   {} to {}", earliest.format("%Y-%m-%d"), latest.format("%Y-%m-%d"));
    }
    if let (Some(min), Some(max)) = (stats.min_price, stats.max_price) {
        println!("  Price range:  ${:.2} to ${:.2}", min, max);
    }
    println!("═══════════════════════════════════════════════════════════════\n");

    Ok(())
}

async fn chainlink_update() -> Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("           CHAINLINK BTC/USD INCREMENTAL UPDATE");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("🔌 Connecting to database...");
    let config = DbConfig::default();
    let db_client = db::connect(&config).await?;

    let latest_stored = chainlink::get_latest_chainlink_timestamp(&db_client).await?;

    match latest_stored {
        Some(ts) => println!("📅 Last stored timestamp: {}", ts),
        None => {
            println!("⚠️  No existing data, use 'chainlink fetch' for initial load");
            return Ok(());
        }
    }

    println!("\n📡 Connecting to Polygon RPC...");
    let client = chainlink::ChainlinkClient::new().await?;

    let latest = client.get_latest_round().await?;
    println!("📊 Latest Chainlink price: ${:.2} at {}", latest.price, latest.updated_at);

    // Fetch last 1 day to catch up
    println!("\n📥 Fetching recent data...");
    let rounds = client.fetch_historical(1).await?;

    // Filter to only new rounds
    let new_rounds: Vec<_> = rounds
        .into_iter()
        .filter(|r| latest_stored.map_or(true, |ts| r.updated_at > ts))
        .collect();

    if new_rounds.is_empty() {
        println!("✅ Already up to date!");
    } else {
        println!("💾 Inserting {} new records...", new_rounds.len());
        let count = chainlink::insert_chainlink_prices(&db_client, &new_rounds).await?;
        println!("✅ Inserted {} records", count);
    }

    let stats = chainlink::get_chainlink_stats(&db_client).await?;
    println!("\n📊 Total rows: {}", stats.total_rows);
    if let Some(latest) = stats.latest {
        println!("📅 Latest: {}", latest);
    }

    Ok(())
}

async fn chainlink_stats() -> Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("              CHAINLINK PRICES DATABASE STATS");
    println!("═══════════════════════════════════════════════════════════════\n");

    let config = DbConfig::default();
    let db_client = db::connect(&config).await?;

    let stats = chainlink::get_chainlink_stats(&db_client).await?;

    println!("  Total rows:   {:>12}", stats.total_rows);

    if stats.total_rows > 0 {
        if let (Some(earliest), Some(latest)) = (stats.earliest, stats.latest) {
            println!("  Earliest:     {}", earliest);
            println!("  Latest:       {}", latest);
            let duration = latest - earliest;
            println!("  Span:         {} days", duration.num_days());
        }
        if let (Some(min), Some(max)) = (stats.min_price, stats.max_price) {
            println!("  Min price:    ${:.2}", min);
            println!("  Max price:    ${:.2}", max);
        }
    } else {
        println!("\n  No data in database. Run 'chainlink fetch' to populate.");
    }

    println!("\n═══════════════════════════════════════════════════════════════\n");

    Ok(())
}
