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
use crate::models::ProbabilityMatrix;

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
    for time_bucket in 0u8..30 {
        for delta_bucket in -6i8..=5i8 {
            let cell = matrix.get_mut(time_bucket, delta_bucket);
            stats::compute_cell_stats(cell);
        }
    }

    // Create output directory
    std::fs::create_dir_all(&output_dir)?;

    // Export files
    let json_path = output_dir.join("matrix.json");
    let csv_path = output_dir.join("matrix.csv");
    let report_path = output_dir.join("report.txt");

    println!("\n💾 Exporting results...");

    output::export_to_json(&matrix, &json_path)?;
    println!("  ✅ JSON: {}", json_path.display());

    output::export_to_csv(&matrix, &csv_path)?;
    println!("  ✅ CSV: {}", csv_path.display());

    let report = output::generate_report(&matrix);
    std::fs::write(&report_path, &report)?;
    println!("  ✅ Report: {}", report_path.display());

    // Print summary
    println!("\n{}", report);

    // Print matrix visualization
    output::print_matrix_summary(&matrix);

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

    // Convert time to bucket
    let time_bucket = (time_elapsed / 30).min(29) as u8;
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
