use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::models::{bucket_to_label, ConfidenceLevel, ProbabilityMatrix};

/// Export the probability matrix to JSON
pub fn export_to_json(matrix: &ProbabilityMatrix, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(matrix)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Export the probability matrix to CSV
/// Format: time_bucket, price_delta_bucket, price_delta_label, count_up, count_down, total,
///         p_up, p_down, p_up_wilson_lower, p_up_wilson_upper, confidence_level
pub fn export_to_csv(matrix: &ProbabilityMatrix, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;

    // Write header
    wtr.write_record(&[
        "time_bucket",
        "time_range",
        "price_delta_bucket",
        "price_delta_label",
        "count_up",
        "count_down",
        "total",
        "p_up",
        "p_down",
        "p_up_wilson_lower",
        "p_up_wilson_upper",
        "beta_alpha",
        "beta_beta",
        "confidence_level",
    ])?;

    // Write data
    for time_bucket in 0u8..30 {
        let time_start_secs = time_bucket as u32 * 30;
        let time_end_secs = time_start_secs + 30;
        let time_range = format!(
            "{}:{:02}-{}:{:02}",
            time_start_secs / 60,
            time_start_secs % 60,
            time_end_secs / 60,
            time_end_secs % 60
        );

        for delta_bucket in -6i8..=5i8 {
            let cell = matrix.get(time_bucket, delta_bucket);

            wtr.write_record(&[
                time_bucket.to_string(),
                time_range.clone(),
                delta_bucket.to_string(),
                bucket_to_label(delta_bucket).to_string(),
                cell.count_up.to_string(),
                cell.count_down.to_string(),
                cell.total().to_string(),
                format!("{:.4}", cell.p_up),
                format!("{:.4}", cell.p_down),
                format!("{:.4}", cell.p_up_wilson_lower),
                format!("{:.4}", cell.p_up_wilson_upper),
                format!("{:.2}", cell.beta_alpha),
                format!("{:.2}", cell.beta_beta),
                format!("{:?}", cell.confidence_level),
            ])?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate a human-readable summary report
pub fn generate_report(matrix: &ProbabilityMatrix) -> String {
    let mut report = String::new();

    report.push_str("╔══════════════════════════════════════════════════════════════════╗\n");
    report.push_str("║       POLYMARKET BTC PROBABILITY MATRIX - ANALYSIS REPORT        ║\n");
    report.push_str("╚══════════════════════════════════════════════════════════════════╝\n\n");

    // Data summary
    report.push_str("📊 DATA SUMMARY\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str(&format!(
        "Total 15-min windows analyzed: {}\n",
        matrix.total_windows
    ));
    if let (Some(start), Some(end)) = (matrix.data_start, matrix.data_end) {
        report.push_str(&format!("Date range: {} to {}\n", start.format("%Y-%m-%d %H:%M"), end.format("%Y-%m-%d %H:%M")));
    }
    report.push('\n');

    // Confidence distribution
    let mut unreliable = 0;
    let mut weak = 0;
    let mut moderate = 0;
    let mut strong = 0;

    for time_bucket in 0u8..30 {
        for delta_bucket in -6i8..=5i8 {
            let cell = matrix.get(time_bucket, delta_bucket);
            match cell.confidence_level {
                ConfidenceLevel::Unreliable => unreliable += 1,
                ConfidenceLevel::Weak => weak += 1,
                ConfidenceLevel::Moderate => moderate += 1,
                ConfidenceLevel::Strong => strong += 1,
            }
        }
    }

    report.push_str("📈 CONFIDENCE DISTRIBUTION (390 cells total)\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str(&format!("  Strong (n≥100):     {:3} cells ({:.1}%)\n", strong, strong as f64 / 3.9));
    report.push_str(&format!("  Moderate (30≤n<100): {:3} cells ({:.1}%)\n", moderate, moderate as f64 / 3.9));
    report.push_str(&format!("  Weak (10≤n<30):      {:3} cells ({:.1}%)\n", weak, weak as f64 / 3.9));
    report.push_str(&format!("  Unreliable (n<10):   {:3} cells ({:.1}%)\n", unreliable, unreliable as f64 / 3.9));
    report.push('\n');

    // Find most extreme cells (highest P(UP) and P(DOWN) with good confidence)
    report.push_str("🎯 MOST BIASED CELLS (Strong/Moderate confidence)\n");
    report.push_str("─────────────────────────────────────────\n");

    let mut significant_cells: Vec<_> = Vec::new();
    for time_bucket in 0u8..30 {
        for delta_bucket in -6i8..=5i8 {
            let cell = matrix.get(time_bucket, delta_bucket);
            if matches!(
                cell.confidence_level,
                ConfidenceLevel::Strong | ConfidenceLevel::Moderate
            ) {
                significant_cells.push(cell);
            }
        }
    }

    // Sort by deviation from 50%
    significant_cells.sort_by(|a, b| {
        let a_dev = (a.p_up - 0.5).abs();
        let b_dev = (b.p_up - 0.5).abs();
        b_dev.partial_cmp(&a_dev).unwrap()
    });

    report.push_str("\nTop 10 most biased towards UP:\n");
    for cell in significant_cells.iter().filter(|c| c.p_up > 0.5).take(10) {
        let time_start = cell.time_bucket as u32 * 30;
        report.push_str(&format!(
            "  Time {:2}:{:02} | {} | P(UP)={:.1}% (CI: {:.1}%-{:.1}%) | n={}\n",
            time_start / 60,
            time_start % 60,
            bucket_to_label(cell.price_delta_bucket),
            cell.p_up * 100.0,
            cell.p_up_wilson_lower * 100.0,
            cell.p_up_wilson_upper * 100.0,
            cell.total()
        ));
    }

    report.push_str("\nTop 10 most biased towards DOWN:\n");
    for cell in significant_cells.iter().filter(|c| c.p_up < 0.5).take(10) {
        let time_start = cell.time_bucket as u32 * 30;
        report.push_str(&format!(
            "  Time {:2}:{:02} | {} | P(DOWN)={:.1}% (CI: {:.1}%-{:.1}%) | n={}\n",
            time_start / 60,
            time_start % 60,
            bucket_to_label(cell.price_delta_bucket),
            cell.p_down * 100.0,
            (1.0 - cell.p_up_wilson_upper) * 100.0,
            (1.0 - cell.p_up_wilson_lower) * 100.0,
            cell.total()
        ));
    }

    report.push('\n');
    report.push_str("═══════════════════════════════════════════════════════════════════\n");

    report
}

/// Print a condensed matrix view to console
pub fn print_matrix_summary(matrix: &ProbabilityMatrix) {
    println!("\n📊 PROBABILITY MATRIX SUMMARY (P(UP) %)");
    println!("Time buckets (rows) × Price delta buckets (columns)\n");

    // Header row with delta bucket labels
    print!("        ");
    for delta in -6i8..=5i8 {
        print!("{:>8}", format!("[{}]", delta));
    }
    println!();

    // Print short labels
    print!("        ");
    for delta in -6i8..=5i8 {
        let label = match delta {
            -6 => "<-300",
            -5 => "-300",
            -4 => "-200",
            -3 => "-100",
            -2 => "-50",
            -1 => "-20",
            0 => "+20",
            1 => "+50",
            2 => "+100",
            3 => "+200",
            4 => "+300",
            5 => ">+300",
            _ => "?",
        };
        print!("{:>8}", label);
    }
    println!("\n");

    // Data rows
    for time_bucket in 0u8..30 {
        let time_secs = time_bucket as u32 * 30;
        print!("{:2}:{:02}   ", time_secs / 60, time_secs % 60);

        for delta in -6i8..=5i8 {
            let cell = matrix.get(time_bucket, delta);
            let symbol = match cell.confidence_level {
                ConfidenceLevel::Strong => "",
                ConfidenceLevel::Moderate => "~",
                ConfidenceLevel::Weak => "?",
                ConfidenceLevel::Unreliable => "-",
            };
            if cell.total() == 0 {
                print!("{:>8}", "-");
            } else {
                print!("{:>7}{}", format!("{:.0}", cell.p_up * 100.0), symbol);
            }
        }
        println!();
    }

    println!("\nLegend: (no suffix)=Strong, ~=Moderate, ?=Weak, -=Unreliable/NoData");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Outcome;
    use rust_decimal_macros::dec;

    #[test]
    fn test_generate_report() {
        let mut matrix = ProbabilityMatrix::new();

        // Add some sample data
        for _ in 0..50 {
            matrix.record(0, dec!(10), Outcome::Up);
        }
        for _ in 0..50 {
            matrix.record(0, dec!(10), Outcome::Down);
        }

        // Compute stats for the cell
        let cell = matrix.get_mut(0, 0);
        crate::stats::compute_cell_stats(cell);

        let report = generate_report(&matrix);
        assert!(report.contains("ANALYSIS REPORT"));
        assert!(report.contains("CONFIDENCE DISTRIBUTION"));
    }
}
