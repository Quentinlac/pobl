use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::models::{bucket_to_label, ConfidenceLevel, FirstPassageMatrix, ProbabilityMatrix};

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
    for time_bucket in 0u8..60 {
        let time_start_secs = time_bucket as u32 * 15;
        let time_end_secs = time_start_secs + 15;
        let time_range = format!(
            "{}:{:02}-{}:{:02}",
            time_start_secs / 60,
            time_start_secs % 60,
            time_end_secs / 60,
            time_end_secs % 60
        );

        for delta_bucket in -17i8..=16i8 {
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

    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
            let cell = matrix.get(time_bucket, delta_bucket);
            match cell.confidence_level {
                ConfidenceLevel::Unreliable => unreliable += 1,
                ConfidenceLevel::Weak => weak += 1,
                ConfidenceLevel::Moderate => moderate += 1,
                ConfidenceLevel::Strong => strong += 1,
            }
        }
    }

    report.push_str("📈 CONFIDENCE DISTRIBUTION (2040 cells total)\n");
    report.push_str("─────────────────────────────────────────\n");
    let total_cells = 2040.0;
    report.push_str(&format!("  Strong (n≥100):     {:4} cells ({:.1}%)\n", strong, strong as f64 / total_cells * 100.0));
    report.push_str(&format!("  Moderate (30≤n<100): {:4} cells ({:.1}%)\n", moderate, moderate as f64 / total_cells * 100.0));
    report.push_str(&format!("  Weak (10≤n<30):      {:4} cells ({:.1}%)\n", weak, weak as f64 / total_cells * 100.0));
    report.push_str(&format!("  Unreliable (n<10):   {:4} cells ({:.1}%)\n", unreliable, unreliable as f64 / total_cells * 100.0));
    report.push('\n');

    // Find most extreme cells (highest P(UP) and P(DOWN) with good confidence)
    report.push_str("🎯 MOST BIASED CELLS (Strong/Moderate confidence)\n");
    report.push_str("─────────────────────────────────────────\n");

    let mut significant_cells: Vec<_> = Vec::new();
    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
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
        let time_start = cell.time_bucket as u32 * 15;
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
        let time_start = cell.time_bucket as u32 * 15;
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

/// Print a condensed matrix view to console (showing a subset of key buckets)
pub fn print_matrix_summary(matrix: &ProbabilityMatrix) {
    println!("\n📊 PROBABILITY MATRIX SUMMARY (P(UP) %)");
    println!("Time buckets (rows) × Price delta buckets (columns)");
    println!("(Showing key buckets: -17, -11, -8, -5, -1, 0, 1, 4, 7, 9, 12, 16)\n");

    // Key buckets to show (representative sample since 34 is too wide)
    let key_buckets: Vec<i8> = vec![-17, -11, -8, -5, -1, 0, 1, 4, 7, 9, 12, 16];

    // Header row with delta bucket labels
    print!("        ");
    for &delta in &key_buckets {
        print!("{:>7}", format!("[{}]", delta));
    }
    println!();

    // Print short labels
    print!("        ");
    for &delta in &key_buckets {
        let label = match delta {
            -17 => "<-300",
            -11 => "-140",
            -8 => "-70",
            -5 => "-30",
            -1 => "-5",
            0 => "+5",
            1 => "+10",
            4 => "+30",
            7 => "+70",
            9 => "+110",
            12 => "+200",
            16 => ">+300",
            _ => "?",
        };
        print!("{:>7}", label);
    }
    println!("\n");

    // Data rows
    for time_bucket in 0u8..60 {
        let time_secs = time_bucket as u32 * 15;
        print!("{:2}:{:02}   ", time_secs / 60, time_secs % 60);

        for &delta in &key_buckets {
            let cell = matrix.get(time_bucket, delta);
            let symbol = match cell.confidence_level {
                ConfidenceLevel::Strong => "",
                ConfidenceLevel::Moderate => "~",
                ConfidenceLevel::Weak => "?",
                ConfidenceLevel::Unreliable => "-",
            };
            if cell.total() == 0 {
                print!("{:>7}", "-");
            } else {
                print!("{:>6}{}", format!("{:.0}", cell.p_up * 100.0), symbol);
            }
        }
        println!();
    }

    println!("\nLegend: (no suffix)=Strong, ~=Moderate, ?=Weak, -=Unreliable/NoData");
    println!("Full data available in output/matrix.csv");
}

// ============================================================================
// FIRST-PASSAGE MATRIX OUTPUT
// ============================================================================

/// Export the first-passage matrix to JSON
pub fn export_first_passage_to_json(matrix: &FirstPassageMatrix, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(matrix)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Export first-passage matrix to CSV (one row per state + target combination)
pub fn export_first_passage_to_csv(matrix: &FirstPassageMatrix, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;

    // Write header
    wtr.write_record(&[
        "time_bucket",
        "time_range",
        "price_delta_bucket",
        "price_delta_label",
        "direction",  // UP or DOWN
        "target_bucket",
        "target_label",
        "count_reached",
        "count_total",
        "p_reach",
        "p_reach_wilson_lower",
        "p_reach_wilson_upper",
        "confidence_level",
    ])?;

    // Write data
    for time_bucket in 0u8..60 {
        let time_start_secs = time_bucket as u32 * 15;
        let time_end_secs = time_start_secs + 15;
        let time_range = format!(
            "{}:{:02}-{}:{:02}",
            time_start_secs / 60,
            time_start_secs % 60,
            time_end_secs / 60,
            time_end_secs % 60
        );

        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get(time_bucket, delta_bucket);

            // UP targets
            for target in -17i8..=16i8 {
                let cell = state.get_up_target(target);
                wtr.write_record(&[
                    time_bucket.to_string(),
                    time_range.clone(),
                    delta_bucket.to_string(),
                    bucket_to_label(delta_bucket).to_string(),
                    "UP".to_string(),
                    target.to_string(),
                    bucket_to_label(target).to_string(),
                    cell.count_reached.to_string(),
                    cell.count_total.to_string(),
                    format!("{:.4}", cell.p_reach),
                    format!("{:.4}", cell.p_reach_wilson_lower),
                    format!("{:.4}", cell.p_reach_wilson_upper),
                    format!("{:?}", cell.confidence_level),
                ])?;
            }

            // DOWN targets
            for target in -17i8..=16i8 {
                let cell = state.get_down_target(target);
                wtr.write_record(&[
                    time_bucket.to_string(),
                    time_range.clone(),
                    delta_bucket.to_string(),
                    bucket_to_label(delta_bucket).to_string(),
                    "DOWN".to_string(),
                    target.to_string(),
                    bucket_to_label(target).to_string(),
                    cell.count_reached.to_string(),
                    cell.count_total.to_string(),
                    format!("{:.4}", cell.p_reach),
                    format!("{:.4}", cell.p_reach_wilson_lower),
                    format!("{:.4}", cell.p_reach_wilson_upper),
                    format!("{:?}", cell.confidence_level),
                ])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate a summary report for the first-passage matrix
pub fn generate_first_passage_report(matrix: &FirstPassageMatrix) -> String {
    let mut report = String::new();

    report.push_str("╔══════════════════════════════════════════════════════════════════╗\n");
    report.push_str("║     FIRST-PASSAGE PROBABILITY MATRIX - ANALYSIS REPORT          ║\n");
    report.push_str("╚══════════════════════════════════════════════════════════════════╝\n\n");

    report.push_str("📊 DATA SUMMARY\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str(&format!(
        "Total observations: {}\n",
        matrix.total_observations
    ));
    if let (Some(start), Some(end)) = (matrix.data_start, matrix.data_end) {
        report.push_str(&format!(
            "Date range: {} to {}\n",
            start.format("%Y-%m-%d %H:%M"),
            end.format("%Y-%m-%d %H:%M")
        ));
    }
    report.push('\n');

    // Show some interesting first-passage probabilities
    report.push_str("🎯 SAMPLE FIRST-PASSAGE PROBABILITIES\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str("From state (time, delta) → P(reaches target)\n\n");

    // Example: From early time buckets (lots of time remaining)
    for time_bucket in [6u8, 20, 40] {
        // 1:30, 5:00, 10:00
        let time_secs = time_bucket as u32 * 15;
        report.push_str(&format!("Time {:2}:{:02} ({} remaining):\n",
            time_secs / 60, time_secs % 60,
            format!("{}:{:02}", (900 - time_secs) / 60, (900 - time_secs) % 60)
        ));

        for delta_bucket in [-10i8, 0, 9] {
            // -$110 to -$90, $0 to $5, +$90 to +$110
            let state = matrix.get(time_bucket, delta_bucket);

            // Show UP target probabilities (reaching higher deltas)
            let up_targets: Vec<(i8, f64, u32)> = [0, 7, 12]
                .iter()
                .map(|&t| {
                    let cell = state.get_up_target(t);
                    (t, cell.p_reach, cell.count_total)
                })
                .collect();

            report.push_str(&format!(
                "  From {}: P(→{})={:.0}% P(→{})={:.0}% P(→{})={:.0}% [n={}]\n",
                bucket_to_label(delta_bucket),
                bucket_to_label(up_targets[0].0),
                up_targets[0].1 * 100.0,
                bucket_to_label(up_targets[1].0),
                up_targets[1].1 * 100.0,
                bucket_to_label(up_targets[2].0),
                up_targets[2].1 * 100.0,
                up_targets[0].2
            ));
        }
        report.push('\n');
    }

    report.push_str("═══════════════════════════════════════════════════════════════════\n");

    report
}

/// Print a condensed first-passage view for a specific starting state
pub fn print_first_passage_from_state(matrix: &FirstPassageMatrix, time_bucket: u8, delta_bucket: i8) {
    let state = matrix.get(time_bucket, delta_bucket);
    let time_secs = time_bucket as u32 * 15;
    let remaining_secs = 900 - time_secs;

    println!("\n📊 FIRST-PASSAGE PROBABILITIES");
    println!("From: Time {}:{:02} | Delta: {}",
        time_secs / 60, time_secs % 60,
        bucket_to_label(delta_bucket)
    );
    println!("Remaining: {}:{:02}\n", remaining_secs / 60, remaining_secs % 60);

    println!("UP Targets (P(max delta ≥ target)):");
    println!("{:>15} {:>10} {:>10} {:>10}", "Target", "P(reach)", "CI Low", "n");
    for target in -17i8..=16i8 {
        let cell = state.get_up_target(target);
        if cell.count_total > 0 {
            println!(
                "{:>15} {:>9.1}% {:>9.1}% {:>10}",
                bucket_to_label(target),
                cell.p_reach * 100.0,
                cell.p_reach_wilson_lower * 100.0,
                cell.count_total
            );
        }
    }

    println!("\nDOWN Targets (P(min delta ≤ target)):");
    println!("{:>15} {:>10} {:>10} {:>10}", "Target", "P(reach)", "CI Low", "n");
    for target in (-17i8..=16i8).rev() {
        let cell = state.get_down_target(target);
        if cell.count_total > 0 {
            println!(
                "{:>15} {:>9.1}% {:>9.1}% {:>10}",
                bucket_to_label(target),
                cell.p_reach * 100.0,
                cell.p_reach_wilson_lower * 100.0,
                cell.count_total
            );
        }
    }
}

// ============================================================================
// PRICE REACH MATRIX OUTPUT
// ============================================================================

use crate::models::{
    crossing_level_to_cents, price_level_to_cents, PriceCrossingMatrix, PriceReachMatrix,
    PRICE_LEVELS,
};

/// Export price reach matrix to JSON
pub fn export_price_reach_to_json(matrix: &PriceReachMatrix, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(matrix)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Export price reach matrix to CSV
/// Format: time_bucket, time_range, delta_bucket, delta_label, count_total,
///         up_0, up_4, up_8, ..., up_100, down_0, down_4, ..., down_100
pub fn export_price_reach_to_csv(matrix: &PriceReachMatrix, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;

    // Build header
    let mut header = vec![
        "time_bucket".to_string(),
        "time_range".to_string(),
        "delta_bucket".to_string(),
        "delta_label".to_string(),
        "count_total".to_string(),
    ];

    // Add UP price columns
    for level in 0..PRICE_LEVELS {
        header.push(format!("p_up_reach_{}c", price_level_to_cents(level)));
    }
    // Add DOWN price columns
    for level in 0..PRICE_LEVELS {
        header.push(format!("p_down_reach_{}c", price_level_to_cents(level)));
    }

    wtr.write_record(&header)?;

    // Write data
    for time_bucket in 0u8..60 {
        let time_start_secs = time_bucket as u32 * 15;
        let time_end_secs = time_start_secs + 15;
        let time_range = format!(
            "{}:{:02}-{}:{:02}",
            time_start_secs / 60,
            time_start_secs % 60,
            time_end_secs / 60,
            time_end_secs % 60
        );

        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get(time_bucket, delta_bucket);

            let mut row = vec![
                time_bucket.to_string(),
                time_range.clone(),
                delta_bucket.to_string(),
                bucket_to_label(delta_bucket).to_string(),
                state.count_total.to_string(),
            ];

            // Add UP probabilities
            for level in 0..PRICE_LEVELS {
                row.push(format!("{:.4}", state.p_up_reach[level]));
            }
            // Add DOWN probabilities
            for level in 0..PRICE_LEVELS {
                row.push(format!("{:.4}", state.p_down_reach[level]));
            }

            wtr.write_record(&row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate a human-readable report for the price reach matrix
pub fn generate_price_reach_report(matrix: &PriceReachMatrix) -> String {
    let mut report = String::new();

    report.push_str("╔══════════════════════════════════════════════════════════════════╗\n");
    report.push_str("║       PRICE REACH PROBABILITY MATRIX - ANALYSIS REPORT          ║\n");
    report.push_str("╚══════════════════════════════════════════════════════════════════╝\n\n");

    report.push_str("📊 DATA SUMMARY\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str(&format!(
        "Total observations: {}\n",
        matrix.total_observations
    ));
    if let (Some(start), Some(end)) = (matrix.data_start, matrix.data_end) {
        report.push_str(&format!(
            "Date range: {} to {}\n",
            start.format("%Y-%m-%d %H:%M"),
            end.format("%Y-%m-%d %H:%M")
        ));
    }
    report.push('\n');

    report.push_str("📈 PRICE LEVELS TRACKED\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str("26 levels in 4¢ increments: 0¢, 4¢, 8¢, ..., 96¢, 100¢\n\n");

    // Show sample data for a few key states
    report.push_str("🎯 SAMPLE PRICE REACH PROBABILITIES\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str("From state (time, delta) → P(token price reaches X¢)\n\n");

    // Sample times: early (1:30), mid (5:00), late (10:00)
    for time_bucket in [6u8, 20, 40] {
        let time_secs = time_bucket as u32 * 15;
        report.push_str(&format!(
            "Time {:2}:{:02} ({} remaining):\n",
            time_secs / 60,
            time_secs % 60,
            format!("{}:{:02}", (900 - time_secs) / 60, (900 - time_secs) % 60)
        ));

        // Sample deltas: -$30, $0, +$30
        for delta_bucket in [-6i8, 0, 5] {
            let state = matrix.get(time_bucket, delta_bucket);

            if state.count_total == 0 {
                continue;
            }

            // Show P(UP reaches) for key prices: 20¢, 40¢, 60¢, 80¢
            report.push_str(&format!(
                "  {} (n={}): UP→20¢={:.0}% 40¢={:.0}% 60¢={:.0}% 80¢={:.0}%\n",
                bucket_to_label(delta_bucket),
                state.count_total,
                state.p_up_reach[5] * 100.0,   // 20¢
                state.p_up_reach[10] * 100.0,  // 40¢
                state.p_up_reach[15] * 100.0,  // 60¢
                state.p_up_reach[20] * 100.0,  // 80¢
            ));

            report.push_str(&format!(
                "                       DOWN→20¢={:.0}% 40¢={:.0}% 60¢={:.0}% 80¢={:.0}%\n",
                state.p_down_reach[5] * 100.0,
                state.p_down_reach[10] * 100.0,
                state.p_down_reach[15] * 100.0,
                state.p_down_reach[20] * 100.0,
            ));
        }
        report.push('\n');
    }

    report.push_str("═══════════════════════════════════════════════════════════════════\n");

    report
}

/// Print a detailed price reach summary for a specific state
pub fn print_price_reach_from_state(matrix: &PriceReachMatrix, time_bucket: u8, delta_bucket: i8) {
    let state = matrix.get(time_bucket, delta_bucket);
    let time_secs = time_bucket as u32 * 15;
    let remaining_secs = 900 - time_secs;

    println!("\n📊 PRICE REACH PROBABILITIES");
    println!(
        "From: Time {}:{:02} | Delta: {}",
        time_secs / 60,
        time_secs % 60,
        bucket_to_label(delta_bucket)
    );
    println!(
        "Remaining: {}:{:02} | Observations: {}",
        remaining_secs / 60,
        remaining_secs % 60,
        state.count_total
    );

    if state.count_total == 0 {
        println!("\n⚠️  No data for this state");
        return;
    }

    println!("\nUP Token Price Reach Probabilities:");
    println!("{:>6} {:>10} {:>10}", "Price", "P(reach)", "Count");
    for level in 0..PRICE_LEVELS {
        let cents = price_level_to_cents(level);
        let p = state.p_up_reach[level];
        let count = state.up_reached[level];
        println!("{:>5}¢ {:>9.1}% {:>10}", cents, p * 100.0, count);
    }

    println!("\nDOWN Token Price Reach Probabilities:");
    println!("{:>6} {:>10} {:>10}", "Price", "P(reach)", "Count");
    for level in 0..PRICE_LEVELS {
        let cents = price_level_to_cents(level);
        let p = state.p_down_reach[level];
        let count = state.down_reached[level];
        println!("{:>5}¢ {:>9.1}% {:>10}", cents, p * 100.0, count);
    }
}

// ============================================================================
// PRICE CROSSING MATRIX OUTPUT
// ============================================================================

/// Export price crossing matrix to JSON
pub fn export_price_crossing_to_json(matrix: &PriceCrossingMatrix, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(matrix)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Export price crossing matrix to CSV (simplified 3-column format)
/// Format: time_bucket, time_range, delta_bucket, delta_label, current_up_cents, trajectories,
///         Then for each level (4c to 100c): up_cents, down_cents, p_reached
pub fn export_price_crossing_to_csv(matrix: &PriceCrossingMatrix, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;

    // Build header
    let mut header = vec![
        "time_bucket".to_string(),
        "time_range".to_string(),
        "delta_bucket".to_string(),
        "delta_label".to_string(),
        "trajectories".to_string(),
    ];

    // Add reached columns (touched level at least once, either direction)
    for level in 0..25 {
        header.push(format!("reached_{}c", crossing_level_to_cents(level)));
    }
    // Add p_reached columns (reached / trajectories)
    for level in 0..25 {
        header.push(format!("p_{}c", crossing_level_to_cents(level)));
    }
    // Add p_normalized columns (reached / sum_reached, sums to 100%)
    for level in 0..25 {
        header.push(format!("pct_{}c", crossing_level_to_cents(level)));
    }
    // Add up_value columns (UP_cents * p_reached)
    for level in 0..25 {
        header.push(format!("up_val_{}c", crossing_level_to_cents(level)));
    }
    // Add dn_value columns (DN_cents * p_reached)
    for level in 0..25 {
        header.push(format!("dn_val_{}c", crossing_level_to_cents(level)));
    }

    wtr.write_record(&header)?;

    // Write data
    for time_bucket in 0u8..60 {
        let time_start_secs = time_bucket as u32 * 15;
        let time_end_secs = time_start_secs + 15;
        let time_range = format!(
            "{}:{:02}-{}:{:02}",
            time_start_secs / 60,
            time_start_secs % 60,
            time_end_secs / 60,
            time_end_secs % 60
        );

        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get(time_bucket, delta_bucket);

            let mut row = vec![
                time_bucket.to_string(),
                time_range.clone(),
                delta_bucket.to_string(),
                bucket_to_label(delta_bucket).to_string(),
                state.count_trajectories.to_string(),
            ];

            // Add reached (count of trajectories that touched this level)
            for level in 0..25 {
                row.push(state.reached[level].to_string());
            }
            // Add p_reached (reached / trajectories)
            for level in 0..25 {
                row.push(format!("{:.4}", state.p_reached[level]));
            }
            // Add p_normalized (percentage, sums to 100%)
            for level in 0..25 {
                row.push(format!("{:.4}", state.p_normalized[level]));
            }
            // Add up_value
            for level in 0..25 {
                row.push(format!("{:.2}", state.up_value[level]));
            }
            // Add dn_value
            for level in 0..25 {
                row.push(format!("{:.2}", state.dn_value[level]));
            }

            wtr.write_record(&row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Generate a human-readable report for the price crossing matrix
pub fn generate_price_crossing_report(matrix: &PriceCrossingMatrix) -> String {
    let mut report = String::new();

    report.push_str("╔══════════════════════════════════════════════════════════════════╗\n");
    report.push_str("║       PRICE CROSSING MATRIX - ANALYSIS REPORT                    ║\n");
    report.push_str("╚══════════════════════════════════════════════════════════════════╝\n\n");

    report.push_str("📊 DATA SUMMARY\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str(&format!(
        "Total trajectories analyzed: {}\n",
        matrix.total_trajectories
    ));
    if let (Some(start), Some(end)) = (matrix.data_start, matrix.data_end) {
        report.push_str(&format!(
            "Date range: {} to {}\n",
            start.format("%Y-%m-%d %H:%M"),
            end.format("%Y-%m-%d %H:%M")
        ));
    }
    report.push('\n');

    report.push_str("📈 PRICE LEVELS TRACKED\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str("25 levels in 4¢ increments: 4¢, 8¢, 12¢, ..., 96¢, 100¢\n");
    report.push_str("Counts how many times price crosses each level (up or down)\n\n");

    // Show sample data for a few key states
    report.push_str("🎯 SAMPLE PRICE CROSSING AVERAGES\n");
    report.push_str("─────────────────────────────────────────\n");
    report.push_str("From state (time, delta) → Avg crossings per trajectory\n\n");

    // Sample times: early (1:30), mid (5:00), late (10:00)
    for time_bucket in [6u8, 20, 40] {
        let time_secs = time_bucket as u32 * 15;
        report.push_str(&format!(
            "Time {:2}:{:02} ({} remaining):\n",
            time_secs / 60,
            time_secs % 60,
            format!("{}:{:02}", (900 - time_secs) / 60, (900 - time_secs) % 60)
        ));

        // Sample deltas: -$30, $0, +$30
        for delta_bucket in [-6i8, 0, 5] {
            let state = matrix.get(time_bucket, delta_bucket);

            if state.count_trajectories == 0 {
                continue;
            }

            // Show avg crossings for key prices: 20¢, 40¢, 60¢, 80¢
            report.push_str(&format!(
                "  {} (n={}): 20¢={:.2} 40¢={:.2} 60¢={:.2} 80¢={:.2} crosses/traj\n",
                bucket_to_label(delta_bucket),
                state.count_trajectories,
                state.avg_crossings[4],  // 20¢ = index 4 (20/4 - 1)
                state.avg_crossings[9],  // 40¢ = index 9
                state.avg_crossings[14], // 60¢ = index 14
                state.avg_crossings[19], // 80¢ = index 19
            ));
        }
        report.push('\n');
    }

    report.push_str("═══════════════════════════════════════════════════════════════════\n");

    report
}

/// Print a detailed price crossing summary for a specific state
pub fn print_price_crossing_from_state(
    matrix: &PriceCrossingMatrix,
    time_bucket: u8,
    delta_bucket: i8,
) {
    let state = matrix.get(time_bucket, delta_bucket);
    let time_secs = time_bucket as u32 * 15;
    let remaining_secs = 900 - time_secs;

    println!("\n📊 PRICE CROSSING STATISTICS");
    println!(
        "From: Time {}:{:02} | Delta: {}",
        time_secs / 60,
        time_secs % 60,
        bucket_to_label(delta_bucket)
    );
    println!(
        "Remaining: {}:{:02} | Trajectories: {}",
        remaining_secs / 60,
        remaining_secs % 60,
        state.count_trajectories
    );

    if state.count_trajectories == 0 {
        println!("\n⚠️  No data for this state");
        return;
    }

    println!("\nPrice Level Crossings:");
    println!(
        "{:>6} {:>12} {:>12}",
        "Price", "Total Cross", "Avg/Traj"
    );
    for level in 0..25 {
        let cents = crossing_level_to_cents(level);
        let total = state.crossings[level];
        let avg = state.avg_crossings[level];
        println!("{:>5}¢ {:>12} {:>12.2}", cents, total, avg);
    }
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
