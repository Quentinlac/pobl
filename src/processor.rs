use chrono::{DateTime, Duration, Timelike, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::models::{
    count_crossings_directional, delta_to_bucket, FifteenMinWindow, FirstPassageMatrix, Outcome, PricePoint,
    PriceCrossingMatrix, PriceReachMatrix, PriceSnapshot, ProbabilityMatrix,
};

/// Process raw price points into 15-minute windows
pub fn process_into_windows(prices: &[PricePoint]) -> Vec<FifteenMinWindow> {
    if prices.is_empty() {
        return Vec::new();
    }

    // Group prices by their 15-minute window start time
    let mut windows_map: HashMap<DateTime<Utc>, Vec<&PricePoint>> = HashMap::new();

    for price in prices {
        let window_start = get_window_start(price.timestamp);
        windows_map.entry(window_start).or_default().push(price);
    }

    // Convert to FifteenMinWindow structs
    let mut windows: Vec<FifteenMinWindow> = windows_map
        .into_iter()
        .filter_map(|(start_time, points)| {
            // Need at least some data points to be useful
            // Chainlink updates every 60s = 15 points per 15-min window
            // Binance updates every 1s = 900 points per window
            // Accept windows with at least 10 points (covers Chainlink data)
            if points.len() < 10 {
                return None;
            }

            build_window(start_time, &points)
        })
        .collect();

    // Sort by start time
    windows.sort_by_key(|w| w.start_time);

    windows
}

/// Get the 15-minute window start time for a given timestamp
/// Windows start at :00, :15, :30, :45
pub fn get_window_start(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let minute = timestamp.minute();
    let window_minute = (minute / 15) * 15;

    timestamp
        .with_minute(window_minute)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

/// Get the time bucket (0-59) for a timestamp within a 15-minute window
pub fn get_time_bucket(window_start: DateTime<Utc>, timestamp: DateTime<Utc>) -> u8 {
    let elapsed = timestamp.signed_duration_since(window_start);
    let seconds = elapsed.num_seconds();

    // Each bucket is 15 seconds
    let bucket = (seconds / 15).min(59).max(0) as u8;
    bucket
}

/// Build a FifteenMinWindow from grouped price points
fn build_window(start_time: DateTime<Utc>, points: &[&PricePoint]) -> Option<FifteenMinWindow> {
    // Sort points by timestamp
    let mut sorted_points: Vec<&PricePoint> = points.to_vec();
    sorted_points.sort_by_key(|p| p.timestamp);

    // Get open and close prices
    let open_price = sorted_points.first()?.close_price;

    // Find the price closest to window end (15 minutes after start)
    let window_end = start_time + Duration::minutes(15);
    let close_price = sorted_points
        .iter()
        .filter(|p| p.timestamp <= window_end)
        .last()?
        .close_price;

    // Determine outcome
    let outcome = if close_price >= open_price {
        Outcome::Up
    } else {
        Outcome::Down
    };

    // Build snapshots for each 15-second bucket
    let mut snapshots = Vec::with_capacity(60);

    for bucket in 0u8..60 {
        let bucket_start = start_time + Duration::seconds(bucket as i64 * 15);
        let bucket_end = bucket_start + Duration::seconds(15);

        // Find the last price point in this bucket (or before it)
        let price_at_bucket = sorted_points
            .iter()
            .filter(|p| p.timestamp < bucket_end)
            .last()
            .map(|p| p.close_price)
            .unwrap_or(open_price);

        let delta_from_open = price_at_bucket - open_price;

        snapshots.push(PriceSnapshot {
            time_bucket: bucket,
            price: price_at_bucket,
            delta_from_open,
        });
    }

    Some(FifteenMinWindow {
        start_time,
        open_price,
        close_price,
        outcome,
        snapshots,
    })
}

/// Populate the probability matrix from processed windows
pub fn populate_matrix(windows: &[FifteenMinWindow], matrix: &mut ProbabilityMatrix) {
    for window in windows {
        matrix.total_windows += 1;

        // Update data range
        if matrix.data_start.is_none() || window.start_time < matrix.data_start.unwrap() {
            matrix.data_start = Some(window.start_time);
        }
        if matrix.data_end.is_none() || window.start_time > matrix.data_end.unwrap() {
            matrix.data_end = Some(window.start_time);
        }

        // Record each snapshot
        for snapshot in &window.snapshots {
            matrix.record(
                snapshot.time_bucket,
                snapshot.delta_from_open,
                window.outcome,
            );
        }
    }
}

/// Process all data and return a populated probability matrix
pub fn build_probability_matrix(prices: &[PricePoint]) -> ProbabilityMatrix {
    let windows = process_into_windows(prices);

    let mut matrix = ProbabilityMatrix::new();
    populate_matrix(&windows, &mut matrix);

    matrix
}

// ============================================================================
// FIRST-PASSAGE MATRIX PROCESSING
// ============================================================================

/// Populate the first-passage matrix from processed windows
/// For each snapshot, track what max/min deltas were reached in remaining time
pub fn populate_first_passage_matrix(
    windows: &[FifteenMinWindow],
    matrix: &mut FirstPassageMatrix,
) {
    for window in windows {
        // Update data range
        if matrix.data_start.is_none() || window.start_time < matrix.data_start.unwrap() {
            matrix.data_start = Some(window.start_time);
        }
        if matrix.data_end.is_none() || window.start_time > matrix.data_end.unwrap() {
            matrix.data_end = Some(window.start_time);
        }

        let snapshots = &window.snapshots;

        // For each snapshot, look at remaining snapshots to find max/min
        for (i, snapshot) in snapshots.iter().enumerate() {
            // Get remaining snapshots (including current for consistency)
            let remaining = &snapshots[i..];

            if remaining.is_empty() {
                continue;
            }

            // Find max and min delta in remaining time
            let max_delta = remaining
                .iter()
                .map(|s| s.delta_from_open)
                .max()
                .unwrap_or(snapshot.delta_from_open);

            let min_delta = remaining
                .iter()
                .map(|s| s.delta_from_open)
                .min()
                .unwrap_or(snapshot.delta_from_open);

            // Record this observation
            matrix.record(
                snapshot.time_bucket,
                snapshot.delta_from_open,
                max_delta,
                min_delta,
            );
        }
    }
}

/// Build both matrices from price data
pub fn build_all_matrices(
    prices: &[PricePoint],
) -> (ProbabilityMatrix, FirstPassageMatrix) {
    let windows = process_into_windows(prices);

    let mut prob_matrix = ProbabilityMatrix::new();
    populate_matrix(&windows, &mut prob_matrix);

    let mut fp_matrix = FirstPassageMatrix::new();
    populate_first_passage_matrix(&windows, &mut fp_matrix);

    (prob_matrix, fp_matrix)
}

/// Build first-passage matrix from price data
pub fn build_first_passage_matrix(prices: &[PricePoint]) -> FirstPassageMatrix {
    let windows = process_into_windows(prices);

    let mut matrix = FirstPassageMatrix::new();
    populate_first_passage_matrix(&windows, &mut matrix);

    matrix
}

// ============================================================================
// PRICE REACH MATRIX PROCESSING
// ============================================================================

/// Populate the price reach matrix from processed windows
///
/// This uses the terminal probability matrix to look up P(UP) for each state,
/// then tracks what max prices were reached during remaining time.
pub fn populate_price_reach_matrix(
    windows: &[FifteenMinWindow],
    terminal_matrix: &ProbabilityMatrix,
    price_matrix: &mut PriceReachMatrix,
) {
    for window in windows {
        // Update data range
        if price_matrix.data_start.is_none() || window.start_time < price_matrix.data_start.unwrap() {
            price_matrix.data_start = Some(window.start_time);
        }
        if price_matrix.data_end.is_none() || window.start_time > price_matrix.data_end.unwrap() {
            price_matrix.data_end = Some(window.start_time);
        }

        let snapshots = &window.snapshots;

        // For each snapshot, look at remaining snapshots to find max P(UP) and max P(DOWN)
        for (i, snapshot) in snapshots.iter().enumerate() {
            // Get remaining snapshots (including current)
            let remaining = &snapshots[i..];

            if remaining.is_empty() {
                continue;
            }

            // For each remaining snapshot, look up P(UP) from terminal matrix
            // and track the max P(UP) and max P(DOWN) reached
            let mut max_p_up: f64 = 0.0;
            let mut max_p_down: f64 = 0.0;

            for future_snapshot in remaining {
                let future_delta_bucket = delta_to_bucket(future_snapshot.delta_from_open);
                let future_cell = terminal_matrix.get(future_snapshot.time_bucket, future_delta_bucket);

                // P(UP) and P(DOWN) from terminal matrix
                let p_up = future_cell.p_up;
                let p_down = future_cell.p_down;

                if p_up > max_p_up {
                    max_p_up = p_up;
                }
                if p_down > max_p_down {
                    max_p_down = p_down;
                }
            }

            // Record this observation in the price reach matrix
            let delta_bucket = delta_to_bucket(snapshot.delta_from_open);
            let state = price_matrix.get_mut(snapshot.time_bucket, delta_bucket);
            state.record(max_p_up, max_p_down);
            price_matrix.total_observations += 1;
        }
    }
}

/// Compute probabilities for all states in the price reach matrix
pub fn compute_price_reach_probabilities(matrix: &mut PriceReachMatrix) {
    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get_mut(time_bucket, delta_bucket);
            state.compute_probabilities();
        }
    }
}

/// Build price reach matrix from windows and terminal matrix
pub fn build_price_reach_matrix(
    windows: &[FifteenMinWindow],
    terminal_matrix: &ProbabilityMatrix,
) -> PriceReachMatrix {
    let mut matrix = PriceReachMatrix::new();
    populate_price_reach_matrix(windows, terminal_matrix, &mut matrix);
    compute_price_reach_probabilities(&mut matrix);
    matrix
}

// ============================================================================
// PRICE CROSSING MATRIX PROCESSING
// Counts how many times price crosses each level (4¢, 8¢, ..., 100¢)
// ============================================================================

/// Populate the price crossing matrix from processed windows
///
/// For each starting state (time, delta), we look at the remaining trajectory
/// of P(UP) values and count how many times each price level is crossed.
pub fn populate_price_crossing_matrix(
    windows: &[FifteenMinWindow],
    terminal_matrix: &ProbabilityMatrix,
    crossing_matrix: &mut PriceCrossingMatrix,
) {
    for window in windows {
        // Update data range
        if crossing_matrix.data_start.is_none()
            || window.start_time < crossing_matrix.data_start.unwrap()
        {
            crossing_matrix.data_start = Some(window.start_time);
        }
        if crossing_matrix.data_end.is_none()
            || window.start_time > crossing_matrix.data_end.unwrap()
        {
            crossing_matrix.data_end = Some(window.start_time);
        }

        let snapshots = &window.snapshots;

        // For each starting snapshot
        for (i, snapshot) in snapshots.iter().enumerate() {
            // Get remaining snapshots (including current)
            let remaining = &snapshots[i..];

            if remaining.len() < 2 {
                continue; // Need at least 2 points to detect crossings
            }

            // Get P(UP) for each remaining snapshot
            let mut p_ups: Vec<f64> = Vec::with_capacity(remaining.len());
            for future_snapshot in remaining {
                let future_delta_bucket = delta_to_bucket(future_snapshot.delta_from_open);
                let future_cell =
                    terminal_matrix.get(future_snapshot.time_bucket, future_delta_bucket);
                p_ups.push(future_cell.p_up);
            }

            // Count crossings between consecutive P(UP) values (directional)
            let mut trajectory_crossings_up = [0u32; 25];
            let mut trajectory_crossings_down = [0u32; 25];
            for j in 1..p_ups.len() {
                let (crossings_up, crossings_down) = count_crossings_directional(p_ups[j - 1], p_ups[j]);
                for k in 0..25 {
                    trajectory_crossings_up[k] += crossings_up[k];
                    trajectory_crossings_down[k] += crossings_down[k];
                }
            }

            // Record this trajectory in the crossing matrix (with direction)
            let delta_bucket = delta_to_bucket(snapshot.delta_from_open);
            let state = crossing_matrix.get_mut(snapshot.time_bucket, delta_bucket);
            state.record_trajectory_directional(&trajectory_crossings_up, &trajectory_crossings_down);
        }

        crossing_matrix.total_trajectories += 1;
    }
}

/// Compute averages for all states in the crossing matrix
pub fn compute_crossing_averages(matrix: &mut PriceCrossingMatrix) {
    for time_bucket in 0u8..60 {
        for delta_bucket in -17i8..=16i8 {
            let state = matrix.get_mut(time_bucket, delta_bucket);
            state.compute_averages();
        }
    }
}

/// Build price crossing matrix from windows and terminal matrix
pub fn build_price_crossing_matrix(
    windows: &[FifteenMinWindow],
    terminal_matrix: &ProbabilityMatrix,
) -> PriceCrossingMatrix {
    let mut matrix = PriceCrossingMatrix::new();
    populate_price_crossing_matrix(windows, terminal_matrix, &mut matrix);
    compute_crossing_averages(&mut matrix);
    matrix
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn test_get_window_start() {
        let ts = Utc.with_ymd_and_hms(2025, 12, 4, 8, 7, 30).unwrap();
        let window_start = get_window_start(ts);
        assert_eq!(window_start.hour(), 8);
        assert_eq!(window_start.minute(), 0);
        assert_eq!(window_start.second(), 0);

        let ts = Utc.with_ymd_and_hms(2025, 12, 4, 8, 23, 45).unwrap();
        let window_start = get_window_start(ts);
        assert_eq!(window_start.minute(), 15);

        let ts = Utc.with_ymd_and_hms(2025, 12, 4, 8, 45, 0).unwrap();
        let window_start = get_window_start(ts);
        assert_eq!(window_start.minute(), 45);
    }

    #[test]
    fn test_get_time_bucket() {
        let window_start = Utc.with_ymd_and_hms(2025, 12, 4, 8, 0, 0).unwrap();

        // 0 seconds = bucket 0
        let ts = window_start;
        assert_eq!(get_time_bucket(window_start, ts), 0);

        // 14 seconds = bucket 0
        let ts = window_start + Duration::seconds(14);
        assert_eq!(get_time_bucket(window_start, ts), 0);

        // 15 seconds = bucket 1
        let ts = window_start + Duration::seconds(15);
        assert_eq!(get_time_bucket(window_start, ts), 1);

        // 12 minutes = bucket 48
        let ts = window_start + Duration::minutes(12);
        assert_eq!(get_time_bucket(window_start, ts), 48);

        // 14:45 = bucket 59
        let ts = window_start + Duration::seconds(14 * 60 + 45);
        assert_eq!(get_time_bucket(window_start, ts), 59);
    }

    #[test]
    fn test_outcome_determination() {
        // Create mock price points
        let window_start = Utc.with_ymd_and_hms(2025, 12, 4, 8, 0, 0).unwrap();

        // Price goes up: open at 100, close at 110
        let mut points: Vec<PricePoint> = Vec::new();
        for i in 0..900 {
            // 900 seconds = 15 minutes
            let ts = window_start + Duration::seconds(i);
            let price = dec!(100) + Decimal::from(i) / dec!(90); // Gradual increase
            points.push(PricePoint {
                timestamp: ts,
                close_price: price,
            });
        }

        let windows = process_into_windows(&points);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].outcome, Outcome::Up);
    }
}
