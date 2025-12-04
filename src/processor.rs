use chrono::{DateTime, Duration, Timelike, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::models::{
    delta_to_bucket, FifteenMinWindow, Outcome, PricePoint, PriceSnapshot, ProbabilityMatrix,
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
            if points.len() < 30 {
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

/// Get the time bucket (0-29) for a timestamp within a 15-minute window
pub fn get_time_bucket(window_start: DateTime<Utc>, timestamp: DateTime<Utc>) -> u8 {
    let elapsed = timestamp.signed_duration_since(window_start);
    let seconds = elapsed.num_seconds();

    // Each bucket is 30 seconds
    let bucket = (seconds / 30).min(29).max(0) as u8;
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

    // Build snapshots for each 30-second bucket
    let mut snapshots = Vec::with_capacity(30);

    for bucket in 0u8..30 {
        let bucket_start = start_time + Duration::seconds(bucket as i64 * 30);
        let bucket_end = bucket_start + Duration::seconds(30);

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

        // 29 seconds = bucket 0
        let ts = window_start + Duration::seconds(29);
        assert_eq!(get_time_bucket(window_start, ts), 0);

        // 30 seconds = bucket 1
        let ts = window_start + Duration::seconds(30);
        assert_eq!(get_time_bucket(window_start, ts), 1);

        // 12 minutes = bucket 24
        let ts = window_start + Duration::minutes(12);
        assert_eq!(get_time_bucket(window_start, ts), 24);

        // 14:30 = bucket 29
        let ts = window_start + Duration::seconds(14 * 60 + 30);
        assert_eq!(get_time_bucket(window_start, ts), 29);
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
