//! Algorithm Backtester - Compare 7 probability models for BTC 15-minute binary options
//!
//! Usage:
//!   cargo run --bin algo-backtester -- --start-date 2024-11-01 --end-date 2024-12-01

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use statrs::distribution::{ContinuousCDF, Normal};
use std::fs::File;
use std::io::Write;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tracing::info;

// =============================================================================
// CLI Arguments
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "algo-backtester")]
#[command(about = "Compare probability models for BTC 15-minute binary options")]
struct Args {
    /// Start date (YYYY-MM-DD)
    #[arg(long)]
    start_date: String,

    /// End date (YYYY-MM-DD)
    #[arg(long)]
    end_date: String,

    /// Output CSV file path
    #[arg(long, default_value = "results/algo_backtest_results.csv")]
    output: String,

    /// Models to test (comma-separated). Default: all
    #[arg(long)]
    models: Option<String>,

    /// Sample rate: evaluate every N seconds (default: 15)
    #[arg(long, default_value = "15")]
    sample_rate: u32,

    /// Verbose output
    #[arg(long, short)]
    verbose: bool,
}

// =============================================================================
// Data Structures
// =============================================================================

/// Outcome of a 15-minute window
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Up,
    Down,
}

/// Raw price point from database
#[derive(Debug, Clone)]
pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub price: f64,
}

/// Enriched snapshot with all derived features
#[derive(Debug, Clone)]
pub struct EnrichedSnapshot {
    pub window_id: String,
    pub time_elapsed: u32,       // 0-900 seconds
    pub time_remaining: u32,     // 900 - time_elapsed

    // Core
    pub btc_price: f64,
    pub open_price: f64,
    pub price_delta: f64,        // btc_price - open_price

    // Velocity/Momentum
    pub velocity_10s: f64,       // delta[t] - delta[t-10]
    pub velocity_30s: f64,       // delta[t] - delta[t-30]
    pub acceleration: f64,       // velocity[t] - velocity[t-10]

    // Path statistics
    pub max_delta_so_far: f64,
    pub min_delta_so_far: f64,
    pub zero_crossings: u32,

    // Volatility
    pub volatility_1m: f64,      // rolling 60s stddev
    pub volatility_5m: f64,      // rolling 300s stddev
    pub vol_ratio: f64,          // vol_1m / vol_5m
}

/// A complete 15-minute window with outcome
#[derive(Debug, Clone)]
pub struct BacktestWindow {
    pub window_id: String,
    pub start_time: DateTime<Utc>,
    pub open_price: f64,
    pub close_price: f64,
    pub outcome: Outcome,
    pub snapshots: Vec<EnrichedSnapshot>,
}

/// Single prediction from a model
#[derive(Debug, Clone)]
pub struct Prediction {
    pub window_id: String,
    pub time_elapsed: u32,
    pub p_up: f64,
    pub actual_outcome: Outcome,
    pub correct: bool,
}

/// Calibration bucket for evaluating model reliability
#[derive(Debug, Clone, Serialize)]
pub struct CalibrationBucket {
    pub predicted_range: String,
    pub predicted_avg: f64,
    pub actual_win_rate: f64,
    pub count: u32,
}

/// Results for a single model
#[derive(Debug, Clone, Serialize)]
pub struct ModelResult {
    pub model_name: String,
    pub total_predictions: u32,
    pub accuracy: f64,
    pub brier_score: f64,
    pub accuracy_p55: f64,      // Accuracy when P > 0.55
    pub accuracy_p60: f64,      // Accuracy when P > 0.60
    pub accuracy_p65: f64,      // Accuracy when P > 0.65
    pub count_p55: u32,
    pub count_p60: u32,
    pub count_p65: u32,
    pub calibration_error: f64,
    pub calibration: Vec<CalibrationBucket>,
}

// =============================================================================
// Probability Model Trait
// =============================================================================

pub trait ProbabilityModel: Send + Sync {
    fn name(&self) -> &str;
    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64;

    /// Optional: train the model on data (for ML models)
    fn train(&mut self, _windows: &[BacktestWindow]) {
        // Default: no training needed
    }
}

// =============================================================================
// Model 1: Current Matrix (Baseline)
// =============================================================================

pub struct CurrentMatrixModel {
    /// cells[time_bucket][delta_bucket+17] = (count_up, count_down)
    cells: Vec<Vec<(u32, u32)>>,
}

impl CurrentMatrixModel {
    pub fn new() -> Self {
        // Initialize 60 time buckets x 34 delta buckets
        let cells = (0..60)
            .map(|_| vec![(0u32, 0u32); 34])
            .collect();
        Self { cells }
    }

    fn delta_to_bucket(delta: f64) -> i8 {
        if delta < 0.0 {
            if delta < -300.0 { -17 }
            else if delta < -260.0 { -16 }
            else if delta < -230.0 { -15 }
            else if delta < -200.0 { -14 }
            else if delta < -170.0 { -13 }
            else if delta < -140.0 { -12 }
            else if delta < -110.0 { -11 }
            else if delta < -90.0 { -10 }
            else if delta < -70.0 { -9 }
            else if delta < -50.0 { -8 }
            else if delta < -40.0 { -7 }
            else if delta < -30.0 { -6 }
            else if delta < -20.0 { -5 }
            else if delta < -15.0 { -4 }
            else if delta < -10.0 { -3 }
            else if delta < -5.0 { -2 }
            else { -1 }
        } else {
            if delta < 5.0 { 0 }
            else if delta < 10.0 { 1 }
            else if delta < 15.0 { 2 }
            else if delta < 20.0 { 3 }
            else if delta < 30.0 { 4 }
            else if delta < 40.0 { 5 }
            else if delta < 50.0 { 6 }
            else if delta < 70.0 { 7 }
            else if delta < 90.0 { 8 }
            else if delta < 110.0 { 9 }
            else if delta < 140.0 { 10 }
            else if delta < 170.0 { 11 }
            else if delta < 200.0 { 12 }
            else if delta < 230.0 { 13 }
            else if delta < 260.0 { 14 }
            else if delta < 300.0 { 15 }
            else { 16 }
        }
    }
}

impl ProbabilityModel for CurrentMatrixModel {
    fn name(&self) -> &str {
        "current_matrix"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
        let delta_bucket = Self::delta_to_bucket(snapshot.price_delta);
        let idx = (delta_bucket + 17) as usize;

        let (up, down) = self.cells[time_bucket][idx];
        let total = up + down;
        if total == 0 {
            0.5 // No data, return 50%
        } else {
            up as f64 / total as f64
        }
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        // Build matrix from training data
        for window in windows {
            for snapshot in &window.snapshots {
                let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
                let delta_bucket = Self::delta_to_bucket(snapshot.price_delta);
                let idx = (delta_bucket + 17) as usize;

                match window.outcome {
                    Outcome::Up => self.cells[time_bucket][idx].0 += 1,
                    Outcome::Down => self.cells[time_bucket][idx].1 += 1,
                }
            }
        }
    }
}

// =============================================================================
// Model 2: Velocity Matrix (3D)
// =============================================================================

pub struct VelocityMatrixModel {
    /// cells[time_bucket][delta_bucket+17][velocity_sign+1] = (count_up, count_down)
    /// velocity_sign: -1 (falling), 0 (neutral), +1 (rising)
    cells: Vec<Vec<Vec<(u32, u32)>>>,
}

impl VelocityMatrixModel {
    pub fn new() -> Self {
        let cells = (0..60)
            .map(|_| (0..34).map(|_| vec![(0u32, 0u32); 3]).collect())
            .collect();
        Self { cells }
    }

    fn velocity_to_sign(velocity: f64) -> i8 {
        if velocity > 5.0 { 1 }
        else if velocity < -5.0 { -1 }
        else { 0 }
    }
}

impl ProbabilityModel for VelocityMatrixModel {
    fn name(&self) -> &str {
        "velocity_matrix"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
        let delta_bucket = CurrentMatrixModel::delta_to_bucket(snapshot.price_delta);
        let delta_idx = (delta_bucket + 17) as usize;
        let vel_sign = Self::velocity_to_sign(snapshot.velocity_10s);
        let vel_idx = (vel_sign + 1) as usize;

        let (up, down) = self.cells[time_bucket][delta_idx][vel_idx];
        let total = up + down;
        if total < 5 {
            // Fall back to 2D matrix if not enough data
            let total_2d: (u32, u32) = self.cells[time_bucket][delta_idx]
                .iter()
                .fold((0, 0), |acc, &(u, d)| (acc.0 + u, acc.1 + d));
            if total_2d.0 + total_2d.1 == 0 {
                0.5
            } else {
                total_2d.0 as f64 / (total_2d.0 + total_2d.1) as f64
            }
        } else {
            up as f64 / total as f64
        }
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        for window in windows {
            for snapshot in &window.snapshots {
                let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
                let delta_bucket = CurrentMatrixModel::delta_to_bucket(snapshot.price_delta);
                let delta_idx = (delta_bucket + 17) as usize;
                let vel_sign = Self::velocity_to_sign(snapshot.velocity_10s);
                let vel_idx = (vel_sign + 1) as usize;

                match window.outcome {
                    Outcome::Up => self.cells[time_bucket][delta_idx][vel_idx].0 += 1,
                    Outcome::Down => self.cells[time_bucket][delta_idx][vel_idx].1 += 1,
                }
            }
        }
    }
}

// =============================================================================
// Model 3: Brownian Bridge
// =============================================================================

pub struct BrownianBridgeModel {
    /// Average volatility from training data ($ per second)
    avg_volatility: f64,
}

impl BrownianBridgeModel {
    pub fn new() -> Self {
        Self { avg_volatility: 0.5 } // Default, will be calibrated
    }
}

impl ProbabilityModel for BrownianBridgeModel {
    fn name(&self) -> &str {
        "brownian_bridge"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let t = snapshot.time_remaining as f64 / 900.0;
        if t <= 0.0 {
            return if snapshot.price_delta > 0.0 { 1.0 } else { 0.0 };
        }

        // Use snapshot volatility if available, otherwise use trained average
        let sigma = if snapshot.volatility_5m > 0.0 {
            snapshot.volatility_5m
        } else {
            self.avg_volatility
        };

        let std_dev = sigma * t.sqrt();
        if std_dev <= 0.0 {
            return if snapshot.price_delta > 0.0 { 1.0 } else { 0.0 };
        }

        // P(end > 0 | current = delta) using normal CDF
        let normal = Normal::new(0.0, 1.0).unwrap();
        let z = snapshot.price_delta / std_dev;
        normal.cdf(z).clamp(0.01, 0.99)
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        // Calculate average volatility from training data
        let mut total_vol = 0.0;
        let mut count = 0;

        for window in windows {
            for snapshot in &window.snapshots {
                if snapshot.volatility_5m > 0.0 {
                    total_vol += snapshot.volatility_5m;
                    count += 1;
                }
            }
        }

        if count > 0 {
            self.avg_volatility = total_vol / count as f64;
        }
    }
}

// =============================================================================
// Model 4: Black-Scholes Digital
// =============================================================================

pub struct BlackScholesDigitalModel {
    avg_volatility: f64,
}

impl BlackScholesDigitalModel {
    pub fn new() -> Self {
        Self { avg_volatility: 0.5 }
    }
}

impl ProbabilityModel for BlackScholesDigitalModel {
    fn name(&self) -> &str {
        "black_scholes_digital"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let t = snapshot.time_remaining as f64 / 900.0;
        if t <= 0.0 {
            return if snapshot.price_delta > 0.0 { 1.0 } else { 0.0 };
        }

        let sigma = if snapshot.volatility_5m > 0.0 {
            snapshot.volatility_5m / snapshot.open_price  // Normalize to percentage
        } else {
            self.avg_volatility / 100000.0
        };

        let sigma_t = sigma * t.sqrt();
        if sigma_t <= 0.0 {
            return if snapshot.price_delta > 0.0 { 1.0 } else { 0.0 };
        }

        // d2 for digital option
        let d2 = snapshot.price_delta / (snapshot.open_price * sigma_t);

        let normal = Normal::new(0.0, 1.0).unwrap();
        normal.cdf(d2).clamp(0.01, 0.99)
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        let mut total_vol = 0.0;
        let mut count = 0;

        for window in windows {
            for snapshot in &window.snapshots {
                if snapshot.volatility_5m > 0.0 {
                    total_vol += snapshot.volatility_5m;
                    count += 1;
                }
            }
        }

        if count > 0 {
            self.avg_volatility = total_vol / count as f64;
        }
    }
}

// =============================================================================
// Model 5: Ornstein-Uhlenbeck with Momentum
// =============================================================================

pub struct OrnsteinUhlenbeckModel {
    theta_momentum: f64,  // Momentum weight
    theta_revert: f64,    // Mean reversion weight
    mu: f64,              // Long-term mean delta
    avg_volatility: f64,
}

impl OrnsteinUhlenbeckModel {
    pub fn new() -> Self {
        Self {
            theta_momentum: 0.3,
            theta_revert: 0.1,
            mu: 0.0,
            avg_volatility: 0.5,
        }
    }
}

impl ProbabilityModel for OrnsteinUhlenbeckModel {
    fn name(&self) -> &str {
        "ornstein_uhlenbeck"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let t = snapshot.time_remaining as f64 / 900.0;
        if t <= 0.0 {
            return if snapshot.price_delta > 0.0 { 1.0 } else { 0.0 };
        }

        // Expected drift based on momentum and mean reversion
        let drift = self.theta_momentum * snapshot.velocity_10s
                  + self.theta_revert * (self.mu - snapshot.price_delta);

        // Expected final delta
        let expected_delta = snapshot.price_delta + drift * t * 100.0;

        let sigma = if snapshot.volatility_5m > 0.0 {
            snapshot.volatility_5m
        } else {
            self.avg_volatility
        };

        let sigma_t = sigma * t.sqrt();
        if sigma_t <= 0.0 {
            return if expected_delta > 0.0 { 1.0 } else { 0.0 };
        }

        let normal = Normal::new(0.0, 1.0).unwrap();
        let z = expected_delta / sigma_t;
        normal.cdf(z).clamp(0.01, 0.99)
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        let mut total_vol = 0.0;
        let mut count = 0;

        for window in windows {
            for snapshot in &window.snapshots {
                if snapshot.volatility_5m > 0.0 {
                    total_vol += snapshot.volatility_5m;
                    count += 1;
                }
            }
        }

        if count > 0 {
            self.avg_volatility = total_vol / count as f64;
        }
    }
}

// =============================================================================
// Model 6: Regime-Switching
// =============================================================================

#[derive(Debug, Clone, Copy)]
enum Regime {
    Trending,
    MeanReverting,
}

pub struct RegimeSwitchingModel {
    base_cells: Vec<Vec<(u32, u32)>>,  // Fallback matrix
}

impl RegimeSwitchingModel {
    pub fn new() -> Self {
        let base_cells = (0..60)
            .map(|_| vec![(0u32, 0u32); 34])
            .collect();
        Self { base_cells }
    }

    fn detect_regime(vol_ratio: f64) -> Regime {
        if vol_ratio > 1.5 {
            Regime::Trending
        } else {
            Regime::MeanReverting
        }
    }
}

impl ProbabilityModel for RegimeSwitchingModel {
    fn name(&self) -> &str {
        "regime_switching"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        // Get base probability from matrix
        let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
        let delta_bucket = CurrentMatrixModel::delta_to_bucket(snapshot.price_delta);
        let idx = (delta_bucket + 17) as usize;

        let (up, down) = self.base_cells[time_bucket][idx];
        let total = up + down;
        let base_p = if total == 0 { 0.5 } else { up as f64 / total as f64 };

        // Adjust based on regime
        let regime = Self::detect_regime(snapshot.vol_ratio);
        match regime {
            Regime::Trending => {
                // In trending regime, follow momentum more
                let momentum_boost = if snapshot.velocity_10s > 0.0 { 0.08 } else { -0.08 };
                (base_p + momentum_boost).clamp(0.01, 0.99)
            }
            Regime::MeanReverting => {
                // In mean-reverting regime, expect price to come back
                base_p
            }
        }
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        for window in windows {
            for snapshot in &window.snapshots {
                let time_bucket = (snapshot.time_elapsed / 15).min(59) as usize;
                let delta_bucket = CurrentMatrixModel::delta_to_bucket(snapshot.price_delta);
                let idx = (delta_bucket + 17) as usize;

                match window.outcome {
                    Outcome::Up => self.base_cells[time_bucket][idx].0 += 1,
                    Outcome::Down => self.base_cells[time_bucket][idx].1 += 1,
                }
            }
        }
    }
}

// =============================================================================
// Model 7: Logistic Regression
// =============================================================================

pub struct LogisticRegressionModel {
    bias: f64,
    weights: Vec<f64>,  // 9 features
}

impl LogisticRegressionModel {
    pub fn new() -> Self {
        Self {
            bias: 0.0,
            weights: vec![0.0; 9],
        }
    }

    fn extract_features(snapshot: &EnrichedSnapshot) -> Vec<f64> {
        vec![
            snapshot.price_delta / 100.0,
            snapshot.velocity_10s / 50.0,
            snapshot.velocity_30s / 50.0,
            snapshot.acceleration / 20.0,
            (snapshot.max_delta_so_far - snapshot.price_delta) / 100.0,
            (snapshot.price_delta - snapshot.min_delta_so_far) / 100.0,
            snapshot.zero_crossings as f64 / 10.0,
            snapshot.vol_ratio - 1.0,
            (snapshot.time_remaining as f64 / 900.0) - 0.5,
        ]
    }

    fn sigmoid(x: f64) -> f64 {
        1.0 / (1.0 + (-x).exp())
    }
}

impl ProbabilityModel for LogisticRegressionModel {
    fn name(&self) -> &str {
        "logistic_regression"
    }

    fn calculate_p_up(&self, snapshot: &EnrichedSnapshot) -> f64 {
        let features = Self::extract_features(snapshot);
        let z = self.bias + self.weights.iter()
            .zip(&features)
            .map(|(w, f)| w * f)
            .sum::<f64>();
        Self::sigmoid(z).clamp(0.01, 0.99)
    }

    fn train(&mut self, windows: &[BacktestWindow]) {
        // Simple gradient descent
        let learning_rate = 0.01;
        let epochs = 100;

        // Collect all training samples
        let mut samples: Vec<(Vec<f64>, f64)> = Vec::new();
        for window in windows {
            let y = if window.outcome == Outcome::Up { 1.0 } else { 0.0 };
            for snapshot in &window.snapshots {
                samples.push((Self::extract_features(snapshot), y));
            }
        }

        if samples.is_empty() {
            return;
        }

        // Mini-batch gradient descent
        for _epoch in 0..epochs {
            let mut grad_bias = 0.0;
            let mut grad_weights = vec![0.0; 9];

            for (features, y) in &samples {
                let z = self.bias + self.weights.iter()
                    .zip(features)
                    .map(|(w, f)| w * f)
                    .sum::<f64>();
                let p = Self::sigmoid(z);
                let error = p - y;

                grad_bias += error;
                for (i, f) in features.iter().enumerate() {
                    grad_weights[i] += error * f;
                }
            }

            let n = samples.len() as f64;
            self.bias -= learning_rate * grad_bias / n;
            for i in 0..9 {
                self.weights[i] -= learning_rate * grad_weights[i] / n;
            }
        }
    }
}

// =============================================================================
// Feature Engineering
// =============================================================================

fn build_enriched_windows(prices: Vec<PricePoint>) -> Vec<BacktestWindow> {
    let mut windows: Vec<BacktestWindow> = Vec::new();

    if prices.is_empty() {
        return windows;
    }

    // Group prices into 15-minute windows (aligned to :00, :15, :30, :45)
    let mut current_window: Option<BacktestWindow> = None;
    let mut window_prices: Vec<PricePoint> = Vec::new();

    for price in prices {
        let window_start = align_to_15min(price.timestamp);

        if let Some(ref mut win) = current_window {
            if win.start_time == window_start {
                window_prices.push(price);
            } else {
                // Finish current window - UPDATE close_price from last price!
                if window_prices.len() >= 10 {
                    if let Some(last) = window_prices.last() {
                        win.close_price = last.price;
                    }
                    let enriched = build_window_snapshots(&window_prices, win);
                    win.snapshots = enriched;
                    windows.push(win.clone());
                }

                // Start new window
                current_window = Some(BacktestWindow {
                    window_id: window_start.format("%Y-%m-%d_%H:%M").to_string(),
                    start_time: window_start,
                    open_price: price.price,
                    close_price: price.price,
                    outcome: Outcome::Up,
                    snapshots: Vec::new(),
                });
                window_prices = vec![price];
            }
        } else {
            current_window = Some(BacktestWindow {
                window_id: window_start.format("%Y-%m-%d_%H:%M").to_string(),
                start_time: window_start,
                open_price: price.price,
                close_price: price.price,
                outcome: Outcome::Up,
                snapshots: Vec::new(),
            });
            window_prices = vec![price];
        }
    }

    // Handle last window
    if let Some(mut win) = current_window {
        if window_prices.len() >= 10 {
            let enriched = build_window_snapshots(&window_prices, &win);
            win.snapshots = enriched;

            // Set outcome based on close vs open
            if let Some(last) = window_prices.last() {
                win.close_price = last.price;
                win.outcome = if win.close_price >= win.open_price {
                    Outcome::Up
                } else {
                    Outcome::Down
                };
            }

            windows.push(win);
        }
    }

    // Set outcomes for all windows
    for window in &mut windows {
        window.outcome = if window.close_price >= window.open_price {
            Outcome::Up
        } else {
            Outcome::Down
        };
    }

    windows
}

fn align_to_15min(ts: DateTime<Utc>) -> DateTime<Utc> {
    let minute = ts.minute();
    let aligned_minute = (minute / 15) * 15;
    ts.with_minute(aligned_minute)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}

fn build_window_snapshots(prices: &[PricePoint], window: &BacktestWindow) -> Vec<EnrichedSnapshot> {
    let mut snapshots = Vec::new();
    let open_price = window.open_price;

    // Pre-compute deltas for velocity/volatility
    let deltas: Vec<f64> = prices.iter()
        .map(|p| p.price - open_price)
        .collect();

    let mut max_delta = f64::MIN;
    let mut min_delta = f64::MAX;
    let mut zero_crossings = 0u32;
    let mut prev_sign = 0i32;

    for (i, price) in prices.iter().enumerate() {
        let time_elapsed = (price.timestamp - window.start_time).num_seconds() as u32;
        if time_elapsed > 900 {
            continue;
        }

        let delta = deltas[i];

        // Track path statistics
        max_delta = max_delta.max(delta);
        min_delta = min_delta.min(delta);

        let sign = if delta > 0.0 { 1 } else if delta < 0.0 { -1 } else { 0 };
        if prev_sign != 0 && sign != 0 && sign != prev_sign {
            zero_crossings += 1;
        }
        prev_sign = sign;

        // Velocity (10s and 30s lookback)
        let velocity_10s = if i >= 10 { delta - deltas[i - 10] } else { 0.0 };
        let velocity_30s = if i >= 30 { delta - deltas[i - 30] } else { 0.0 };

        // Acceleration
        let velocity_prev = if i >= 10 && i >= 20 {
            deltas[i - 10] - deltas[i - 20]
        } else {
            0.0
        };
        let acceleration = velocity_10s - velocity_prev;

        // Volatility (rolling stddev)
        let volatility_1m = if i >= 60 {
            rolling_stddev(&deltas[i-60..=i])
        } else {
            rolling_stddev(&deltas[0..=i])
        };

        let volatility_5m = if i >= 300 {
            rolling_stddev(&deltas[i-300..=i])
        } else {
            rolling_stddev(&deltas[0..=i])
        };

        let vol_ratio = if volatility_5m > 0.0 {
            volatility_1m / volatility_5m
        } else {
            1.0
        };

        snapshots.push(EnrichedSnapshot {
            window_id: window.window_id.clone(),
            time_elapsed,
            time_remaining: 900 - time_elapsed,
            btc_price: price.price,
            open_price,
            price_delta: delta,
            velocity_10s,
            velocity_30s,
            acceleration,
            max_delta_so_far: max_delta,
            min_delta_so_far: min_delta,
            zero_crossings,
            volatility_1m,
            volatility_5m,
            vol_ratio,
        });
    }

    snapshots
}

fn rolling_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    variance.sqrt()
}

// =============================================================================
// Database Loading
// =============================================================================

async fn load_prices_from_db(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PricePoint>> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| {
            "host=zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com \
             port=5432 \
             dbname=polymarket \
             user=qoveryadmin \
             password=xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp".to_string()
        });

    // Create TLS connector that accepts invalid certs (for internal RDS)
    let tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let tls = MakeTlsConnector::new(tls_connector);

    let (client, connection) = tokio_postgres::connect(&db_url, tls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let rows = client
        .query(
            "SELECT timestamp, close_price
             FROM binance_prices
             WHERE symbol = 'BTCUSDT'
               AND timestamp >= $1
               AND timestamp < $2
             ORDER BY timestamp ASC",
            &[&start_date, &end_date],
        )
        .await
        .context("Failed to fetch prices")?;

    let prices: Vec<PricePoint> = rows
        .iter()
        .map(|row| {
            let timestamp: DateTime<Utc> = row.get(0);
            let price: Decimal = row.get(1);
            PricePoint {
                timestamp,
                price: price.to_string().parse().unwrap_or(0.0),
            }
        })
        .collect();

    Ok(prices)
}

// =============================================================================
// Evaluation
// =============================================================================

fn evaluate_model(
    model: &dyn ProbabilityModel,
    windows: &[BacktestWindow],
    sample_rate: u32,
) -> ModelResult {
    let mut predictions = Vec::new();

    for window in windows {
        for snapshot in &window.snapshots {
            // Sample at specified rate
            if snapshot.time_elapsed % sample_rate != 0 {
                continue;
            }

            let p_up = model.calculate_p_up(snapshot);
            let predicted_up = p_up > 0.5;
            let actual_up = window.outcome == Outcome::Up;

            predictions.push(Prediction {
                window_id: window.window_id.clone(),
                time_elapsed: snapshot.time_elapsed,
                p_up,
                actual_outcome: window.outcome,
                correct: predicted_up == actual_up,
            });
        }
    }

    if predictions.is_empty() {
        return ModelResult {
            model_name: model.name().to_string(),
            total_predictions: 0,
            accuracy: 0.0,
            brier_score: 0.0,
            accuracy_p55: 0.0,
            accuracy_p60: 0.0,
            accuracy_p65: 0.0,
            count_p55: 0,
            count_p60: 0,
            count_p65: 0,
            calibration_error: 0.0,
            calibration: Vec::new(),
        };
    }

    // Basic accuracy
    let correct = predictions.iter().filter(|p| p.correct).count();
    let accuracy = correct as f64 / predictions.len() as f64;

    // Brier score
    let brier_score = predictions.iter()
        .map(|p| {
            let y = if p.actual_outcome == Outcome::Up { 1.0 } else { 0.0 };
            (p.p_up - y).powi(2)
        })
        .sum::<f64>() / predictions.len() as f64;

    // Accuracy at different confidence thresholds
    let p55: Vec<_> = predictions.iter().filter(|p| p.p_up > 0.55 || p.p_up < 0.45).collect();
    let p60: Vec<_> = predictions.iter().filter(|p| p.p_up > 0.60 || p.p_up < 0.40).collect();
    let p65: Vec<_> = predictions.iter().filter(|p| p.p_up > 0.65 || p.p_up < 0.35).collect();

    let accuracy_p55 = if p55.is_empty() { 0.0 }
        else { p55.iter().filter(|p| p.correct).count() as f64 / p55.len() as f64 };
    let accuracy_p60 = if p60.is_empty() { 0.0 }
        else { p60.iter().filter(|p| p.correct).count() as f64 / p60.len() as f64 };
    let accuracy_p65 = if p65.is_empty() { 0.0 }
        else { p65.iter().filter(|p| p.correct).count() as f64 / p65.len() as f64 };

    // Calibration buckets
    let mut calibration = Vec::new();
    let buckets = [
        (0.0, 0.35, "0-35%"),
        (0.35, 0.45, "35-45%"),
        (0.45, 0.55, "45-55%"),
        (0.55, 0.65, "55-65%"),
        (0.65, 1.0, "65-100%"),
    ];

    let mut calibration_error = 0.0;
    for (low, high, label) in buckets {
        let bucket_preds: Vec<_> = predictions.iter()
            .filter(|p| p.p_up >= low && p.p_up < high)
            .collect();

        if !bucket_preds.is_empty() {
            let predicted_avg = bucket_preds.iter().map(|p| p.p_up).sum::<f64>() / bucket_preds.len() as f64;
            let actual_up = bucket_preds.iter().filter(|p| p.actual_outcome == Outcome::Up).count();
            let actual_win_rate = actual_up as f64 / bucket_preds.len() as f64;

            calibration_error += (predicted_avg - actual_win_rate).abs() * bucket_preds.len() as f64;

            calibration.push(CalibrationBucket {
                predicted_range: label.to_string(),
                predicted_avg,
                actual_win_rate,
                count: bucket_preds.len() as u32,
            });
        }
    }
    calibration_error /= predictions.len() as f64;

    ModelResult {
        model_name: model.name().to_string(),
        total_predictions: predictions.len() as u32,
        accuracy,
        brier_score,
        accuracy_p55,
        accuracy_p60,
        accuracy_p65,
        count_p55: p55.len() as u32,
        count_p60: p60.len() as u32,
        count_p65: p65.len() as u32,
        calibration_error,
        calibration,
    }
}

// =============================================================================
// Output
// =============================================================================

fn write_results_csv(results: &[ModelResult], path: &str) -> Result<()> {
    let mut file = File::create(path)?;

    writeln!(file, "model_name,total_predictions,accuracy,brier_score,accuracy_p55,count_p55,accuracy_p60,count_p60,accuracy_p65,count_p65,calibration_error")?;

    for r in results {
        writeln!(
            file,
            "{},{},{:.4},{:.4},{:.4},{},{:.4},{},{:.4},{},{:.4}",
            r.model_name,
            r.total_predictions,
            r.accuracy,
            r.brier_score,
            r.accuracy_p55,
            r.count_p55,
            r.accuracy_p60,
            r.count_p60,
            r.accuracy_p65,
            r.count_p65,
            r.calibration_error
        )?;
    }

    Ok(())
}

fn write_calibration_csv(results: &[ModelResult], path: &str) -> Result<()> {
    let mut file = File::create(path)?;

    writeln!(file, "model_name,p_bucket,predicted_avg,actual_win_rate,count")?;

    for r in results {
        for c in &r.calibration {
            writeln!(
                file,
                "{},{},{:.4},{:.4},{}",
                r.model_name,
                c.predicted_range,
                c.predicted_avg,
                c.actual_win_rate,
                c.count
            )?;
        }
    }

    Ok(())
}

fn print_results_table(results: &[ModelResult]) {
    println!("\n{:=<100}", "");
    println!("ALGORITHM COMPARISON RESULTS");
    println!("{:=<100}", "");
    println!(
        "{:<25} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Model", "Accuracy", "Brier", "Acc@55%", "Acc@60%", "Acc@65%", "CalibErr"
    );
    println!("{:-<100}", "");

    for r in results {
        println!(
            "{:<25} {:>10.2}% {:>10.4} {:>10.2}% {:>10.2}% {:>10.2}% {:>10.4}",
            r.model_name,
            r.accuracy * 100.0,
            r.brier_score,
            r.accuracy_p55 * 100.0,
            r.accuracy_p60 * 100.0,
            r.accuracy_p65 * 100.0,
            r.calibration_error
        );
    }
    println!("{:=<100}\n", "");
}

// =============================================================================
// Main
// =============================================================================

use chrono::Timelike;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let args = Args::parse();

    // Parse dates
    let start_date = NaiveDate::parse_from_str(&args.start_date, "%Y-%m-%d")?;
    let end_date = NaiveDate::parse_from_str(&args.end_date, "%Y-%m-%d")?;

    let start_dt = Utc.from_utc_datetime(&start_date.and_hms_opt(0, 0, 0).unwrap());
    let end_dt = Utc.from_utc_datetime(&end_date.and_hms_opt(0, 0, 0).unwrap());

    info!("Loading price data from {} to {}", start_date, end_date);
    let prices = load_prices_from_db(start_dt, end_dt).await?;
    info!("Loaded {} price points", prices.len());

    info!("Building enriched windows...");
    let windows = build_enriched_windows(prices);
    info!("Built {} windows with {} total snapshots",
          windows.len(),
          windows.iter().map(|w| w.snapshots.len()).sum::<usize>());

    // Split into train (80%) and test (20%)
    let split_idx = (windows.len() as f64 * 0.8) as usize;
    let (train_windows, test_windows) = windows.split_at(split_idx);
    info!("Train: {} windows, Test: {} windows", train_windows.len(), test_windows.len());

    // Initialize models
    let mut models: Vec<Box<dyn ProbabilityModel>> = vec![
        Box::new(CurrentMatrixModel::new()),
        Box::new(VelocityMatrixModel::new()),
        Box::new(BrownianBridgeModel::new()),
        Box::new(BlackScholesDigitalModel::new()),
        Box::new(OrnsteinUhlenbeckModel::new()),
        Box::new(RegimeSwitchingModel::new()),
        Box::new(LogisticRegressionModel::new()),
    ];

    // Filter models if specified
    let selected_models: Option<Vec<&str>> = args.models.as_ref()
        .map(|m| m.split(',').collect());

    // Train models
    info!("Training models...");
    for model in &mut models {
        if let Some(ref selected) = selected_models {
            if !selected.contains(&model.name()) {
                continue;
            }
        }
        info!("Training {}...", model.name());
        model.train(train_windows);
    }

    // Evaluate models
    info!("Evaluating models on test set...");
    let mut results = Vec::new();
    for model in &models {
        if let Some(ref selected) = selected_models {
            if !selected.contains(&model.name()) {
                continue;
            }
        }
        info!("Evaluating {}...", model.name());
        let result = evaluate_model(model.as_ref(), test_windows, args.sample_rate);
        results.push(result);
    }

    // Sort by accuracy descending
    results.sort_by(|a, b| b.accuracy.partial_cmp(&a.accuracy).unwrap());

    // Output results
    print_results_table(&results);

    // Create output directory if needed
    if let Some(parent) = std::path::Path::new(&args.output).parent() {
        std::fs::create_dir_all(parent)?;
    }

    write_results_csv(&results, &args.output)?;
    info!("Results written to {}", args.output);

    let calibration_path = args.output.replace(".csv", "_calibration.csv");
    write_calibration_csv(&results, &calibration_path)?;
    info!("Calibration data written to {}", calibration_path);

    // Print best model
    if let Some(best) = results.first() {
        println!("\nBest model: {} with {:.2}% accuracy", best.model_name, best.accuracy * 100.0);
    }

    Ok(())
}
