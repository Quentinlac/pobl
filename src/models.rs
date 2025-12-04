use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Raw price data from Binance (1-second candles)
#[derive(Debug, Clone)]
pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub close_price: Decimal,
}

/// Outcome of a 15-minute window
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Up,
    Down,
}

/// A 15-minute betting window with all its price snapshots
#[derive(Debug, Clone)]
pub struct FifteenMinWindow {
    pub start_time: DateTime<Utc>,
    pub open_price: Decimal,
    pub close_price: Decimal,
    pub outcome: Outcome,
    /// Price at each 30-second interval (30 snapshots)
    pub snapshots: Vec<PriceSnapshot>,
}

/// A snapshot within a 15-minute window
#[derive(Debug, Clone)]
pub struct PriceSnapshot {
    /// 0-29 (which 30-second bucket)
    pub time_bucket: u8,
    /// Price at this moment
    pub price: Decimal,
    /// Delta from window open price (can be negative)
    pub delta_from_open: Decimal,
}

/// Time bucket (0-29, representing 30-second intervals within 15 minutes)
pub const TIME_BUCKETS: u8 = 30;

/// Price delta bucket boundaries in dollars
/// Bucket -6: < -300
/// Bucket -5: -300 to -200
/// Bucket -4: -200 to -100
/// Bucket -3: -100 to -50
/// Bucket -2: -50 to -20
/// Bucket -1: -20 to 0
/// Bucket  0: 0 to 20
/// Bucket +1: 20 to 50
/// Bucket +2: 50 to 100
/// Bucket +3: 100 to 200
/// Bucket +4: 200 to 300
/// Bucket +5: > 300
pub const PRICE_DELTA_BUCKETS: i8 = 13; // -6 to +6

/// Map a price delta (in dollars) to a bucket index (-6 to +6)
pub fn delta_to_bucket(delta: Decimal) -> i8 {
    let delta_f64 = delta.to_string().parse::<f64>().unwrap_or(0.0);

    if delta_f64 < -300.0 {
        -6
    } else if delta_f64 < -200.0 {
        -5
    } else if delta_f64 < -100.0 {
        -4
    } else if delta_f64 < -50.0 {
        -3
    } else if delta_f64 < -20.0 {
        -2
    } else if delta_f64 < 0.0 {
        -1
    } else if delta_f64 < 20.0 {
        0
    } else if delta_f64 < 50.0 {
        1
    } else if delta_f64 < 100.0 {
        2
    } else if delta_f64 < 200.0 {
        3
    } else if delta_f64 < 300.0 {
        4
    } else {
        5
    }
}

/// Get human-readable label for a price delta bucket
pub fn bucket_to_label(bucket: i8) -> &'static str {
    match bucket {
        -6 => "< -$300",
        -5 => "-$300 to -$200",
        -4 => "-$200 to -$100",
        -3 => "-$100 to -$50",
        -2 => "-$50 to -$20",
        -1 => "-$20 to $0",
        0 => "$0 to +$20",
        1 => "+$20 to +$50",
        2 => "+$50 to +$100",
        3 => "+$100 to +$200",
        4 => "+$200 to +$300",
        5 => "> +$300",
        _ => "Unknown",
    }
}

/// Confidence level for a probability estimate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfidenceLevel {
    /// n < 10: Don't bet
    Unreliable,
    /// 10 <= n < 30: Small bets only
    Weak,
    /// 30 <= n < 100: Standard betting
    Moderate,
    /// n >= 100: High confidence
    Strong,
}

impl ConfidenceLevel {
    pub fn from_sample_count(n: u32) -> Self {
        if n < 10 {
            ConfidenceLevel::Unreliable
        } else if n < 30 {
            ConfidenceLevel::Weak
        } else if n < 100 {
            ConfidenceLevel::Moderate
        } else {
            ConfidenceLevel::Strong
        }
    }
}

/// Statistics for a single cell in the probability matrix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellStats {
    /// Time bucket (0-29)
    pub time_bucket: u8,
    /// Price delta bucket (-6 to +5)
    pub price_delta_bucket: i8,

    /// Number of times outcome was UP
    pub count_up: u32,
    /// Number of times outcome was DOWN
    pub count_down: u32,

    /// Raw probability of UP
    pub p_up: f64,
    /// Raw probability of DOWN
    pub p_down: f64,

    /// Wilson Score CI lower bound for P(UP) at 95% confidence
    pub p_up_wilson_lower: f64,
    /// Wilson Score CI upper bound for P(UP) at 95% confidence
    pub p_up_wilson_upper: f64,

    /// Bayesian posterior alpha (for Beta distribution)
    pub beta_alpha: f64,
    /// Bayesian posterior beta (for Beta distribution)
    pub beta_beta: f64,

    /// Confidence level based on sample size
    pub confidence_level: ConfidenceLevel,
}

impl CellStats {
    pub fn new(time_bucket: u8, price_delta_bucket: i8) -> Self {
        Self {
            time_bucket,
            price_delta_bucket,
            count_up: 0,
            count_down: 0,
            p_up: 0.5,
            p_down: 0.5,
            p_up_wilson_lower: 0.0,
            p_up_wilson_upper: 1.0,
            beta_alpha: 1.0,  // Uniform prior
            beta_beta: 1.0,
            confidence_level: ConfidenceLevel::Unreliable,
        }
    }

    pub fn total(&self) -> u32 {
        self.count_up + self.count_down
    }

    pub fn increment(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Up => self.count_up += 1,
            Outcome::Down => self.count_down += 1,
        }
    }
}

/// The complete probability matrix: 30 time buckets x 13 price delta buckets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityMatrix {
    /// 2D array: matrix[time_bucket][price_delta_bucket + 6]
    /// The +6 offset converts -6..+6 to 0..12 for array indexing
    pub cells: Vec<Vec<CellStats>>,

    /// Total number of 15-minute windows analyzed
    pub total_windows: u32,

    /// Date range of data
    pub data_start: Option<DateTime<Utc>>,
    pub data_end: Option<DateTime<Utc>>,
}

impl ProbabilityMatrix {
    pub fn new() -> Self {
        let mut cells = Vec::with_capacity(TIME_BUCKETS as usize);

        for time_bucket in 0..TIME_BUCKETS {
            let mut row = Vec::with_capacity(PRICE_DELTA_BUCKETS as usize);
            for delta_bucket in -6i8..=5i8 {
                row.push(CellStats::new(time_bucket, delta_bucket));
            }
            cells.push(row);
        }

        Self {
            cells,
            total_windows: 0,
            data_start: None,
            data_end: None,
        }
    }

    /// Get a cell by time bucket (0-29) and price delta bucket (-6 to +5)
    pub fn get(&self, time_bucket: u8, price_delta_bucket: i8) -> &CellStats {
        &self.cells[time_bucket as usize][(price_delta_bucket + 6) as usize]
    }

    /// Get a mutable cell
    pub fn get_mut(&mut self, time_bucket: u8, price_delta_bucket: i8) -> &mut CellStats {
        &mut self.cells[time_bucket as usize][(price_delta_bucket + 6) as usize]
    }

    /// Record an observation
    pub fn record(&mut self, time_bucket: u8, price_delta: Decimal, outcome: Outcome) {
        let delta_bucket = delta_to_bucket(price_delta);
        self.get_mut(time_bucket, delta_bucket).increment(outcome);
    }
}

/// Recommendation from the bot API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetRecommendation {
    /// Should we place a bet?
    pub should_bet: bool,
    /// Which direction to bet (if should_bet is true)
    pub direction: Option<Outcome>,
    /// Our calculated probability of winning
    pub our_probability: f64,
    /// Market implied probability (from Polymarket price)
    pub market_probability: f64,
    /// Edge (our_probability - market_probability)
    pub edge: f64,
    /// Confidence level
    pub confidence: ConfidenceLevel,
    /// Recommended bet size (as fraction of bankroll)
    pub kelly_fraction: f64,
    /// Recommended bet amount in dollars
    pub bet_amount: f64,
    /// Sample count for this cell
    pub sample_count: u32,
    /// Wilson CI lower bound for our probability
    pub probability_lower_bound: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_delta_to_bucket() {
        assert_eq!(delta_to_bucket(dec!(-500)), -6);
        assert_eq!(delta_to_bucket(dec!(-250)), -5);
        assert_eq!(delta_to_bucket(dec!(-150)), -4);
        assert_eq!(delta_to_bucket(dec!(-75)), -3);
        assert_eq!(delta_to_bucket(dec!(-30)), -2);
        assert_eq!(delta_to_bucket(dec!(-10)), -1);
        assert_eq!(delta_to_bucket(dec!(0)), 0);
        assert_eq!(delta_to_bucket(dec!(10)), 0);
        assert_eq!(delta_to_bucket(dec!(30)), 1);
        assert_eq!(delta_to_bucket(dec!(75)), 2);
        assert_eq!(delta_to_bucket(dec!(150)), 3);
        assert_eq!(delta_to_bucket(dec!(250)), 4);
        assert_eq!(delta_to_bucket(dec!(500)), 5);
    }

    #[test]
    fn test_confidence_level() {
        assert_eq!(ConfidenceLevel::from_sample_count(5), ConfidenceLevel::Unreliable);
        assert_eq!(ConfidenceLevel::from_sample_count(15), ConfidenceLevel::Weak);
        assert_eq!(ConfidenceLevel::from_sample_count(50), ConfidenceLevel::Moderate);
        assert_eq!(ConfidenceLevel::from_sample_count(150), ConfidenceLevel::Strong);
    }

    #[test]
    fn test_probability_matrix() {
        let mut matrix = ProbabilityMatrix::new();

        // Record some observations
        matrix.record(0, dec!(10), Outcome::Up);
        matrix.record(0, dec!(10), Outcome::Up);
        matrix.record(0, dec!(10), Outcome::Down);

        let cell = matrix.get(0, 0); // time_bucket=0, delta_bucket=0 ($0 to $20)
        assert_eq!(cell.count_up, 2);
        assert_eq!(cell.count_down, 1);
        assert_eq!(cell.total(), 3);
    }
}
