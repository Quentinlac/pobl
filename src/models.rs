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
    /// Price at each 15-second interval (60 snapshots)
    pub snapshots: Vec<PriceSnapshot>,
}

/// A snapshot within a 15-minute window
#[derive(Debug, Clone)]
pub struct PriceSnapshot {
    /// 0-59 (which 15-second bucket)
    pub time_bucket: u8,
    /// Price at this moment
    pub price: Decimal,
    /// Delta from window open price (can be negative)
    pub delta_from_open: Decimal,
}

/// Time bucket (0-59, representing 15-second intervals within 15 minutes)
pub const TIME_BUCKETS: u8 = 60;

/// Price delta bucket boundaries in dollars
/// Finer granularity near zero (5$ steps), coarser at extremes (30$ steps)
///
/// Negative buckets (-17 to -1):
/// -17: < -300, -16: -300 to -260, -15: -260 to -230, -14: -230 to -200
/// -13: -200 to -170, -12: -170 to -140, -11: -140 to -110, -10: -110 to -90
/// -9: -90 to -70, -8: -70 to -50, -7: -50 to -40, -6: -40 to -30
/// -5: -30 to -20, -4: -20 to -15, -3: -15 to -10, -2: -10 to -5, -1: -5 to 0
///
/// Positive buckets (0 to 16):
/// 0: 0 to 5, 1: 5 to 10, 2: 10 to 15, 3: 15 to 20
/// 4: 20 to 30, 5: 30 to 40, 6: 40 to 50, 7: 50 to 70
/// 8: 70 to 90, 9: 90 to 110, 10: 110 to 140, 11: 140 to 170
/// 12: 170 to 200, 13: 200 to 230, 14: 230 to 260, 15: 260 to 300, 16: > 300
pub const PRICE_DELTA_BUCKETS: i8 = 34; // -17 to +16
pub const DELTA_BUCKET_MIN: i8 = -17;
pub const DELTA_BUCKET_MAX: i8 = 16;

/// Map a price delta (in dollars) to a bucket index (-17 to +16)
pub fn delta_to_bucket(delta: Decimal) -> i8 {
    let d = delta.to_string().parse::<f64>().unwrap_or(0.0);

    if d < 0.0 {
        // Negative deltas: -17 to -1
        if d < -300.0 { -17 }
        else if d < -260.0 { -16 }
        else if d < -230.0 { -15 }
        else if d < -200.0 { -14 }
        else if d < -170.0 { -13 }
        else if d < -140.0 { -12 }
        else if d < -110.0 { -11 }
        else if d < -90.0 { -10 }
        else if d < -70.0 { -9 }
        else if d < -50.0 { -8 }
        else if d < -40.0 { -7 }
        else if d < -30.0 { -6 }
        else if d < -20.0 { -5 }
        else if d < -15.0 { -4 }
        else if d < -10.0 { -3 }
        else if d < -5.0 { -2 }
        else { -1 } // -5 to 0
    } else {
        // Positive deltas: 0 to 16
        if d < 5.0 { 0 }
        else if d < 10.0 { 1 }
        else if d < 15.0 { 2 }
        else if d < 20.0 { 3 }
        else if d < 30.0 { 4 }
        else if d < 40.0 { 5 }
        else if d < 50.0 { 6 }
        else if d < 70.0 { 7 }
        else if d < 90.0 { 8 }
        else if d < 110.0 { 9 }
        else if d < 140.0 { 10 }
        else if d < 170.0 { 11 }
        else if d < 200.0 { 12 }
        else if d < 230.0 { 13 }
        else if d < 260.0 { 14 }
        else if d < 300.0 { 15 }
        else { 16 } // > 300
    }
}

/// Get human-readable label for a price delta bucket
pub fn bucket_to_label(bucket: i8) -> &'static str {
    match bucket {
        // Negative buckets
        -17 => "< -$300",
        -16 => "-$300 to -$260",
        -15 => "-$260 to -$230",
        -14 => "-$230 to -$200",
        -13 => "-$200 to -$170",
        -12 => "-$170 to -$140",
        -11 => "-$140 to -$110",
        -10 => "-$110 to -$90",
        -9 => "-$90 to -$70",
        -8 => "-$70 to -$50",
        -7 => "-$50 to -$40",
        -6 => "-$40 to -$30",
        -5 => "-$30 to -$20",
        -4 => "-$20 to -$15",
        -3 => "-$15 to -$10",
        -2 => "-$10 to -$5",
        -1 => "-$5 to $0",
        // Positive buckets
        0 => "$0 to +$5",
        1 => "+$5 to +$10",
        2 => "+$10 to +$15",
        3 => "+$15 to +$20",
        4 => "+$20 to +$30",
        5 => "+$30 to +$40",
        6 => "+$40 to +$50",
        7 => "+$50 to +$70",
        8 => "+$70 to +$90",
        9 => "+$90 to +$110",
        10 => "+$110 to +$140",
        11 => "+$140 to +$170",
        12 => "+$170 to +$200",
        13 => "+$200 to +$230",
        14 => "+$230 to +$260",
        15 => "+$260 to +$300",
        16 => "> +$300",
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
    /// Time bucket (0-59)
    pub time_bucket: u8,
    /// Price delta bucket (-17 to +16)
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

/// The complete probability matrix: 60 time buckets x 34 price delta buckets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityMatrix {
    /// 2D array: matrix[time_bucket][price_delta_bucket + 17]
    /// The +17 offset converts -17..+16 to 0..33 for array indexing
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
            for delta_bucket in -17i8..=16i8 {
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

    /// Get a cell by time bucket (0-59) and price delta bucket (-17 to +16)
    pub fn get(&self, time_bucket: u8, price_delta_bucket: i8) -> &CellStats {
        &self.cells[time_bucket as usize][(price_delta_bucket + 17) as usize]
    }

    /// Get a mutable cell
    pub fn get_mut(&mut self, time_bucket: u8, price_delta_bucket: i8) -> &mut CellStats {
        &mut self.cells[time_bucket as usize][(price_delta_bucket + 17) as usize]
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

// ============================================================================
// FIRST-PASSAGE MATRIX (Matrix 2)
// For each (time, price_delta) state, tracks probability of reaching target prices
// ============================================================================

/// Statistics for reaching a specific target from a given state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstPassageCell {
    /// Number of times the target was reached
    pub count_reached: u32,
    /// Total observations from this state
    pub count_total: u32,
    /// Probability of reaching the target
    pub p_reach: f64,
    /// Wilson Score CI bounds
    pub p_reach_wilson_lower: f64,
    pub p_reach_wilson_upper: f64,
    /// Confidence level
    pub confidence_level: ConfidenceLevel,
}

impl FirstPassageCell {
    pub fn new() -> Self {
        Self {
            count_reached: 0,
            count_total: 0,
            p_reach: 0.0,
            p_reach_wilson_lower: 0.0,
            p_reach_wilson_upper: 1.0,
            confidence_level: ConfidenceLevel::Unreliable,
        }
    }

    pub fn record(&mut self, reached: bool) {
        self.count_total += 1;
        if reached {
            self.count_reached += 1;
        }
    }
}

impl Default for FirstPassageCell {
    fn default() -> Self {
        Self::new()
    }
}

/// First-passage probabilities for a single (time, price_delta) state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstPassageState {
    /// Time bucket (0-59)
    pub time_bucket: u8,
    /// Price delta bucket (-17 to +16)
    pub price_delta_bucket: i8,
    /// Probabilities of reaching UP targets (indexed by target bucket -17 to +16)
    /// P(max_delta reaches target_bucket) during remaining time
    pub up_targets: Vec<FirstPassageCell>,
    /// Probabilities of reaching DOWN targets (indexed by target bucket -17 to +16)
    /// P(min_delta reaches target_bucket) during remaining time
    pub down_targets: Vec<FirstPassageCell>,
}

impl FirstPassageState {
    pub fn new(time_bucket: u8, price_delta_bucket: i8) -> Self {
        Self {
            time_bucket,
            price_delta_bucket,
            up_targets: (0..PRICE_DELTA_BUCKETS).map(|_| FirstPassageCell::new()).collect(),
            down_targets: (0..PRICE_DELTA_BUCKETS).map(|_| FirstPassageCell::new()).collect(),
        }
    }

    /// Get UP target cell (for when we hold UP and want to know P(price rises to target))
    pub fn get_up_target(&self, target_bucket: i8) -> &FirstPassageCell {
        &self.up_targets[(target_bucket + 17) as usize]
    }

    /// Get DOWN target cell (for when we hold DOWN and want to know P(price falls to target))
    pub fn get_down_target(&self, target_bucket: i8) -> &FirstPassageCell {
        &self.down_targets[(target_bucket + 17) as usize]
    }

    /// Get mutable UP target cell
    pub fn get_up_target_mut(&mut self, target_bucket: i8) -> &mut FirstPassageCell {
        &mut self.up_targets[(target_bucket + 17) as usize]
    }

    /// Get mutable DOWN target cell
    pub fn get_down_target_mut(&mut self, target_bucket: i8) -> &mut FirstPassageCell {
        &mut self.down_targets[(target_bucket + 17) as usize]
    }
}

/// The complete First-Passage Matrix
/// Dimensions: [time_bucket][price_delta_bucket] -> FirstPassageState
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstPassageMatrix {
    /// 2D array: states[time_bucket][price_delta_bucket + 17]
    pub states: Vec<Vec<FirstPassageState>>,
    /// Total observations
    pub total_observations: u32,
    /// Date range
    pub data_start: Option<DateTime<Utc>>,
    pub data_end: Option<DateTime<Utc>>,
}

impl FirstPassageMatrix {
    pub fn new() -> Self {
        let mut states = Vec::with_capacity(TIME_BUCKETS as usize);

        for time_bucket in 0..TIME_BUCKETS {
            let mut row = Vec::with_capacity(PRICE_DELTA_BUCKETS as usize);
            for delta_bucket in -17i8..=16i8 {
                row.push(FirstPassageState::new(time_bucket, delta_bucket));
            }
            states.push(row);
        }

        Self {
            states,
            total_observations: 0,
            data_start: None,
            data_end: None,
        }
    }

    /// Get state by time bucket and price delta bucket
    pub fn get(&self, time_bucket: u8, price_delta_bucket: i8) -> &FirstPassageState {
        &self.states[time_bucket as usize][(price_delta_bucket + 17) as usize]
    }

    /// Get mutable state
    pub fn get_mut(&mut self, time_bucket: u8, price_delta_bucket: i8) -> &mut FirstPassageState {
        &mut self.states[time_bucket as usize][(price_delta_bucket + 17) as usize]
    }

    /// Record an observation: from (time_bucket, current_delta),
    /// what were the max and min deltas reached in remaining time?
    pub fn record(
        &mut self,
        time_bucket: u8,
        current_delta: Decimal,
        max_delta_remaining: Decimal,
        min_delta_remaining: Decimal,
    ) {
        let current_bucket = delta_to_bucket(current_delta);
        let max_bucket = delta_to_bucket(max_delta_remaining);
        let min_bucket = delta_to_bucket(min_delta_remaining);

        let state = self.get_mut(time_bucket, current_bucket);

        // For UP targets: did max_delta reach or exceed each target?
        for target in -17i8..=16i8 {
            let reached = max_bucket >= target;
            state.get_up_target_mut(target).record(reached);
        }

        // For DOWN targets: did min_delta reach or go below each target?
        for target in -17i8..=16i8 {
            let reached = min_bucket <= target;
            state.get_down_target_mut(target).record(reached);
        }

        self.total_observations += 1;
    }
}

impl Default for FirstPassageMatrix {
    fn default() -> Self {
        Self::new()
    }
}

/// Exit strategy recommendation based on first-passage analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitRecommendation {
    /// Should we set an exit target?
    pub should_exit_early: bool,
    /// Best exit target price (as probability, e.g., 0.60 = 60%)
    pub exit_target: f64,
    /// Expected value of exiting at target vs holding
    pub exit_ev: f64,
    /// Expected value of holding to settlement
    pub hold_ev: f64,
    /// Probability of reaching exit target
    pub p_reach_target: f64,
    /// Confidence level
    pub confidence: ConfidenceLevel,
}

// ============================================================================
// PRICE REACH MATRIX (Matrix 3)
// Tracks how often UP/DOWN token prices reach specific levels from each state
// Price levels: 0¢, 4¢, 8¢, 12¢, ..., 96¢, 100¢ (26 levels)
// ============================================================================

/// Number of price levels tracked (0, 4, 8, ..., 100 = 26 levels)
pub const PRICE_LEVELS: usize = 26;

/// Convert price level index (0-25) to cents (0, 4, 8, ..., 100)
pub fn price_level_to_cents(level: usize) -> u8 {
    (level * 4) as u8
}

/// Convert cents to price level index (rounds down to nearest 4¢)
pub fn cents_to_price_level(cents: u8) -> usize {
    (cents as usize / 4).min(25)
}

/// State for tracking price level reaches from a specific (time, delta) state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceReachState {
    /// Time bucket (0-59)
    pub time_bucket: u8,
    /// Price delta bucket (-17 to +16)
    pub delta_bucket: i8,
    /// Total observations from this state
    pub count_total: u32,
    /// UP token: count of times each price level was reached (max P(UP) >= level)
    /// Index 0 = 0¢, 1 = 4¢, 2 = 8¢, ..., 25 = 100¢
    pub up_reached: [u32; PRICE_LEVELS],
    /// DOWN token: count of times each price level was reached (max P(DOWN) >= level)
    pub down_reached: [u32; PRICE_LEVELS],
    /// Computed probabilities for UP (filled after processing)
    pub p_up_reach: [f64; PRICE_LEVELS],
    /// Computed probabilities for DOWN
    pub p_down_reach: [f64; PRICE_LEVELS],
}

impl PriceReachState {
    pub fn new(time_bucket: u8, delta_bucket: i8) -> Self {
        Self {
            time_bucket,
            delta_bucket,
            count_total: 0,
            up_reached: [0; PRICE_LEVELS],
            down_reached: [0; PRICE_LEVELS],
            p_up_reach: [0.0; PRICE_LEVELS],
            p_down_reach: [0.0; PRICE_LEVELS],
        }
    }

    /// Record an observation: max_p_up and max_p_down reached during remaining window
    /// Both are probabilities (0.0 to 1.0)
    /// Records ONLY the bucket where the max price fell (not cumulative)
    /// This creates a distribution that sums to 100%
    pub fn record(&mut self, max_p_up: f64, max_p_down: f64) {
        self.count_total += 1;

        // For UP: record only the bucket where max_p_up falls
        // E.g., 52% -> bucket for 52¢ (level 13: 52/4 = 13)
        let max_up_cents = (max_p_up * 100.0).floor() as u8;
        let up_level = cents_to_price_level(max_up_cents);
        self.up_reached[up_level] += 1;

        // For DOWN: record only the bucket where max_p_down falls
        let max_down_cents = (max_p_down * 100.0).floor() as u8;
        let down_level = cents_to_price_level(max_down_cents);
        self.down_reached[down_level] += 1;
    }

    /// Compute probabilities from counts
    pub fn compute_probabilities(&mut self) {
        if self.count_total == 0 {
            return;
        }
        let total = self.count_total as f64;
        for level in 0..PRICE_LEVELS {
            self.p_up_reach[level] = self.up_reached[level] as f64 / total;
            self.p_down_reach[level] = self.down_reached[level] as f64 / total;
        }
    }
}

/// The complete Price Reach Matrix
/// Tracks P(UP/DOWN price reaches X¢) from each (time, delta) state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceReachMatrix {
    /// 2D array: states[time_bucket][delta_bucket + 17]
    pub states: Vec<Vec<PriceReachState>>,
    /// Total observations
    pub total_observations: u32,
    /// Date range
    pub data_start: Option<DateTime<Utc>>,
    pub data_end: Option<DateTime<Utc>>,
}

impl PriceReachMatrix {
    pub fn new() -> Self {
        let mut states = Vec::with_capacity(TIME_BUCKETS as usize);

        for time_bucket in 0..TIME_BUCKETS {
            let mut row = Vec::with_capacity(PRICE_DELTA_BUCKETS as usize);
            for delta_bucket in -17i8..=16i8 {
                row.push(PriceReachState::new(time_bucket, delta_bucket));
            }
            states.push(row);
        }

        Self {
            states,
            total_observations: 0,
            data_start: None,
            data_end: None,
        }
    }

    /// Get state by time bucket and price delta bucket
    pub fn get(&self, time_bucket: u8, delta_bucket: i8) -> &PriceReachState {
        &self.states[time_bucket as usize][(delta_bucket + 17) as usize]
    }

    /// Get mutable state
    pub fn get_mut(&mut self, time_bucket: u8, delta_bucket: i8) -> &mut PriceReachState {
        &mut self.states[time_bucket as usize][(delta_bucket + 17) as usize]
    }
}

impl Default for PriceReachMatrix {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PRICE CROSSING MATRIX (Matrix 4)
// Counts how many times price crosses each level (0.04, 0.08, ..., 1.00)
// A "crossing" = price goes from below to above OR above to below a level
// ============================================================================

/// Estimate UP token cents from delta bucket
/// This is an approximation based on typical BTC volatility
pub fn delta_bucket_to_up_cents(delta_bucket: i8) -> u8 {
    // Mapping delta bucket to approximate UP token price in cents
    // delta = 0 means BTC unchanged from open → UP ≈ 50¢
    // Each bucket step ≈ 2-3¢ change in UP price (rough approximation)
    match delta_bucket {
        -17 => 4,   // < -$300 → UP very low
        -16 => 8,   // -$300 to -$260
        -15 => 10,  // -$260 to -$230
        -14 => 12,  // -$230 to -$200
        -13 => 16,  // -$200 to -$170
        -12 => 20,  // -$170 to -$140
        -11 => 24,  // -$140 to -$110
        -10 => 28,  // -$110 to -$90
        -9 => 32,   // -$90 to -$70
        -8 => 36,   // -$70 to -$50
        -7 => 40,   // -$50 to -$40
        -6 => 42,   // -$40 to -$30
        -5 => 44,   // -$30 to -$20
        -4 => 45,   // -$20 to -$15
        -3 => 46,   // -$15 to -$10
        -2 => 48,   // -$10 to -$5
        -1 => 49,   // -$5 to $0
        0 => 50,    // $0 to +$5
        1 => 52,    // +$5 to +$10
        2 => 54,    // +$10 to +$15
        3 => 56,    // +$15 to +$20
        4 => 58,    // +$20 to +$30
        5 => 60,    // +$30 to +$40
        6 => 64,    // +$40 to +$50
        7 => 68,    // +$50 to +$70
        8 => 72,    // +$70 to +$90
        9 => 76,    // +$90 to +$110
        10 => 80,   // +$110 to +$140
        11 => 84,   // +$140 to +$170
        12 => 88,   // +$170 to +$200
        13 => 90,   // +$200 to +$230
        14 => 92,   // +$230 to +$260
        15 => 94,   // +$260 to +$300
        16 => 96,   // > +$300 → UP very high
        _ => 50,
    }
}

/// State for tracking price crossings from a specific (time, delta) state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceCrossingState {
    /// Time bucket (0-59)
    pub time_bucket: u8,
    /// Price delta bucket (-17 to +16)
    pub delta_bucket: i8,
    /// Total trajectories (historical windows starting from this state)
    pub count_trajectories: u32,
    /// Estimated current UP price in cents (based on delta bucket)
    pub current_up_cents: u8,
    /// Combined P(reached) for each level - uses p_reach_up if level > current, p_reach_down if level < current
    pub p_reached: [f64; 25],
    /// Count of trajectories that reached each level (combined)
    pub reached: [u32; 25],
    /// Normalized distribution: reached[i] / sum(reached) - sums to 100%
    pub p_normalized: [f64; 25],
    /// UP value = UP_cents * p_reached (expected value for UP holder)
    pub up_value: [f64; 25],
    /// DN value = DN_cents * p_reached (expected value for DOWN holder)
    pub dn_value: [f64; 25],
    /// Count of trajectories that REACHED each level going UP (at least once)
    /// This is what we need for P(limit order fills)
    pub reached_up: [u32; 25],
    /// Count of trajectories that REACHED each level going DOWN (at least once)
    pub reached_down: [u32; 25],
    /// P(reach) UP = reached_up / count_trajectories
    pub p_reach_up: [f64; 25],
    /// P(reach) DOWN = reached_down / count_trajectories
    pub p_reach_down: [f64; 25],
    /// Count of UPWARD crossings for each price level (total, can be > trajectories)
    /// Upward = price goes from below to above the level
    /// Index 0 = 0.04 (4¢), 1 = 0.08 (8¢), ..., 24 = 1.00 (100¢)
    pub crossings_up: [u32; 25],
    /// Count of DOWNWARD crossings for each price level (total)
    /// Downward = price goes from above to below the level
    pub crossings_down: [u32; 25],
    /// Average UPWARD crossings per trajectory for each level
    pub avg_crossings_up: [f64; 25],
    /// Average DOWNWARD crossings per trajectory for each level
    pub avg_crossings_down: [f64; 25],
    /// Legacy: total crossings (up + down) - kept for backward compatibility
    pub crossings: [u32; 25],
    /// Legacy: average total crossings
    pub avg_crossings: [f64; 25],
}

impl PriceCrossingState {
    pub fn new(time_bucket: u8, delta_bucket: i8) -> Self {
        Self {
            time_bucket,
            delta_bucket,
            count_trajectories: 0,
            current_up_cents: delta_bucket_to_up_cents(delta_bucket),
            p_reached: [0.0; 25],
            reached: [0; 25],
            p_normalized: [0.0; 25],
            up_value: [0.0; 25],
            dn_value: [0.0; 25],
            reached_up: [0; 25],
            reached_down: [0; 25],
            p_reach_up: [0.0; 25],
            p_reach_down: [0.0; 25],
            crossings_up: [0; 25],
            crossings_down: [0; 25],
            avg_crossings_up: [0.0; 25],
            avg_crossings_down: [0.0; 25],
            crossings: [0; 25],
            avg_crossings: [0.0; 25],
        }
    }

    /// Record crossings from a single trajectory (directional)
    /// crossings_up[i] = number of times this trajectory crossed level i going UP
    /// crossings_down[i] = number of times this trajectory crossed level i going DOWN
    pub fn record_trajectory_directional(
        &mut self,
        crossings_up: &[u32; 25],
        crossings_down: &[u32; 25],
    ) {
        self.count_trajectories += 1;
        for i in 0..25 {
            // Total crossings (for average calculation)
            self.crossings_up[i] += crossings_up[i];
            self.crossings_down[i] += crossings_down[i];
            self.crossings[i] += crossings_up[i] + crossings_down[i];

            // Unique reaches per direction
            if crossings_up[i] > 0 {
                self.reached_up[i] += 1;
            }
            if crossings_down[i] > 0 {
                self.reached_down[i] += 1;
            }

            // Combined reach: touched this level at least once (either direction)
            if crossings_up[i] > 0 || crossings_down[i] > 0 {
                self.reached[i] += 1;
            }
        }
    }

    /// Legacy: Record crossings from a single trajectory (non-directional)
    pub fn record_trajectory(&mut self, crossings_per_level: &[u32; 25]) {
        self.count_trajectories += 1;
        for i in 0..25 {
            self.crossings[i] += crossings_per_level[i];
        }
    }

    /// Compute averages and probabilities
    pub fn compute_averages(&mut self) {
        if self.count_trajectories == 0 {
            return;
        }
        let total = self.count_trajectories as f64;

        // Sum of all reached counts for normalization
        let sum_reached: u32 = self.reached.iter().sum();
        let sum_reached_f64 = sum_reached as f64;

        for i in 0..25 {
            // Average crossings per trajectory
            self.avg_crossings_up[i] = self.crossings_up[i] as f64 / total;
            self.avg_crossings_down[i] = self.crossings_down[i] as f64 / total;
            self.avg_crossings[i] = self.crossings[i] as f64 / total;

            // P(reach) = trajectories that reached / total trajectories
            self.p_reach_up[i] = self.reached_up[i] as f64 / total;
            self.p_reach_down[i] = self.reached_down[i] as f64 / total;

            // Combined p_reached from reached (already computed in record_trajectory_directional)
            self.p_reached[i] = self.reached[i] as f64 / total;

            // Normalized: reached[i] / sum(reached) - sums to 100%
            if sum_reached_f64 > 0.0 {
                self.p_normalized[i] = self.reached[i] as f64 / sum_reached_f64;
            }

            // UP and DN values: price * p_reached
            let up_cents = ((i + 1) * 4) as f64;
            let dn_cents = 100.0 - up_cents;
            self.up_value[i] = up_cents * self.p_reached[i];
            self.dn_value[i] = dn_cents * self.p_reached[i];
        }
    }
}

/// The complete Price Crossing Matrix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceCrossingMatrix {
    /// 2D array: states[time_bucket][delta_bucket + 17]
    pub states: Vec<Vec<PriceCrossingState>>,
    /// Total trajectories analyzed
    pub total_trajectories: u32,
    /// Date range
    pub data_start: Option<DateTime<Utc>>,
    pub data_end: Option<DateTime<Utc>>,
}

impl PriceCrossingMatrix {
    pub fn new() -> Self {
        let mut states = Vec::with_capacity(TIME_BUCKETS as usize);

        for time_bucket in 0..TIME_BUCKETS {
            let mut row = Vec::with_capacity(PRICE_DELTA_BUCKETS as usize);
            for delta_bucket in -17i8..=16i8 {
                row.push(PriceCrossingState::new(time_bucket, delta_bucket));
            }
            states.push(row);
        }

        Self {
            states,
            total_trajectories: 0,
            data_start: None,
            data_end: None,
        }
    }

    /// Get state by time bucket and price delta bucket
    pub fn get(&self, time_bucket: u8, delta_bucket: i8) -> &PriceCrossingState {
        &self.states[time_bucket as usize][(delta_bucket + 17) as usize]
    }

    /// Get mutable state
    pub fn get_mut(&mut self, time_bucket: u8, delta_bucket: i8) -> &mut PriceCrossingState {
        &mut self.states[time_bucket as usize][(delta_bucket + 17) as usize]
    }
}

impl Default for PriceCrossingMatrix {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert price level index (0-24) to cents (4, 8, ..., 100)
pub fn crossing_level_to_cents(level: usize) -> u8 {
    ((level + 1) * 4) as u8
}

/// Count crossings between two consecutive prices for all 25 levels
/// Returns (crossings_up, crossings_down) arrays
/// crossings_up[i] = 1 if price crossed level (i+1)*4 cents going UP
/// crossings_down[i] = 1 if price crossed level (i+1)*4 cents going DOWN
pub fn count_crossings_directional(prev_price: f64, curr_price: f64) -> ([u32; 25], [u32; 25]) {
    let mut crossings_up = [0u32; 25];
    let mut crossings_down = [0u32; 25];

    for i in 0..25 {
        let level = (i + 1) as f64 * 0.04; // 0.04, 0.08, ..., 1.00

        // Crossing up: prev < level <= curr (price went UP through the level)
        if prev_price < level && curr_price >= level {
            crossings_up[i] = 1;
        }
        // Crossing down: prev >= level > curr (price went DOWN through the level)
        if prev_price >= level && curr_price < level {
            crossings_down[i] = 1;
        }
    }

    (crossings_up, crossings_down)
}

/// Legacy: Count crossings (both directions combined)
pub fn count_crossings(prev_price: f64, curr_price: f64) -> [u32; 25] {
    let mut crossings = [0u32; 25];

    for i in 0..25 {
        let level = (i + 1) as f64 * 0.04;
        let crossed_up = prev_price < level && curr_price >= level;
        let crossed_down = prev_price >= level && curr_price < level;
        if crossed_up || crossed_down {
            crossings[i] = 1;
        }
    }

    crossings
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_delta_to_bucket() {
        // Extreme negative
        assert_eq!(delta_to_bucket(dec!(-500)), -17);  // < -300
        assert_eq!(delta_to_bucket(dec!(-280)), -16);  // -300 to -260
        assert_eq!(delta_to_bucket(dec!(-240)), -15);  // -260 to -230
        assert_eq!(delta_to_bucket(dec!(-180)), -13);  // -200 to -170
        assert_eq!(delta_to_bucket(dec!(-80)), -9);    // -90 to -70
        assert_eq!(delta_to_bucket(dec!(-25)), -5);    // -30 to -20
        assert_eq!(delta_to_bucket(dec!(-3)), -1);     // -5 to 0
        // Positive
        assert_eq!(delta_to_bucket(dec!(0)), 0);       // 0 to 5
        assert_eq!(delta_to_bucket(dec!(4)), 0);       // 0 to 5
        assert_eq!(delta_to_bucket(dec!(7)), 1);       // 5 to 10
        assert_eq!(delta_to_bucket(dec!(12)), 2);      // 10 to 15
        assert_eq!(delta_to_bucket(dec!(25)), 4);      // 20 to 30
        assert_eq!(delta_to_bucket(dec!(55)), 7);      // 50 to 70
        assert_eq!(delta_to_bucket(dec!(100)), 9);     // 90 to 110
        assert_eq!(delta_to_bucket(dec!(180)), 12);    // 170 to 200
        assert_eq!(delta_to_bucket(dec!(250)), 14);    // 230 to 260
        assert_eq!(delta_to_bucket(dec!(500)), 16);    // > 300
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
