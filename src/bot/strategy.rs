use super::config::BotConfig;
use super::polymarket::PriceQuote;
use crate::models::{delta_to_bucket, CellStats, ConfidenceLevel, FirstPassageMatrix, PriceCrossingMatrix, ProbabilityMatrix};
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

/// Map target price (0.0-1.0) to delta bucket for exit strategy
/// For UP positions: higher price = more positive delta (price went up)
/// For DOWN positions: higher price = more negative delta (price went down)
///
/// New 34-bucket system: -17 to +16
/// Price roughly correlates to delta:
///   35¢ → ~-$110 (bucket -11)
///   45¢ → ~-$30  (bucket -6)
///   55¢ → ~+$30  (bucket 5)
///   65¢ → ~+$70  (bucket 7)
///   75¢ → ~+$130 (bucket 10)
///   85¢ → ~+$200 (bucket 12)
///   95¢ → ~+$280 (bucket 15)
fn price_to_delta_bucket(target_price: f64, direction: BetDirection) -> i8 {
    let price_pct = (target_price * 100.0) as i32;

    match direction {
        BetDirection::Up => {
            // Higher UP price = more positive delta
            match price_pct {
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
        }
        BetDirection::Down => {
            // Higher DOWN price = more negative delta (price dropped)
            match price_pct {
                0..=35 => 11,    // ~+$140 (inverted for DOWN)
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
        }
    }
}

/// Direction to bet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BetDirection {
    Up,
    Down,
}

/// Decision from the strategy
#[derive(Debug, Clone)]
pub struct BetDecision {
    pub should_bet: bool,
    pub direction: Option<BetDirection>,
    pub edge: f64,
    pub our_probability: f64,
    pub market_probability: f64,
    pub bet_amount: f64,
    pub confidence: ConfidenceLevel,
    pub reason: String,
    pub exit_target: Option<f64>,  // None = terminal strategy (hold), Some(price) = exit strategy
    pub strategy_type: String,     // "TERMINAL" or "EXIT"
}

/// Strategy context for making decisions
pub struct StrategyContext<'a> {
    pub config: &'a BotConfig,
    pub matrix: &'a ProbabilityMatrix,
    pub fp_matrix: Option<&'a FirstPassageMatrix>,  // For exit strategy EV calculation (legacy)
    pub crossing_matrix: Option<&'a PriceCrossingMatrix>,  // For crossing-based exit strategy
    pub bankroll: f64,
    pub consecutive_losses: u32,
    pub terminal_bets_this_window: u32,  // Bets from terminal strategy
    pub exit_bets_this_window: u32,      // Bets from exit strategy
    pub daily_pnl: f64,
    pub open_positions: u32,
}

/// Result of exit strategy EV calculation
#[derive(Debug, Clone)]
pub struct ExitStrategyResult {
    pub best_target: f64,
    pub best_ev: f64,           // Expected profit in dollars (e.g., 0.1056 = 10.56¢)
    pub ev_return_pct: f64,     // EV as percentage of entry (e.g., 0.18 = 18%)
    pub p_reach_target: f64,    // Probability of reaching target
}

impl<'a> StrategyContext<'a> {
    /// Make a betting decision based on current market state
    pub fn decide(
        &self,
        time_elapsed: u32,
        price_delta: f64,
        up_quote: &PriceQuote,
        down_quote: &PriceQuote,
    ) -> BetDecision {
        let time_remaining = 900u32.saturating_sub(time_elapsed);

        // ═══════════════════════════════════════════════════════════════
        // SHARED CHECKS (apply to both strategies)
        // ═══════════════════════════════════════════════════════════════

        // Check timing - too early
        if time_elapsed < self.config.timing.min_seconds_elapsed {
            return BetDecision::no_bet(format!(
                "Too early in window: {}s < {}s minimum",
                time_elapsed, self.config.timing.min_seconds_elapsed
            ));
        }

        // Check max open positions (shared limit)
        if self.open_positions >= self.config.risk.max_open_positions {
            return BetDecision::no_bet(format!(
                "Max open positions reached: {}",
                self.config.risk.max_open_positions
            ));
        }

        // Check daily loss limit
        let daily_loss_limit = self.bankroll * self.config.risk.daily_loss_limit_pct;
        if self.daily_pnl < 0.0 && self.daily_pnl.abs() >= daily_loss_limit {
            return BetDecision::no_bet(format!(
                "Daily loss limit reached: ${:.2} >= ${:.2}",
                self.daily_pnl.abs(),
                daily_loss_limit
            ));
        }

        // Check price filters
        if price_delta.abs() > self.config.price_filters.max_price_delta {
            return BetDecision::no_bet(format!(
                "Price delta too large: ${:.2} > ${:.2}",
                price_delta.abs(),
                self.config.price_filters.max_price_delta
            ));
        }

        // Check spread constraints
        let up_spread = up_quote.spread_pct;
        let down_spread = down_quote.spread_pct;
        if up_spread > self.config.markets.max_spread_pct
            || down_spread > self.config.markets.max_spread_pct
        {
            return BetDecision::no_bet(format!(
                "Spread too wide: UP={:.2}%, DOWN={:.2}% > {:.2}%",
                up_spread * 100.0,
                down_spread * 100.0,
                self.config.markets.max_spread_pct * 100.0
            ));
        }

        // Check liquidity
        let min_liquidity = self.config.markets.min_liquidity_usdc;
        if up_quote.ask_liquidity < min_liquidity && down_quote.ask_liquidity < min_liquidity {
            return BetDecision::no_bet(format!(
                "Insufficient liquidity: UP=${:.2}, DOWN=${:.2} < ${:.2}",
                up_quote.ask_liquidity, down_quote.ask_liquidity, min_liquidity
            ));
        }

        // Get matrix cell for current situation (15-second intervals)
        let time_bucket = (time_elapsed / 15).min(59) as u8;
        let delta_bucket = delta_to_bucket(
            Decimal::try_from(price_delta).unwrap_or_default()
        );

        let cell = self.matrix.get(time_bucket, delta_bucket);

        // Check sample size
        if cell.total() < self.config.timing.min_samples_in_bucket {
            return BetDecision::no_bet(format!(
                "Insufficient samples in bucket: {} < {}",
                cell.total(),
                self.config.timing.min_samples_in_bucket
            ));
        }

        // ═══════════════════════════════════════════════════════════════
        // PER-STRATEGY AVAILABILITY
        // ═══════════════════════════════════════════════════════════════

        // Check if terminal strategy can bet
        let terminal_available = self.config.terminal_strategy.enabled
            && time_remaining >= self.config.terminal_strategy.min_seconds_remaining
            && self.terminal_bets_this_window < self.config.terminal_strategy.max_bets_per_window;

        // Check if exit strategy can bet (crossing_matrix preferred, fp_matrix as fallback)
        let exit_available = self.config.exit_strategy.enabled
            && time_remaining >= self.config.exit_strategy.min_seconds_remaining
            && self.exit_bets_this_window < self.config.exit_strategy.max_bets_per_window
            && (self.crossing_matrix.is_some() || self.fp_matrix.is_some());

        if !terminal_available && !exit_available {
            return BetDecision::no_bet(format!(
                "No strategy available: terminal={} ({}s rem, {}/{} bets), exit={} ({}s rem, {}/{} bets)",
                terminal_available, time_remaining,
                self.terminal_bets_this_window, self.config.terminal_strategy.max_bets_per_window,
                exit_available, time_remaining,
                self.exit_bets_this_window, self.config.exit_strategy.max_bets_per_window
            ));
        }

        // Get confidence-based minimum edge (used as baseline, strategies have their own)
        let confidence_str = format!("{:?}", cell.confidence_level);
        if self.config.min_edge_for_confidence(&confidence_str).is_none() {
            return BetDecision::no_bet(format!(
                "Confidence level {:?} not allowed",
                cell.confidence_level
            ));
        }

        // Get market prices
        let up_entry_price = up_quote.best_ask;
        let down_entry_price = down_quote.best_ask;
        let up_market_prob = up_quote.mid_price;
        let down_market_prob = down_quote.mid_price;

        // Use Wilson lower bound for conservative probability estimate
        let our_p_up = cell.p_up_wilson_lower;
        let our_p_down = 1.0 - cell.p_up_wilson_upper;

        // ═══════════════════════════════════════════════════════════════
        // STRATEGY 1: TERMINAL EDGE (original strategy - hold to settlement)
        // ═══════════════════════════════════════════════════════════════
        let terminal_up_edge = if up_market_prob > 0.0 {
            (our_p_up - up_market_prob) / up_market_prob
        } else { 0.0 };
        let terminal_down_edge = if down_market_prob > 0.0 {
            (our_p_down - down_market_prob) / down_market_prob
        } else { 0.0 };

        // ═══════════════════════════════════════════════════════════════
        // STRATEGY 2: EXIT STRATEGY EV (new strategy - sell at target)
        // Prefer crossing_matrix (dynamic targets) over fp_matrix (legacy)
        // ═══════════════════════════════════════════════════════════════
        let (up_exit_result, down_exit_result) = if self.crossing_matrix.is_some() {
            // Use crossing-based calculation (more conservative, higher hit rate)
            (
                self.calculate_crossing_exit_ev(time_bucket, delta_bucket, BetDirection::Up, up_entry_price),
                self.calculate_crossing_exit_ev(time_bucket, delta_bucket, BetDirection::Down, down_entry_price),
            )
        } else {
            // Fallback to first-passage matrix (legacy)
            (
                self.calculate_exit_strategy_ev(time_bucket, delta_bucket, BetDirection::Up, up_entry_price),
                self.calculate_exit_strategy_ev(time_bucket, delta_bucket, BetDirection::Down, down_entry_price),
            )
        };

        let up_exit_ev = up_exit_result.as_ref().map(|r| r.ev_return_pct).unwrap_or(0.0);
        let down_exit_ev = down_exit_result.as_ref().map(|r| r.ev_return_pct).unwrap_or(0.0);

        debug!(
            "Terminal edges: UP={:.2}%, DOWN={:.2}% | Exit EV: UP={:.2}%, DOWN={:.2}%",
            terminal_up_edge * 100.0, terminal_down_edge * 100.0,
            up_exit_ev * 100.0, down_exit_ev * 100.0
        );

        // ═══════════════════════════════════════════════════════════════
        // FIND BEST OPPORTUNITY FROM EITHER STRATEGY
        // ═══════════════════════════════════════════════════════════════

        // Collect all opportunities: (direction, edge, strategy_type, exit_result, max_bet)
        let mut opportunities: Vec<(BetDirection, f64, &str, Option<ExitStrategyResult>, f64)> = Vec::new();

        let terminal_min_edge = self.config.terminal_strategy.min_edge;
        let exit_min_edge = self.config.exit_strategy.min_edge;

        // Terminal edge opportunities (if terminal strategy is available)
        if terminal_available {
            if terminal_up_edge >= terminal_min_edge {
                opportunities.push((
                    BetDirection::Up,
                    terminal_up_edge,
                    "TERMINAL",
                    None,
                    self.config.terminal_strategy.max_bet_usdc
                ));
            }
            if terminal_down_edge >= terminal_min_edge {
                opportunities.push((
                    BetDirection::Down,
                    terminal_down_edge,
                    "TERMINAL",
                    None,
                    self.config.terminal_strategy.max_bet_usdc
                ));
            }
        }

        // Exit strategy opportunities (if exit strategy is available)
        if exit_available {
            if up_exit_ev >= exit_min_edge {
                if let Some(result) = up_exit_result {
                    // Only use exit strategy if there's an actual exit target (not just holding)
                    if result.best_target < 1.0 {
                        opportunities.push((
                            BetDirection::Up,
                            up_exit_ev,
                            "EXIT",
                            Some(result),
                            self.config.exit_strategy.max_bet_usdc
                        ));
                    }
                }
            }
            if down_exit_ev >= exit_min_edge {
                if let Some(result) = down_exit_result {
                    if result.best_target < 1.0 {
                        opportunities.push((
                            BetDirection::Down,
                            down_exit_ev,
                            "EXIT",
                            Some(result),
                            self.config.exit_strategy.max_bet_usdc
                        ));
                    }
                }
            }
        }

        // No opportunities found
        if opportunities.is_empty() {
            return BetDecision::no_bet(format!(
                "No edge: terminal UP={:.1}%/{:.1}% DOWN={:.1}%/{:.1}%, exit UP={:.1}%/{:.1}% DOWN={:.1}%/{:.1}%",
                terminal_up_edge * 100.0, terminal_min_edge * 100.0,
                terminal_down_edge * 100.0, terminal_min_edge * 100.0,
                up_exit_ev * 100.0, exit_min_edge * 100.0,
                down_exit_ev * 100.0, exit_min_edge * 100.0
            ));
        }

        // Pick the best opportunity (highest edge)
        let (direction, edge, strategy, exit_result, max_bet) = opportunities
            .into_iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .unwrap();

        let (our_prob, market_prob) = match direction {
            BetDirection::Up => (our_p_up, up_entry_price),
            BetDirection::Down => (our_p_down, down_entry_price),
        };

        // Log which strategy won
        match (strategy, &exit_result) {
            ("EXIT", Some(result)) => {
                info!(
                    "EXIT STRATEGY: {:?} at {:.0}¢ → target {:.0}¢ (P={:.1}%), EV={:.1}% return, max=${:.0}",
                    direction,
                    market_prob * 100.0,
                    result.best_target * 100.0,
                    result.p_reach_target * 100.0,
                    edge * 100.0,
                    max_bet
                );
            }
            _ => {
                info!(
                    "TERMINAL STRATEGY: {:?} edge={:.1}%, our_P={:.1}%, market={:.0}¢, max=${:.0} → hold to settlement",
                    direction,
                    edge * 100.0,
                    our_prob * 100.0,
                    market_prob * 100.0,
                    max_bet
                );
            }
        }

        // Calculate bet size using Kelly, capped by strategy-specific max
        let kelly_bet = self.calculate_bet_size(edge, market_prob, cell);
        let bet_amount = kelly_bet.min(max_bet);  // Apply strategy-specific max

        if bet_amount < self.config.betting.min_bet_usdc {
            return BetDecision::no_bet(format!(
                "Bet size too small: ${:.2} < ${:.2}",
                bet_amount, self.config.betting.min_bet_usdc
            ));
        }

        // Determine exit target based on strategy
        let (exit_target, strategy_type) = match (strategy, &exit_result) {
            ("EXIT", Some(result)) => (Some(result.best_target), "EXIT".to_string()),
            _ => (None, "TERMINAL".to_string()),  // None = hold to settlement
        };

        let min_edge_used = if strategy == "EXIT" { exit_min_edge } else { terminal_min_edge };

        info!(
            "BET {:?} [{}]: edge={:.2}%, amount=${:.2} (max ${:.0}), exit={:?}",
            direction,
            strategy_type,
            edge * 100.0,
            bet_amount,
            max_bet,
            exit_target.map(|t| format!("{:.0}¢", t * 100.0)).unwrap_or("HOLD".to_string())
        );

        BetDecision {
            should_bet: true,
            direction: Some(direction),
            edge,
            our_probability: our_prob,
            market_probability: market_prob,
            bet_amount,
            confidence: cell.confidence_level.clone(),
            reason: format!(
                "{} strategy: edge {:.1}% > {:.1}% min",
                strategy_type,
                edge * 100.0,
                min_edge_used * 100.0
            ),
            exit_target,
            strategy_type,
        }
    }

    /// Calculate bet size using fractional Kelly
    fn calculate_bet_size(&self, edge: f64, market_prob: f64, cell: &CellStats) -> f64 {
        // Kelly formula: (p*b - q) / b
        // where p = our probability, b = odds - 1, q = 1 - p
        let odds = 1.0 / market_prob;
        let b = odds - 1.0;
        let p = market_prob + edge * market_prob; // our probability
        let q = 1.0 - p;

        let kelly = if b > 0.0 { (p * b - q) / b } else { 0.0 };
        let kelly = kelly.max(0.0); // Never negative

        // Apply fractional Kelly
        let fraction = self.config.betting.kelly_fraction;

        // Apply confidence multiplier
        let confidence_str = format!("{:?}", cell.confidence_level);
        let confidence_mult = self.config.confidence_multiplier(&confidence_str);

        // Apply loss reduction
        let loss_mult = self
            .config
            .risk
            .loss_reduction_factor
            .powi(self.consecutive_losses as i32);

        let adjusted_kelly = kelly * fraction * confidence_mult * loss_mult;

        // Calculate bet amount
        let mut bet = self.bankroll * adjusted_kelly;

        // Apply caps
        bet = bet.min(self.bankroll * self.config.betting.max_bet_pct);
        bet = bet.min(self.config.betting.max_bet_usdc);

        bet
    }

    /// Calculate the best exit strategy EV for a given entry
    ///
    /// Returns the best exit target and expected value, considering all possible targets
    pub fn calculate_exit_strategy_ev(
        &self,
        time_bucket: u8,
        delta_bucket: i8,
        direction: BetDirection,
        entry_price: f64,
    ) -> Option<ExitStrategyResult> {
        let fp_matrix = self.fp_matrix?;

        let state = fp_matrix.get(time_bucket, delta_bucket);
        let terminal_cell = self.matrix.get(time_bucket, delta_bucket);

        // Calculate EV of holding to settlement
        let p_win = match direction {
            BetDirection::Up => terminal_cell.p_up,
            BetDirection::Down => terminal_cell.p_down,
        };
        let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

        // Target prices to evaluate
        let targets: Vec<f64> = vec![
            0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95
        ];

        let mut best_ev = hold_ev;  // Start with hold as baseline
        let mut best_target = 1.0;   // 1.0 means hold to settlement
        let mut best_p_reach = p_win;

        for target_price in targets {
            // Skip targets at or below entry (no profit possible)
            if target_price <= entry_price + 0.01 {
                continue;
            }

            // Map target price to delta bucket
            let target_delta_bucket = price_to_delta_bucket(target_price, direction);

            // Get P(reach target) from first-passage matrix
            let fp_cell = match direction {
                BetDirection::Up => state.get_up_target(target_delta_bucket),
                BetDirection::Down => state.get_down_target(target_delta_bucket),
            };

            // Only use strong confidence cells
            if self.config.exit_strategy.only_strong_confidence
                && fp_cell.confidence_level != ConfidenceLevel::Strong {
                continue;
            }

            let p_reach = fp_cell.p_reach;
            let gain = target_price - entry_price;

            // EV of exit strategy: P(reach) × gain + P(not reach) × hold_ev
            let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;

            if exit_ev > best_ev {
                best_ev = exit_ev;
                best_target = target_price;
                best_p_reach = p_reach;
            }
        }

        // Calculate EV as return percentage
        let ev_return_pct = if entry_price > 0.0 {
            best_ev / entry_price
        } else {
            0.0
        };

        Some(ExitStrategyResult {
            best_target,
            best_ev,
            ev_return_pct,
            p_reach_target: best_p_reach,
        })
    }

    /// Calculate the best exit strategy EV using CROSSING MATRIX
    ///
    /// This uses P(reach) = 1 - e^(-avg_crossings) (Poisson approximation)
    /// Returns the best exit target and expected value
    pub fn calculate_crossing_exit_ev(
        &self,
        time_bucket: u8,
        delta_bucket: i8,
        direction: BetDirection,
        entry_price: f64,
    ) -> Option<ExitStrategyResult> {
        let crossing_matrix = self.crossing_matrix?;
        let terminal_cell = self.matrix.get(time_bucket, delta_bucket);

        // Calculate EV of holding to settlement
        let p_win = match direction {
            BetDirection::Up => terminal_cell.p_up,
            BetDirection::Down => terminal_cell.p_down,
        };
        let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

        let crossing_state = crossing_matrix.get(time_bucket, delta_bucket);

        // Not enough data
        if crossing_state.count_trajectories < 30 {
            return None;
        }

        let mut best_ev = hold_ev;
        let mut best_target = 1.0; // 1.0 means hold
        let mut best_p_reach = p_win;

        // Evaluate each price level (4¢, 8¢, ..., 100¢)
        // Index 0 = 4¢, 1 = 8¢, ..., 24 = 100¢
        for level in 0..25 {
            let target_price = (level + 1) as f64 * 0.04; // 0.04, 0.08, ..., 1.00

            // Skip targets at or below entry (no profit)
            if target_price <= entry_price + 0.01 {
                continue;
            }

            // Get average crossings for this level
            let avg_crossings = crossing_state.avg_crossings[level];

            // P(reach) = 1 - e^(-avg_crossings) (at least one crossing)
            let p_reach = 1.0 - (-avg_crossings).exp();

            // Must have reasonable P(reach) to be viable
            if p_reach < 0.20 {
                continue;
            }

            let gain = target_price - entry_price;

            // EV of exit strategy: P(reach) × gain + P(not reach) × hold_ev
            let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;

            if exit_ev > best_ev {
                best_ev = exit_ev;
                best_target = target_price;
                best_p_reach = p_reach;
            }
        }

        // Calculate EV as return percentage
        let ev_return_pct = if entry_price > 0.0 {
            best_ev / entry_price
        } else {
            0.0
        };

        Some(ExitStrategyResult {
            best_target,
            best_ev,
            ev_return_pct,
            p_reach_target: best_p_reach,
        })
    }
}

impl BetDecision {
    pub fn no_bet(reason: String) -> Self {
        Self {
            should_bet: false,
            direction: None,
            edge: 0.0,
            our_probability: 0.0,
            market_probability: 0.0,
            bet_amount: 0.0,
            confidence: ConfidenceLevel::Unreliable,
            reason,
            exit_target: None,
            strategy_type: "NONE".to_string(),
        }
    }
}

// ============================================================================
// EXIT STRATEGY
// ============================================================================

/// Decision about whether to exit a position
#[derive(Debug, Clone)]
pub struct ExitDecision {
    pub should_exit: bool,
    pub exit_price: f64,
    pub ev_exit: f64,
    pub ev_hold: f64,
    pub ev_improvement: f64,
    pub reason: String,
}

impl ExitDecision {
    pub fn no_exit(reason: String) -> Self {
        Self {
            should_exit: false,
            exit_price: 0.0,
            ev_exit: 0.0,
            ev_hold: 0.0,
            ev_improvement: 0.0,
            reason,
        }
    }
}

/// Calculate the best exit target price from first-passage matrix
///
/// Returns (exit_target_price, expected_ev_improvement)
pub fn calculate_exit_target(
    fp_matrix: &FirstPassageMatrix,
    terminal_matrix: &ProbabilityMatrix,
    time_bucket: u8,
    delta_bucket: i8,
    direction: BetDirection,
    entry_price: f64,
    only_strong: bool,
) -> Option<(f64, f64)> {
    let state = fp_matrix.get(time_bucket, delta_bucket);
    let terminal_cell = terminal_matrix.get(time_bucket, delta_bucket);

    // Calculate EV of holding to settlement
    let p_win = match direction {
        BetDirection::Up => terminal_cell.p_up,
        BetDirection::Down => terminal_cell.p_down,
    };
    let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

    // Target prices to consider (from entry price upward)
    let targets: Vec<f64> = vec![0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90];

    let mut best_ev = hold_ev;
    let mut best_target: Option<f64> = None;
    let mut best_improvement = 0.0;

    for target_price in targets {
        // Skip targets at or below entry price (no profit)
        if target_price <= entry_price + 0.01 {
            continue;
        }

        // Map target price to delta bucket (simplified mapping)
        // Higher prices correspond to more extreme deltas
        let target_delta_bucket = price_to_delta_bucket(target_price, direction);

        // Get P(reach target) from first-passage matrix
        let fp_cell = match direction {
            BetDirection::Up => state.get_up_target(target_delta_bucket),
            BetDirection::Down => state.get_down_target(target_delta_bucket),
        };

        // Skip if insufficient confidence
        if only_strong && fp_cell.confidence_level != ConfidenceLevel::Strong {
            continue;
        }

        let p_reach = fp_cell.p_reach;
        let gain = target_price - entry_price;

        // EV of exit strategy: P(reach) × gain + P(not reach) × hold_ev
        let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;
        let improvement = exit_ev - hold_ev;

        if exit_ev > best_ev {
            best_ev = exit_ev;
            best_target = Some(target_price);
            best_improvement = improvement;
        }
    }

    best_target.map(|t| (t, best_improvement))
}

/// Check if we should exit a position
pub fn decide_exit(
    config: &BotConfig,
    fp_matrix: &FirstPassageMatrix,
    terminal_matrix: &ProbabilityMatrix,
    time_bucket: u8,
    delta_bucket: i8,
    direction: BetDirection,
    entry_price: f64,
    current_bid: f64,
    exit_target: f64,
) -> ExitDecision {
    // Check if exit strategy is enabled
    if !config.exit_strategy.enabled {
        return ExitDecision::no_exit("Exit strategy disabled".to_string());
    }

    // Get terminal probability at CURRENT state
    let terminal_cell = terminal_matrix.get(time_bucket, delta_bucket);
    let p_win = match direction {
        BetDirection::Up => terminal_cell.p_up,
        BetDirection::Down => terminal_cell.p_down,
    };

    // Calculate hold EV from current state
    let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

    // Check if current bid meets or exceeds exit target (TAKE PROFIT)
    if current_bid >= exit_target {
        let exit_ev = current_bid - entry_price;
        let improvement = (exit_ev - hold_ev) * 100.0; // in cents

        info!(
            "EXIT TARGET HIT! bid={:.2}¢ >= target={:.2}¢, EV improvement={:.2}¢",
            current_bid * 100.0,
            exit_target * 100.0,
            improvement
        );

        return ExitDecision {
            should_exit: true,
            exit_price: current_bid,
            ev_exit: exit_ev,
            ev_hold: hold_ev,
            ev_improvement: improvement,
            reason: format!(
                "Target reached: {:.0}¢ >= {:.0}¢",
                current_bid * 100.0,
                exit_target * 100.0
            ),
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // STOP-LOSS CHECK: Recalculate forward-looking EV from current state
    // If EV has turned significantly negative, exit to cut losses
    // ═══════════════════════════════════════════════════════════════

    // Get P(reach target) from first-passage matrix at CURRENT state
    let state = fp_matrix.get(time_bucket, delta_bucket);

    // Map exit target to delta bucket (same logic as entry)
    let target_delta_bucket = price_to_delta_bucket(exit_target, direction);

    // Get P(reach target) from current state
    let fp_cell = match direction {
        BetDirection::Up => state.get_up_target(target_delta_bucket),
        BetDirection::Down => state.get_down_target(target_delta_bucket),
    };

    // Only use strong/moderate confidence for stop-loss decisions
    let p_reach = if fp_cell.confidence_level == crate::models::ConfidenceLevel::Strong
        || fp_cell.confidence_level == crate::models::ConfidenceLevel::Moderate
    {
        fp_cell.p_reach
    } else {
        // If unreliable data, don't trigger stop-loss based on P(reach)
        0.5 // Conservative assumption
    };

    // Calculate forward-looking EV from current state
    let gain_if_hit = exit_target - entry_price;
    let forward_ev = p_reach * gain_if_hit + (1.0 - p_reach) * hold_ev;

    // Calculate what we'd get if we sell now
    let exit_now_pnl = current_bid - entry_price;

    // ═══════════════════════════════════════════════════════════════
    // EARLY TAKE-PROFIT: Lock in gains when edge disappears
    // If we're in profit but forward EV is negative, sell before reversal
    // ═══════════════════════════════════════════════════════════════
    if exit_now_pnl > 0.0 && forward_ev < 0.0 {
        let improvement = (exit_now_pnl - forward_ev) * 100.0;

        info!(
            "EARLY TAKE-PROFIT! In profit {:.1}¢ but forward_ev={:.2}¢ negative. Locking gains.",
            exit_now_pnl * 100.0,
            forward_ev * 100.0
        );

        return ExitDecision {
            should_exit: true,
            exit_price: current_bid,
            ev_exit: exit_now_pnl,
            ev_hold: forward_ev,
            ev_improvement: improvement,
            reason: format!(
                "EARLY TAKE-PROFIT: +{:.1}¢ profit, forward_ev={:.1}¢ negative",
                exit_now_pnl * 100.0,
                forward_ev * 100.0
            ),
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // STOP-LOSS: Cut losses when edge turns against us
    // Exit if forward EV is negative AND P(win) dropped below 45%
    // ═══════════════════════════════════════════════════════════════
    let p_win_threshold = 0.45;

    if forward_ev < 0.0 && p_win < p_win_threshold && exit_now_pnl < 0.0 {
        let improvement = (exit_now_pnl - forward_ev) * 100.0;

        warn!(
            "STOP-LOSS TRIGGERED! P(win)={:.0}%, P(reach)={:.0}%, forward_ev={:.2}¢, bid={:.0}¢",
            p_win * 100.0,
            p_reach * 100.0,
            forward_ev * 100.0,
            current_bid * 100.0
        );

        return ExitDecision {
            should_exit: true,
            exit_price: current_bid,
            ev_exit: exit_now_pnl,
            ev_hold: forward_ev,
            ev_improvement: improvement,
            reason: format!(
                "STOP-LOSS: forward_ev={:.1}¢ negative, P(win)={:.0}%",
                forward_ev * 100.0,
                p_win * 100.0
            ),
        };
    }

    ExitDecision::no_exit(format!(
        "Bid {:.0}¢ < target {:.0}¢ | P(win)={:.0}% P(reach)={:.0}% fwd_ev={:.1}¢",
        current_bid * 100.0,
        exit_target * 100.0,
        p_win * 100.0,
        p_reach * 100.0,
        forward_ev * 100.0
    ))
}

// ============================================================================
// CROSSING-BASED EXIT STRATEGY (Dynamic Target Updates)
// ============================================================================

/// Calculate best exit target from current state using crossing matrix
/// Returns (best_target_price, best_ev) or None if no good target
pub fn calculate_crossing_target(
    crossing_matrix: &PriceCrossingMatrix,
    terminal_matrix: &ProbabilityMatrix,
    time_bucket: u8,
    delta_bucket: i8,
    entry_price: f64,
) -> Option<(f64, f64)> {
    let terminal_cell = terminal_matrix.get(time_bucket, delta_bucket);
    let crossing_state = crossing_matrix.get(time_bucket, delta_bucket);

    // Need sufficient data
    if crossing_state.count_trajectories < 30 {
        return None;
    }

    // Calculate hold EV (P(UP) for now, direction doesn't matter for target calc)
    let p_up = terminal_cell.p_up;
    let hold_ev = p_up * (1.0 - entry_price) - (1.0 - p_up) * entry_price;

    let mut best_ev = hold_ev;
    let mut best_target: Option<f64> = None;

    // Evaluate each price level (4¢, 8¢, ..., 100¢)
    for level in 0..25 {
        let target_price = (level + 1) as f64 * 0.04;

        // Skip targets at or below entry
        if target_price <= entry_price + 0.01 {
            continue;
        }

        let avg_crossings = crossing_state.avg_crossings[level];
        let p_reach = 1.0 - (-avg_crossings).exp();

        // Must have reasonable P(reach)
        if p_reach < 0.15 {
            continue;
        }

        let gain = target_price - entry_price;
        let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;

        if exit_ev > best_ev {
            best_ev = exit_ev;
            best_target = Some(target_price);
        }
    }

    best_target.map(|t| (t, best_ev))
}

/// Dynamic exit decision using crossing matrix
/// Key features:
/// 1. Recalculates best target from CURRENT state (not fixed at entry)
/// 2. Sells immediately if current_price > best_target_EV
/// 3. Updates target dynamically every tick
pub fn decide_exit_crossing(
    config: &BotConfig,
    crossing_matrix: &PriceCrossingMatrix,
    terminal_matrix: &ProbabilityMatrix,
    time_bucket: u8,
    delta_bucket: i8,
    direction: BetDirection,
    entry_price: f64,
    current_bid: f64,
    _original_exit_target: f64,  // Ignored - we recalculate dynamically
) -> ExitDecision {
    if !config.exit_strategy.enabled {
        return ExitDecision::no_exit("Exit strategy disabled".to_string());
    }

    let terminal_cell = terminal_matrix.get(time_bucket, delta_bucket);
    let p_win = match direction {
        BetDirection::Up => terminal_cell.p_up,
        BetDirection::Down => terminal_cell.p_down,
    };

    // Calculate hold EV from current state
    let hold_ev = p_win * (1.0 - entry_price) - (1.0 - p_win) * entry_price;

    // What we'd get if we sell now
    let exit_now_pnl = current_bid - entry_price;

    // ═══════════════════════════════════════════════════════════════
    // DYNAMIC TARGET CALCULATION using crossing matrix
    // ═══════════════════════════════════════════════════════════════
    let crossing_state = crossing_matrix.get(time_bucket, delta_bucket);

    let mut best_target_ev = hold_ev;
    let mut best_target_price = 1.0; // 1.0 = hold to settlement
    let mut best_p_reach = p_win;
    let has_crossing_data = crossing_state.count_trajectories >= 30;

    if has_crossing_data {
        for level in 0..25 {
            let target_price = (level + 1) as f64 * 0.04;

            if target_price <= entry_price + 0.01 {
                continue;
            }

            let avg_crossings = crossing_state.avg_crossings[level];
            let p_reach = 1.0 - (-avg_crossings).exp();

            if p_reach < 0.15 {
                continue;
            }

            let gain = target_price - entry_price;
            let exit_ev = p_reach * gain + (1.0 - p_reach) * hold_ev;

            if exit_ev > best_target_ev {
                best_target_ev = exit_ev;
                best_target_price = target_price;
                best_p_reach = p_reach;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // EXIT RULE: Compare SELL NOW profit vs HOLD expected value
    // Every tick, we ask: "Is selling now better than holding?"
    // ═══════════════════════════════════════════════════════════════
    let profit_if_sell_now = current_bid - entry_price;

    // If selling now gives more profit than the expected value of holding, SELL!
    if has_crossing_data && profit_if_sell_now > best_target_ev {
        info!(
            "SELL NOW! profit {:.1}¢ > hold_ev {:.1}¢ | bid={:.0}¢ entry={:.0}¢",
            profit_if_sell_now * 100.0,
            best_target_ev * 100.0,
            current_bid * 100.0,
            entry_price * 100.0
        );

        return ExitDecision {
            should_exit: true,
            exit_price: current_bid,
            ev_exit: profit_if_sell_now,
            ev_hold: best_target_ev,
            ev_improvement: (profit_if_sell_now - best_target_ev) * 100.0,
            reason: format!(
                "Profit {:.1}¢ > HoldEV {:.1}¢",
                profit_if_sell_now * 100.0,
                best_target_ev * 100.0
            ),
        };
    }

    // EARLY TAKE-PROFIT removed - redundant with rule 1 (bid > EV)

    // ═══════════════════════════════════════════════════════════════
    // STOP-LOSS: DISABLED for crossing strategy
    // The crossing strategy bets on temporary oscillations, not terminal outcomes.
    // We let positions ride until target is hit or window settles.
    // ═══════════════════════════════════════════════════════════════
    // (removed - was conflicting with crossing strategy logic)

    // Log dynamic target info
    let target_str = if !has_crossing_data {
        format!("HOLD (no data: {} < 30)", crossing_state.count_trajectories)
    } else if best_target_price < 1.0 {
        format!("{:.0}¢", best_target_price * 100.0)
    } else {
        "HOLD".to_string()
    };

    ExitDecision::no_exit(format!(
        "bid={:.0}¢ | target={} P={:.0}% | fwd_ev={:.1}¢ | P(win)={:.0}%",
        current_bid * 100.0,
        target_str,
        best_p_reach * 100.0,
        best_target_ev * 100.0,
        p_win * 100.0
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bet_direction() {
        assert_eq!(BetDirection::Up, BetDirection::Up);
        assert_ne!(BetDirection::Up, BetDirection::Down);
    }
}
