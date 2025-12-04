use super::config::BotConfig;
use super::polymarket::PriceQuote;
use crate::models::{delta_to_bucket, CellStats, ConfidenceLevel, ProbabilityMatrix};
use rust_decimal::Decimal;
use tracing::{debug, info};

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
}

/// Strategy context for making decisions
pub struct StrategyContext<'a> {
    pub config: &'a BotConfig,
    pub matrix: &'a ProbabilityMatrix,
    pub bankroll: f64,
    pub consecutive_losses: u32,
    pub bets_this_window: u32,
    pub daily_pnl: f64,
    pub open_positions: u32,
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
        // Check timing constraints
        if time_elapsed < self.config.timing.min_seconds_elapsed {
            return BetDecision::no_bet(format!(
                "Too early in window: {}s < {}s minimum",
                time_elapsed, self.config.timing.min_seconds_elapsed
            ));
        }

        let time_remaining = 900u32.saturating_sub(time_elapsed);
        if time_remaining < self.config.timing.min_seconds_remaining {
            return BetDecision::no_bet(format!(
                "Too late in window: {}s remaining < {}s minimum",
                time_remaining, self.config.timing.min_seconds_remaining
            ));
        }

        // Check risk limits
        if self.bets_this_window >= self.config.risk.max_bets_per_window {
            return BetDecision::no_bet(format!(
                "Max bets per window reached: {}",
                self.config.risk.max_bets_per_window
            ));
        }

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

        // Get matrix cell for current situation
        let time_bucket = (time_elapsed / 30).min(29) as u8;
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

        // Get minimum edge for this confidence level
        let confidence_str = format!("{:?}", cell.confidence_level);
        let min_edge = match self.config.min_edge_for_confidence(&confidence_str) {
            Some(e) => e,
            None => {
                return BetDecision::no_bet(format!(
                    "Confidence level {:?} not allowed",
                    cell.confidence_level
                ));
            }
        };

        // Calculate edge for both directions
        let up_market_prob = up_quote.mid_price;
        let down_market_prob = down_quote.mid_price;

        // Use Wilson lower bound for conservative probability estimate
        let our_p_up = cell.p_up_wilson_lower;
        let our_p_down = 1.0 - cell.p_up_wilson_upper; // Conservative P(DOWN)

        // Calculate edges
        let up_edge = if up_market_prob > 0.0 {
            (our_p_up - up_market_prob) / up_market_prob
        } else {
            0.0
        };

        let down_edge = if down_market_prob > 0.0 {
            (our_p_down - down_market_prob) / down_market_prob
        } else {
            0.0
        };

        debug!(
            "Edges: UP={:.2}%, DOWN={:.2}% (min required: {:.2}%)",
            up_edge * 100.0,
            down_edge * 100.0,
            min_edge * 100.0
        );

        // Determine best direction
        let (direction, edge, our_prob, market_prob) = if up_edge > down_edge && up_edge >= min_edge
        {
            (BetDirection::Up, up_edge, our_p_up, up_market_prob)
        } else if down_edge >= min_edge {
            (BetDirection::Down, down_edge, our_p_down, down_market_prob)
        } else {
            return BetDecision::no_bet(format!(
                "Edge too low: UP={:.2}%, DOWN={:.2}% < {:.2}%",
                up_edge * 100.0,
                down_edge * 100.0,
                min_edge * 100.0
            ));
        };

        // Calculate bet size using Kelly
        let bet_amount = self.calculate_bet_size(edge, market_prob, cell);

        if bet_amount < self.config.betting.min_bet_usdc {
            return BetDecision::no_bet(format!(
                "Bet size too small: ${:.2} < ${:.2}",
                bet_amount, self.config.betting.min_bet_usdc
            ));
        }

        info!(
            "BET {:?}: edge={:.2}%, our_p={:.2}%, market_p={:.2}%, amount=${:.2}",
            direction,
            edge * 100.0,
            our_prob * 100.0,
            market_prob * 100.0,
            bet_amount
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
                "Edge {:.1}% > {:.1}% min, {:?} confidence",
                edge * 100.0,
                min_edge * 100.0,
                cell.confidence_level
            ),
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
        }
    }
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
