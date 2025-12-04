use crate::models::{BetRecommendation, CellStats, ConfidenceLevel, Outcome};

/// Minimum edge required to consider betting (5%)
pub const MIN_EDGE_THRESHOLD: f64 = 0.05;

/// Minimum edge for weak confidence cells (15%)
pub const MIN_EDGE_WEAK_THRESHOLD: f64 = 0.15;

/// Convert Polymarket price to implied probability
/// Price of 0.10 (10 cents) = 10% implied probability
pub fn price_to_implied_probability(price: f64) -> f64 {
    price.clamp(0.01, 0.99)
}

/// Calculate edge: our_probability - market_implied_probability
pub fn calculate_edge(our_probability: f64, market_probability: f64) -> f64 {
    our_probability - market_probability
}

/// Determine if we should bet based on cell statistics and market price
///
/// # Arguments
/// * `cell` - The cell statistics for this (time_bucket, price_delta_bucket) combination
/// * `market_price_up` - Polymarket price for UP outcome (e.g., 0.45 = 45 cents)
/// * `bankroll` - Total bankroll in dollars
///
/// # Returns
/// A `BetRecommendation` struct with all details
pub fn get_recommendation(
    cell: &CellStats,
    market_price_up: f64,
    bankroll: f64,
) -> BetRecommendation {
    let market_prob_up = price_to_implied_probability(market_price_up);
    let market_prob_down = 1.0 - market_prob_up;

    // Use Wilson lower bound for conservative probability estimate
    let our_prob_up = cell.p_up_wilson_lower;
    let our_prob_down = 1.0 - cell.p_up_wilson_upper; // Conservative for down too

    // Calculate edges
    let edge_up = calculate_edge(our_prob_up, market_prob_up);
    let edge_down = calculate_edge(our_prob_down, market_prob_down);

    // Determine which direction has better edge
    let (best_direction, best_edge, our_prob, market_prob) = if edge_up > edge_down {
        (Outcome::Up, edge_up, our_prob_up, market_prob_up)
    } else {
        (Outcome::Down, edge_down, our_prob_down, market_prob_down)
    };

    // Determine minimum edge threshold based on confidence
    let min_edge = match cell.confidence_level {
        ConfidenceLevel::Unreliable => f64::MAX, // Never bet
        ConfidenceLevel::Weak => MIN_EDGE_WEAK_THRESHOLD,
        ConfidenceLevel::Moderate | ConfidenceLevel::Strong => MIN_EDGE_THRESHOLD,
    };

    // Should we bet?
    let should_bet = best_edge >= min_edge && cell.confidence_level != ConfidenceLevel::Unreliable;

    // Calculate Kelly fraction
    let (kelly_fraction, bet_amount) = if should_bet {
        let kelly = calculate_kelly_fraction(our_prob, market_prob, cell.confidence_level);
        let amount = kelly * bankroll;
        (kelly, amount)
    } else {
        (0.0, 0.0)
    };

    BetRecommendation {
        should_bet,
        direction: if should_bet {
            Some(best_direction)
        } else {
            None
        },
        our_probability: our_prob,
        market_probability: market_prob,
        edge: best_edge,
        confidence: cell.confidence_level,
        kelly_fraction,
        bet_amount,
        sample_count: cell.total(),
        probability_lower_bound: if best_direction == Outcome::Up {
            cell.p_up_wilson_lower
        } else {
            1.0 - cell.p_up_wilson_upper
        },
    }
}

/// Calculate fractional Kelly criterion
///
/// Kelly formula: f = (p * b - q) / b
/// where:
///   p = probability of winning
///   q = probability of losing (1 - p)
///   b = odds received on the bet (payout ratio)
///
/// For Polymarket:
///   If you buy at 0.40 (40 cents) and win, you get $1.00, so b = (1.0 - 0.40) / 0.40 = 1.5
///
/// We use fractional Kelly (0.25x) to reduce variance
pub fn calculate_kelly_fraction(
    win_probability: f64,
    market_price: f64,
    confidence: ConfidenceLevel,
) -> f64 {
    if market_price <= 0.0 || market_price >= 1.0 {
        return 0.0;
    }

    // Calculate odds: if we buy at price p, payout is (1-p)/p
    let odds = (1.0 - market_price) / market_price;

    let lose_probability = 1.0 - win_probability;

    // Kelly formula
    let kelly = (win_probability * odds - lose_probability) / odds;

    // Don't bet if Kelly is negative
    if kelly <= 0.0 {
        return 0.0;
    }

    // Apply fractional Kelly based on confidence
    let kelly_multiplier = match confidence {
        ConfidenceLevel::Unreliable => 0.0,
        ConfidenceLevel::Weak => 0.10,      // 10% Kelly for weak confidence
        ConfidenceLevel::Moderate => 0.25,  // 25% Kelly
        ConfidenceLevel::Strong => 0.50,    // 50% Kelly for strong confidence
    };

    let fractional_kelly = kelly * kelly_multiplier;

    // Cap at 10% of bankroll maximum
    fractional_kelly.min(0.10)
}

/// Calculate expected value of a bet
///
/// EV = p_win * payout - p_lose * stake
/// For $1 stake at price p:
///   EV = p_win * (1 - p) - p_lose * p
pub fn calculate_expected_value(win_probability: f64, market_price: f64, stake: f64) -> f64 {
    let payout = stake * (1.0 - market_price) / market_price;
    let lose_probability = 1.0 - win_probability;

    win_probability * payout - lose_probability * stake
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_to_implied_probability() {
        assert!((price_to_implied_probability(0.50) - 0.50).abs() < 0.001);
        assert!((price_to_implied_probability(0.10) - 0.10).abs() < 0.001);
        assert!((price_to_implied_probability(0.0) - 0.01).abs() < 0.001); // Clamped
        assert!((price_to_implied_probability(1.0) - 0.99).abs() < 0.001); // Clamped
    }

    #[test]
    fn test_calculate_edge() {
        // Our probability 65%, market says 50%
        let edge = calculate_edge(0.65, 0.50);
        assert!((edge - 0.15).abs() < 0.001);

        // Our probability 45%, market says 50%
        let edge = calculate_edge(0.45, 0.50);
        assert!((edge - (-0.05)).abs() < 0.001);
    }

    #[test]
    fn test_calculate_kelly_fraction() {
        // 60% win probability, market at 0.40 (odds of 1.5)
        let kelly = calculate_kelly_fraction(0.60, 0.40, ConfidenceLevel::Strong);

        // Full Kelly would be (0.6 * 1.5 - 0.4) / 1.5 = 0.333
        // With 50% Kelly = 0.167
        assert!(kelly > 0.10 && kelly < 0.20);

        // No edge case
        let kelly = calculate_kelly_fraction(0.40, 0.40, ConfidenceLevel::Strong);
        assert_eq!(kelly, 0.0);
    }

    #[test]
    fn test_expected_value() {
        // 60% chance to win, buying at 0.40, stake $10
        let ev = calculate_expected_value(0.60, 0.40, 10.0);
        // Payout if win: $15 (10 * 0.6 / 0.4)
        // EV = 0.6 * 15 - 0.4 * 10 = 9 - 4 = 5
        assert!((ev - 5.0).abs() < 0.1);

        // 40% chance to win, buying at 0.40, stake $10 (no edge)
        let ev = calculate_expected_value(0.40, 0.40, 10.0);
        assert!(ev.abs() < 0.1);
    }
}
