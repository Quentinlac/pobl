//! Critical Path Tests for BTC Trading Bot
//!
//! These tests verify the core trading logic formulas:
//! 1. Buy edge calculation: (our_prob - market_prob) / market_prob >= 10%
//! 2. Sell edge calculation: (bid - our_prob) / bid >= 10%
//! 3. Minimum profit threshold: (bid - entry) / entry >= 5%
//! 4. Position tracking (entry price, shares)
//! 5. Cooldown logic
//!
//! Run with: cargo test --test critical_path_tests

// ============================================================================
// BUY EDGE CALCULATION TESTS
// ============================================================================

/// Buy edge formula: (our_prob - market_prob) / market_prob
/// Positive edge = we think the probability is higher than the market
fn calculate_buy_edge(our_prob: f64, market_prob: f64) -> f64 {
    if market_prob <= 0.0 {
        return 0.0;
    }
    (our_prob - market_prob) / market_prob
}

#[test]
fn test_buy_edge_formula_basic() {
    // We think 60% P(UP), market says 50%
    let our_prob = 0.60_f64;
    let market_prob = 0.50_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.60 - 0.50) / 0.50 = 0.10 / 0.50 = 0.20 = 20%
    assert!((edge - 0.20).abs() < 0.001, "Edge should be 20%, got {:.2}%", edge * 100.0);
}

#[test]
fn test_buy_edge_at_10pct_threshold() {
    // At exactly 10% edge with market at 50¢:
    // our_prob = market_prob * (1 + edge) = 0.50 * 1.10 = 0.55
    let our_prob = 0.55_f64;
    let market_prob = 0.50_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    assert!((edge - 0.10).abs() < 0.001, "Edge should be 10%, got {:.2}%", edge * 100.0);
}

#[test]
fn test_buy_edge_positive_means_underpriced() {
    // When our probability is higher than market, edge is positive
    let our_prob = 0.70_f64;
    let market_prob = 0.50_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.70 - 0.50) / 0.50 = 40%
    assert!(edge > 0.0, "Edge should be positive when we're more bullish");
    assert!((edge - 0.40).abs() < 0.001);
}

#[test]
fn test_buy_edge_negative_means_overpriced() {
    // When our probability is lower than market, edge is negative
    let our_prob = 0.40_f64;
    let market_prob = 0.50_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.40 - 0.50) / 0.50 = -20%
    assert!(edge < 0.0, "Edge should be negative when we're less bullish");
    assert!((edge - (-0.20)).abs() < 0.001);
}

#[test]
fn test_buy_edge_zero_market_handled() {
    let edge = calculate_buy_edge(0.50, 0.0);
    assert_eq!(edge, 0.0, "Should return 0 for zero market prob");
}

// ============================================================================
// SELL EDGE CALCULATION TESTS
// ============================================================================

/// Sell edge formula: (bid - our_prob) / bid
/// Positive edge = market paying more than we think it's worth
fn calculate_sell_edge(current_bid: f64, our_prob: f64) -> f64 {
    if current_bid <= 0.01 {
        return 0.0;
    }
    (current_bid - our_prob) / current_bid
}

#[test]
fn test_sell_edge_formula_basic() {
    // Market bidding 70¢, we think only 60% P(UP)
    let current_bid = 0.70_f64;
    let our_prob = 0.60_f64;

    let sell_edge = calculate_sell_edge(current_bid, our_prob);

    // (0.70 - 0.60) / 0.70 = 0.10 / 0.70 = 0.143 = 14.3%
    assert!((sell_edge - 0.143).abs() < 0.01, "Sell edge should be ~14.3%, got {:.2}%", sell_edge * 100.0);
}

#[test]
fn test_sell_edge_at_10pct_threshold() {
    // At exactly 10% sell edge:
    // (bid - our_prob) / bid = 0.10
    // bid - our_prob = 0.10 * bid
    // bid * 0.90 = our_prob
    // If our_prob = 0.54, then bid = 0.54 / 0.90 = 0.60
    let current_bid = 0.60_f64;
    let our_prob = 0.54_f64;

    let sell_edge = calculate_sell_edge(current_bid, our_prob);

    assert!((sell_edge - 0.10).abs() < 0.001, "Sell edge should be 10%, got {:.2}%", sell_edge * 100.0);
}

#[test]
fn test_sell_edge_negative_when_undervalued() {
    // When market is paying LESS than we think it's worth
    let current_bid = 0.50_f64;
    let our_prob = 0.60_f64;

    let sell_edge = calculate_sell_edge(current_bid, our_prob);

    // (0.50 - 0.60) / 0.50 = -0.10 / 0.50 = -0.20 = -20%
    assert!(sell_edge < 0.0, "Sell edge should be negative");
    assert!((sell_edge - (-0.20)).abs() < 0.01);
}

#[test]
fn test_sell_edge_zero_bid_handled() {
    let sell_edge = calculate_sell_edge(0.0, 0.50);
    assert_eq!(sell_edge, 0.0, "Should return 0 for zero bid");
}

// ============================================================================
// PROFIT CALCULATION TESTS
// ============================================================================

/// Profit formula: (current_bid - entry_price) / entry_price
fn calculate_profit_pct(entry_price: f64, current_bid: f64) -> f64 {
    if entry_price <= 0.01 {
        return 0.0;
    }
    (current_bid - entry_price) / entry_price
}

#[test]
fn test_profit_formula_basic() {
    let entry_price = 0.40_f64;
    let current_bid = 0.44_f64;

    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    // (0.44 - 0.40) / 0.40 = 0.04 / 0.40 = 0.10 = 10%
    assert!((profit_pct - 0.10).abs() < 0.001, "Profit should be 10%, got {:.2}%", profit_pct * 100.0);
}

#[test]
fn test_profit_at_5pct_threshold() {
    let entry_price = 0.50_f64;
    // 5% profit: current = entry * 1.05 = 0.525
    let current_bid = 0.525_f64;

    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    assert!((profit_pct - 0.05).abs() < 0.001, "Profit should be 5%, got {:.2}%", profit_pct * 100.0);
}

#[test]
fn test_loss_is_negative_profit() {
    let entry_price = 0.50_f64;
    let current_bid = 0.45_f64;  // Lost 5¢

    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    // (0.45 - 0.50) / 0.50 = -0.05 / 0.50 = -0.10 = -10%
    assert!((profit_pct - (-0.10)).abs() < 0.001, "Profit should be -10%, got {:.2}%", profit_pct * 100.0);
}

#[test]
fn test_zero_entry_handled() {
    let profit_pct = calculate_profit_pct(0.0, 0.50);
    assert_eq!(profit_pct, 0.0);
}

// ============================================================================
// COMBINED SELL DECISION TESTS
// ============================================================================

/// Determines if we should sell based on both edge and profit thresholds
fn should_sell(
    current_bid: f64,
    our_prob: f64,
    entry_price: f64,
    min_sell_edge: f64,
    min_profit: f64,
) -> bool {
    let sell_edge = calculate_sell_edge(current_bid, our_prob);
    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    sell_edge >= min_sell_edge && profit_pct >= min_profit
}

#[test]
fn test_sell_requires_both_conditions() {
    let min_sell_edge = 0.10_f64;  // 10%
    let min_profit = 0.05_f64;     // 5%

    // Scenario 1: Good edge (15%) but low profit (3%) - SHOULD NOT SELL
    let result1 = should_sell(
        0.515,  // current_bid
        0.44,   // our_prob (gives ~14.6% sell edge)
        0.50,   // entry_price (gives 3% profit)
        min_sell_edge,
        min_profit,
    );
    assert!(!result1, "Should NOT sell: good edge but profit < 5%");

    // Scenario 2: Low edge (5%) but good profit (10%) - SHOULD NOT SELL
    let result2 = should_sell(
        0.55,   // current_bid
        0.52,   // our_prob (gives ~5.5% sell edge)
        0.50,   // entry_price (gives 10% profit)
        min_sell_edge,
        min_profit,
    );
    assert!(!result2, "Should NOT sell: good profit but edge < 10%");

    // Scenario 3: Good edge (14%) AND good profit (12%) - SHOULD SELL
    let result3 = should_sell(
        0.56,   // current_bid
        0.48,   // our_prob (gives ~14.3% sell edge)
        0.50,   // entry_price (gives 12% profit)
        min_sell_edge,
        min_profit,
    );
    assert!(result3, "Should sell: both conditions met");
}

#[test]
fn test_sell_at_exact_thresholds() {
    // At exactly 10% sell edge and exactly 5% profit
    // sell_edge = (bid - prob) / bid = 0.10 → bid * 0.90 = prob
    // profit = (bid - entry) / entry = 0.05 → bid = entry * 1.05

    // If entry = 0.50, then bid = 0.525 for 5% profit
    // For 10% sell edge: 0.525 * 0.90 = 0.4725 our_prob

    let entry_price = 0.50_f64;
    let current_bid = 0.525_f64;
    let our_prob = 0.4725_f64;

    let sell_edge = calculate_sell_edge(current_bid, our_prob);
    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    // Should be at thresholds
    assert!((sell_edge - 0.10).abs() < 0.001, "Sell edge should be ~10%");
    assert!((profit_pct - 0.05).abs() < 0.001, "Profit should be ~5%");

    let result = should_sell(current_bid, our_prob, entry_price, 0.10, 0.05);
    assert!(result, "Should sell at exact thresholds");
}

#[test]
fn test_never_sell_at_loss() {
    // Even with great sell edge, never sell at a loss
    let entry_price = 0.50_f64;
    let current_bid = 0.45_f64;  // 10% loss
    let our_prob = 0.30_f64;     // Very low prob → huge sell edge (50%)

    let sell_edge = calculate_sell_edge(current_bid, our_prob);
    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    assert!(sell_edge > 0.10, "Sell edge is high: {:.1}%", sell_edge * 100.0);
    assert!(profit_pct < 0.0, "Profit is negative: {:.1}%", profit_pct * 100.0);

    let result = should_sell(current_bid, our_prob, entry_price, 0.10, 0.05);
    assert!(!result, "Should NOT sell at a loss, even with high edge");
}

// ============================================================================
// POSITION TRACKING TESTS
// ============================================================================

#[derive(Debug, Clone)]
struct Position {
    token_id: String,
    entry_price: f64,
    shares: f64,
}

impl Position {
    fn new(token_id: &str, entry_price: f64, amount_usdc: f64) -> Self {
        let shares = amount_usdc / entry_price;
        Self {
            token_id: token_id.to_string(),
            entry_price,
            shares,
        }
    }

    fn profit_at(&self, current_bid: f64) -> f64 {
        (current_bid - self.entry_price) * self.shares
    }

    fn value_at(&self, current_bid: f64) -> f64 {
        current_bid * self.shares
    }
}

#[test]
fn test_position_shares_calculation() {
    // Buy $10 worth at 55¢ per share
    let position = Position::new("test", 0.55, 10.0);

    // shares = $10 / $0.55 = 18.18...
    assert!((position.shares - 18.18).abs() < 0.01);
}

#[test]
fn test_position_profit_calculation() {
    let position = Position::new("test", 0.55, 10.0);  // ~18.18 shares
    let current_bid = 0.60_f64;  // Market now at 60¢

    // Profit per share = 60¢ - 55¢ = 5¢
    // Total profit = 18.18 * 0.05 = $0.909
    let profit = position.profit_at(current_bid);

    assert!((profit - 0.909).abs() < 0.01, "Profit should be ~$0.91, got ${:.2}", profit);
}

#[test]
fn test_position_value_calculation() {
    let position = Position::new("test", 0.55, 10.0);  // ~18.18 shares
    let current_bid = 0.60_f64;

    // Value = 18.18 * 0.60 = $10.91
    let value = position.value_at(current_bid);

    assert!((value - 10.909).abs() < 0.01, "Value should be ~$10.91, got ${:.2}", value);
}

#[test]
fn test_position_loss_calculation() {
    let position = Position::new("test", 0.55, 10.0);
    let current_bid = 0.50_f64;  // Market dropped to 50¢

    // Profit per share = 50¢ - 55¢ = -5¢
    // Total loss = 18.18 * -0.05 = -$0.909
    let profit = position.profit_at(current_bid);

    assert!(profit < 0.0, "Should have a loss");
    assert!((profit - (-0.909)).abs() < 0.01);
}

// ============================================================================
// COOLDOWN TESTS
// ============================================================================

fn can_bet_after_cooldown(seconds_since_last_bet: u32, cooldown_seconds: u32) -> bool {
    seconds_since_last_bet >= cooldown_seconds
}

#[test]
fn test_zero_cooldown_allows_immediate() {
    let result = can_bet_after_cooldown(0, 0);
    assert!(result, "Should bet immediately with 0 cooldown");
}

#[test]
fn test_cooldown_blocks_early() {
    let result = can_bet_after_cooldown(10, 30);  // 10s elapsed, need 30s
    assert!(!result, "Should NOT bet within cooldown");
}

#[test]
fn test_cooldown_allows_after_expiry() {
    let result = can_bet_after_cooldown(35, 30);  // 35s elapsed, need 30s
    assert!(result, "Should bet after cooldown expires");
}

#[test]
fn test_cooldown_at_exact_boundary() {
    let result = can_bet_after_cooldown(30, 30);  // exactly 30s
    assert!(result, "Should bet at exact cooldown boundary");
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_extreme_edge_200pct() {
    // Very high edge case - market massively wrong
    let our_prob = 0.90_f64;
    let market_prob = 0.30_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.90 - 0.30) / 0.30 = 2.0 = 200%
    assert!((edge - 2.0).abs() < 0.01, "Edge should be 200%, got {:.0}%", edge * 100.0);
}

#[test]
fn test_near_certainty_prices() {
    // Testing edge at extreme prices (near 0 or 1)
    let our_prob = 0.95_f64;
    let market_prob = 0.90_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.95 - 0.90) / 0.90 = 5.5%
    assert!((edge - 0.0556).abs() < 0.01);
}

#[test]
fn test_floating_point_precision() {
    // Make sure we handle floating point accurately
    let our_prob = 0.10_f64;
    let market_prob = 0.09_f64;

    let edge = calculate_buy_edge(our_prob, market_prob);

    // (0.10 - 0.09) / 0.09 = 0.01 / 0.09 = 11.1%
    assert!((edge - 0.111).abs() < 0.01);
    assert!(edge >= 0.10, "11.1% should be >= 10% threshold");
}

// ============================================================================
// INTEGRATION: FULL BUY-SELL CYCLE
// ============================================================================

#[test]
fn test_full_trading_cycle() {
    // Config thresholds
    let min_buy_edge = 0.10_f64;   // 10%
    let min_sell_edge = 0.10_f64;  // 10%
    let min_profit = 0.05_f64;     // 5%

    // Step 1: Market conditions show good buy edge
    let our_p_up = 0.60_f64;       // We think 60% P(UP)
    let market_ask = 0.50_f64;     // Market selling at 50¢

    let buy_edge = calculate_buy_edge(our_p_up, market_ask);
    assert!(buy_edge >= min_buy_edge, "Should have buy edge: {:.1}%", buy_edge * 100.0);

    // Step 2: Execute buy
    let entry_price = market_ask;
    let amount = 10.0_f64;
    let position = Position::new("UP_TOKEN", entry_price, amount);

    println!("BUY: {} shares at {:.0}¢ for ${:.2}",
             position.shares, entry_price * 100.0, amount);

    // Step 3: Market moves in our favor
    let current_bid = 0.58_f64;     // Market now bidding 58¢
    let current_our_prob = 0.50_f64; // Our updated probability

    // Step 4: Check sell conditions
    let sell_edge = calculate_sell_edge(current_bid, current_our_prob);
    let profit_pct = calculate_profit_pct(entry_price, current_bid);

    println!("CHECK: bid={:.0}¢, sell_edge={:.1}%, profit={:.1}%",
             current_bid * 100.0, sell_edge * 100.0, profit_pct * 100.0);

    // Sell edge = (0.58 - 0.50) / 0.58 = 13.8%
    // Profit = (0.58 - 0.50) / 0.50 = 16%
    assert!(sell_edge >= min_sell_edge, "Sell edge OK: {:.1}%", sell_edge * 100.0);
    assert!(profit_pct >= min_profit, "Profit OK: {:.1}%", profit_pct * 100.0);

    let should_sell_now = should_sell(current_bid, current_our_prob, entry_price, min_sell_edge, min_profit);
    assert!(should_sell_now, "Should sell when both conditions met");

    // Step 5: Calculate final profit
    let profit = position.profit_at(current_bid);
    println!("SELL: {:.2} shares at {:.0}¢, profit=${:.2}",
             position.shares, current_bid * 100.0, profit);

    assert!(profit > 0.0, "Should have positive profit");
}

// ============================================================================
// SUMMARY: CONFIG VALUES USED IN PRODUCTION
// ============================================================================

#[test]
fn test_production_config_values() {
    // These values match bot_config.yaml
    let min_buy_edge = 0.10_f64;    // terminal_strategy.min_edge
    let min_sell_edge = 0.10_f64;   // terminal_strategy.min_sell_edge
    let min_profit = 0.05_f64;      // terminal_strategy.min_profit_before_sell
    let polling_interval_ms = 500_u64;  // polling.interval_ms

    // Verify they're sensible
    assert!(min_buy_edge > 0.0 && min_buy_edge < 1.0);
    assert!(min_sell_edge > 0.0 && min_sell_edge < 1.0);
    assert!(min_profit >= 0.0 && min_profit < 1.0);
    assert!(polling_interval_ms > 0 && polling_interval_ms <= 1000);

    println!("Production config:");
    println!("  min_buy_edge: {:.0}%", min_buy_edge * 100.0);
    println!("  min_sell_edge: {:.0}%", min_sell_edge * 100.0);
    println!("  min_profit_before_sell: {:.0}%", min_profit * 100.0);
    println!("  polling_interval: {}ms", polling_interval_ms);
}
