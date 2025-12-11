# Finding the Best Algorithm for BTC 15-Minute Binary Options

## Executive Summary

This document summarizes our investigation into why the bot's edge calculation was producing losing trades despite showing high "edge" values. The key finding: **the probability matrix doesn't capture momentum**, leading to contrarian signals that lose 85-90% of the time.

---

## Table of Contents

1. [The Problem](#the-problem)
2. [Investigation: What Happens 10 Seconds After a Signal?](#investigation-10-seconds-later)
3. [Root Cause: Contrarian vs Momentum Trades](#root-cause)
4. [How the Probability Matrix is Built](#probability-matrix)
5. [Why the Matrix Fails](#why-matrix-fails)
6. [HOLD vs SELL Strategy Analysis](#hold-vs-sell)
7. [Advanced Techniques for Better Edge](#advanced-techniques)
8. [Recommendations](#recommendations)

---

## The Problem

We observed that high edge signals (>15%) were often losing trades. The hypothesis was that either:
1. **Latency**: We're too slow - by the time we trade, the edge is gone
2. **Stale data**: The edge is an artifact of price lag

**Spoiler**: Neither was the primary issue. The edge PERSISTS, but we're betting on the wrong side.

---

## Investigation: 10 Seconds Later {#investigation-10-seconds-later}

### Does the Edge Disappear?

| Edge Bucket | Signals | Edge at Signal | Edge 10s Later | % Decay | % Gone (<7%) |
|-------------|---------|----------------|----------------|---------|--------------|
| 50%+ | 749 | 85.1% | 60.1% | 23.4% | 11.9% |
| 30-50% | 829 | 37.9% | 27.6% | 26.4% | 14.8% |
| 20-30% | 1579 | 24.5% | 17.7% | 28.1% | 21.4% |
| 15-20% | 1611 | 17.2% | 14.6% | 15.2% | 22.3% |

**Key Finding**: The edge PERSISTS! A 50%+ edge only drops to 60% after 10 seconds. Only ~12-22% of edges disappear completely.

**Latency is NOT the problem.**

---

## Root Cause: Contrarian vs Momentum {#root-cause}

### The Smoking Gun Data

| Trade Type | Direction | Signals | Edge at Signal | Edge 10s Later | Edge Persists | WIN RATE |
|------------|-----------|---------|----------------|----------------|---------------|----------|
| **Aligned (momentum)** | DOWN | 678 | 19.3% | 13.3% | 71.8% | **54.4%** |
| **Aligned (momentum)** | UP | 1741 | 21.7% | 16.6% | 81.1% | **61.4%** |
| **Contrarian** | DOWN | 3209 | **47.7%** | **38.8%** | **78.6%** | **14.1%** |
| **Contrarian** | UP | 3106 | **37.3%** | **28.5%** | **73.3%** | **10.8%** |

### What This Means

- **Contrarian signals have HIGHER edge** (47.7% vs 19.3%)
- **Contrarian signals PERSIST** (78.6% still above 10%)
- **But contrarian signals LOSE** (14.1% win rate vs 54.4%)

### Definitions

| Term | Meaning | Example |
|------|---------|---------|
| **Aligned (Momentum)** | Bet direction = delta direction | BTC is UP $50 → bet UP |
| **Contrarian** | Bet direction ≠ delta direction | BTC is UP $50 → bet DOWN |

---

## How the Probability Matrix is Built {#probability-matrix}

### Data Flow

```
1. Load historical BTC price data (1-second candles from Binance)

2. Group into 15-minute windows (aligned to :00, :15, :30, :45)

3. Each window has:
   - OUTCOME = UP if close > open, DOWN if close < open

4. For each window, record 60 snapshots:
   - time_bucket (0-59): which 15-second period
   - delta_bucket (-17 to +16): price change from open

5. Cell (time=T, delta=D) stores:
   - count_up: how many windows at this state ended UP
   - count_down: how many windows at this state ended DOWN
   - p_up = count_up / total
```

### Matrix Structure

- **60 time buckets** (0-59): Each represents 15 seconds within the 15-minute window
- **34 delta buckets** (-17 to +16): Price delta ranges ($0-$5, $5-$10, ... $300+)
- Each cell stores: count_up, count_down, p_up, p_down, wilson bounds

---

## Why the Matrix Fails {#why-matrix-fails}

### The Fundamental Flaw

The matrix captures: **"Given state (time, delta), what's P(window ends UP)?"**

But it **ignores HOW you got to that state** (momentum):

```
Example: delta = +$80 at minute 5

Scenario A: BTC jumped +$80 in last 30 seconds (STRONG momentum)
  → Likely to STAY up → ends UP

Scenario B: BTC was +$150, then fell to +$80 (REVERSAL in progress)
  → Likely to keep falling → ends DOWN

The matrix AVERAGES both scenarios → shows ~55% UP
But the market sees the RECENT price action and prices accordingly!
```

### Why Contrarian Signals Have High Edge But Lose

When you see high edge_down with delta = +$80:

| What Matrix Thinks | What Market Sees |
|-------------------|------------------|
| "Historically, +$80 → 55% UP" | "BTC just jumped up, momentum is strong" |
| "DOWN has 45% chance!" | "DOWN has 5% chance" |
| "Edge = (45% - 5%) / 5% = 800%!" | "This is a momentum move, stay away from DOWN" |

The **market is pricing the MOMENTUM**, not just the state. Our matrix doesn't capture momentum → contrarian bets lose 85-90% of the time.

---

## HOLD vs SELL Strategy Analysis {#hold-vs-sell}

### Simulation Results (24 hours, $1 bets)

| Strategy | Trades | Win Rate | P&L |
|----------|--------|----------|-----|
| **DA + SELL** | 80 | 82.5% | $16 |
| **DA + HOLD** | 53 | 73.6% | $37 |
| **noDA + SELL** | 94 | 76.6% | $15 |
| **noDA + HOLD** | 94 | 57.4% | $48 |

### Key Findings

1. **HOLD beats SELL by 2-3x P&L**
   - SELL captures small profits (+5-20%) instead of full payout (+50-200%)
   - The few losses (-100%) are the same either way

2. **Delta Alignment (DA) improves win rate but reduces total P&L**
   - DA: 73.6% win rate, $37 P&L (fewer trades)
   - noDA: 57.4% win rate, $48 P&L (more trades)

3. **Even misaligned trades are marginally profitable**
   - DOWN bets when delta > 0: 42.9% win rate, $10.73 total
   - UP bets when delta < 0: 30.0% win rate, $0.96 total

### Recommendation

- **Disable SELL strategy** - HOLD generates 2-3x more P&L
- **Use Delta Alignment for conservative approach** - Higher win rate, less variance
- **Disable Delta Alignment for aggressive approach** - More trades, higher total P&L

---

## Advanced Techniques for Better Edge {#advanced-techniques}

### 1. Brownian Bridge Approach

For conditional probability of barrier crossing:

```
P(max > barrier | start=a, end=b) = exp(-2(barrier-a)(barrier-b) / σ²T)
```

**Implementation:**
```rust
fn brownian_bridge_p_up(current_delta: f64, time_remaining: f64, sigma: f64) -> f64 {
    let std_dev = sigma * (time_remaining / 900.0).sqrt();
    normal_cdf(current_delta / std_dev)
}
```

**Source:** [Pricing Barrier Options with Brownian Bridge](https://www.theissaclee.com/project/pricingbarrierwithbridge/)

### 2. Add Velocity Dimension to Matrix

Instead of (time, delta), use **(time, delta, velocity_sign)**:

```rust
pub struct EnhancedCellStats {
    pub time_bucket: u8,           // 0-59
    pub delta_bucket: i8,          // -17 to +16
    pub velocity_sign: i8,         // -1, 0, +1 (recent direction)

    pub count_up: u32,
    pub count_down: u32,
}
```

This separates:
- "Delta +$80, just jumped up" → momentum, likely stays UP
- "Delta +$80, falling from +$150" → reversal, might go DOWN

**SQL to calculate velocity:**
```sql
SELECT
    time_elapsed,
    price_delta,
    price_delta - LAG(price_delta, 20) OVER (
        PARTITION BY market_slug ORDER BY time_elapsed
    ) as velocity
FROM market_logs
```

### 3. Ornstein-Uhlenbeck with Momentum

Standard OU models mean reversion:
```
dX = θ(μ - X)dt + σdW
```

Modified for momentum:
```
dX = (θ_momentum * velocity + θ_revert * (μ - X))dt + σdW
```

**Source:** [OU Process Simulation](https://www.quantstart.com/articles/ornstein-uhlenbeck-simulation-with-python/)

### 4. Regime-Switching Model

BTC alternates between:
1. **Trending regime** (high momentum persistence)
2. **Mean-reverting regime** (reversals likely)

**Detection:**
```rust
fn detect_regime(volatility_1m: f64, volatility_5m: f64) -> Regime {
    let vol_ratio = volatility_1m / volatility_5m;
    if vol_ratio > 1.5 {
        Regime::Trending  // Recent volatility spike = momentum
    } else {
        Regime::MeanReverting
    }
}
```

**Source:** [Regime-Switching Options](https://link.springer.com/article/10.1007/s11147-017-9139-1)

### 5. Path-Dependent Features

Add more features beyond just (time, delta):

| Feature | Description |
|---------|-------------|
| `delta` | Current price change |
| `velocity` | Rate of change (delta_now - delta_10s_ago) |
| `acceleration` | Change in velocity |
| `max_delta` | Maximum delta reached so far |
| `min_delta` | Minimum delta reached so far |
| `crossings` | Number of times crossed 0 |

**Source:** [Digital Barrier Options Research](https://link.springer.com/article/10.1007/s40096-016-0179-8)

### 6. Formula-Based (No Matrix Needed)

Using Black-Scholes for digital options:

```rust
fn digital_option_prob(delta: f64, time_remaining_secs: f64, sigma: f64) -> f64 {
    let t = time_remaining_secs / 900.0;
    let sigma_t = sigma * t.sqrt();
    let d2 = delta / (100000.0 * sigma_t);  // BTC ~$100k
    normal_cdf(d2)
}
```

**Source:** [Option Pricing Theory](https://pages.stern.nyu.edu/~adamodar/pdfiles/valn2ed/ch5.pdf)

---

## Recommendations {#recommendations}

### Immediate (Implemented)

1. ✅ **Delta Alignment Filter** - Only bet WITH momentum
   - `require_delta_alignment: true` in config
   - Improves win rate from ~50% to ~60-75%

2. ✅ **Disable SELL Strategy** - Hold to expiration
   - HOLD generates 2-3x more P&L than SELL

### Short-Term Improvements

3. **Add Velocity to Matrix**
   - Third dimension: velocity_sign (-1, 0, +1)
   - Separates momentum moves from reversals
   - Expected improvement: +10-15% win rate

4. **Use Formula-Based Probability**
   - Brownian Bridge or Black-Scholes digital
   - No training data needed
   - Theoretically correct for GBM assumption

### Long-Term Improvements

5. **Regime Detection**
   - Identify trending vs mean-reverting periods
   - Use different strategies per regime

6. **Machine Learning**
   - Train on path features, not just state
   - XGBoost or neural network for P(UP)

---

## Appendix: Key Queries

### Check Velocity Impact on Win Rate

```sql
WITH velocity_data AS (
    SELECT
        market_slug,
        time_elapsed,
        price_delta,
        price_delta - LAG(price_delta, 10) OVER w as velocity,
        edge_up, edge_down,
        CASE WHEN edge_up > edge_down THEN 'UP' ELSE 'DOWN' END as signal_dir
    FROM market_logs
    WHERE timestamp > NOW() - INTERVAL '7 days'
    WINDOW w AS (PARTITION BY market_slug ORDER BY time_elapsed)
),
with_outcome AS (
    SELECT v.*,
        (SELECT CASE WHEN m.price_delta > 0 THEN 'UP' ELSE 'DOWN' END
         FROM market_logs m
         WHERE m.market_slug = v.market_slug AND m.time_elapsed >= 850
         LIMIT 1) as outcome
    FROM velocity_data v
    WHERE (edge_up >= 0.07 OR edge_down >= 0.07)
)
SELECT
    CASE WHEN velocity > 10 THEN 'UP momentum'
         WHEN velocity < -10 THEN 'DOWN momentum'
         ELSE 'neutral' END as velocity_regime,
    signal_dir,
    COUNT(*) as trades,
    ROUND(100.0 * COUNT(CASE WHEN signal_dir = outcome THEN 1 END) / COUNT(*)::numeric, 1) as win_rate
FROM with_outcome
WHERE outcome IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Analyze Edge Persistence

```sql
WITH snapshots_with_later AS (
    SELECT
        market_slug, time_elapsed, edge_up, edge_down, price_delta,
        LEAD(edge_up, 20) OVER w as edge_up_after,
        LEAD(edge_down, 20) OVER w as edge_down_after
    FROM market_logs
    WHERE timestamp > NOW() - INTERVAL '24 hours'
    WINDOW w AS (PARTITION BY market_slug ORDER BY time_elapsed)
)
SELECT
    CASE
        WHEN GREATEST(edge_up, edge_down) >= 0.30 THEN '30%+'
        WHEN GREATEST(edge_up, edge_down) >= 0.20 THEN '20-30%'
        ELSE '15-20%'
    END as edge_bucket,
    COUNT(*) as signals,
    ROUND(AVG(GREATEST(edge_up, edge_down))::numeric, 3) as edge_at_signal,
    ROUND(AVG(CASE WHEN edge_up > edge_down THEN edge_up_after ELSE edge_down_after END)::numeric, 3) as edge_after
FROM snapshots_with_later
WHERE GREATEST(edge_up, edge_down) >= 0.15
  AND edge_up_after IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;
```

---

## References

1. [Pricing Barrier Options with Brownian Bridge MC Simulation](https://www.theissaclee.com/project/pricingbarrierwithbridge/)
2. [Digital Barrier Options Pricing: An Improved Monte Carlo Algorithm](https://link.springer.com/article/10.1007/s40096-016-0179-8)
3. [Ornstein-Uhlenbeck Simulation with Python](https://www.quantstart.com/articles/ornstein-uhlenbeck-simulation-with-python/)
4. [Option Pricing Theory and Models (NYU Stern)](https://pages.stern.nyu.edu/~adamodar/pdfiles/valn2ed/ch5.pdf)
5. [Pricing Exotic Options in a Regime Switching Economy](https://link.springer.com/article/10.1007/s11147-017-9139-1)
6. [Imperial College - Correctly Pricing Continuous Barrier Contracts](https://www.imperial.ac.uk/media/imperial-college/faculty-of-natural-sciences/department-of-mathematics/math-finance/212251550---Felix-Eychenne---EYCHENNE_FELIX_02299994.pdf)

---

*Document generated: December 2024*
*Based on analysis of 48 hours of market_logs data*
