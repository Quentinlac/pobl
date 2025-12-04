# Polymarket Bitcoin Price Predictor

## Overview

A Rust application that analyzes historical Binance BTC prices to calculate the probability of price going UP or DOWN at the end of 15-minute Polymarket betting windows. The goal is to detect mispriced markets by comparing historical probabilities to Polymarket odds.

## Core Concept

- **Markets**: Polymarket runs 15-min BTC price prediction markets (starts at 8:00, 8:15, 8:30, etc.)
- **Goal**: For any point within a 15-min window, calculate P(UP) and P(DOWN) based on:
  1. Time elapsed since window start (bucketed into 30s intervals)
  2. Current price delta from window open price (bucketed into $ ranges)
- **Edge Detection**: Compare historical probability to Polymarket implied probability to find mispriced bets

## Probability Matrix Structure

### Time Buckets (30 intervals)
| Bucket | Time Range |
|--------|------------|
| 0 | 0:00 - 0:30 |
| 1 | 0:30 - 1:00 |
| ... | ... |
| 29 | 14:30 - 15:00 |

### Price Delta Buckets (13 intervals)
| Bucket | Price Delta Range |
|--------|-------------------|
| -6 | < -$300 |
| -5 | -$300 to -$200 |
| -4 | -$200 to -$100 |
| -3 | -$100 to -$50 |
| -2 | -$50 to -$20 |
| -1 | -$20 to $0 |
| 0 | $0 to +$20 |
| +1 | +$20 to +$50 |
| +2 | +$50 to +$100 |
| +3 | +$100 to +$200 |
| +4 | +$200 to +$300 |
| +5 | > +$300 |

**Total Cells**: 30 × 13 = 390 cells

---

## Statistical Methods

### 1. Wilson Score Confidence Interval
Best for binomial proportions, works well with small samples (n ≥ 10).

```
Wilson CI = (p̂ + z²/2n ± z√[p̂(1-p̂)/n + z²/4n²]) / (1 + z²/n)
```

### 2. Bayesian Beta-Binomial
For cells with sparse data. Prior: Beta(α₀, β₀), Posterior: Beta(α₀ + wins, β₀ + losses)

### 3. Confidence Levels
| Samples (n) | Level | Action |
|-------------|-------|--------|
| n < 10 | Unreliable | Don't bet |
| 10 ≤ n < 30 | Weak | Small bets only |
| 30 ≤ n < 100 | Moderate | Standard betting |
| n ≥ 100 | Strong | High confidence |

### 4. Kelly Criterion (Fractional)
```
kelly_fraction = (p_win * odds - p_lose) / odds
bet_size = bankroll * kelly_fraction * 0.25  // quarter Kelly
```

---

## Database

- **Connection**: `postgresql://qoveryadmin:***@zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com:5432`
- **Database**: `polymarket`
- **Table**: `binance_prices`

### Schema
```sql
binance_prices (
  symbol         VARCHAR(20)     -- e.g., 'BTCUSDT'
  timestamp      TIMESTAMPTZ     -- per-second granularity
  open_price     NUMERIC(20,8)   -- ~$95,000 range
  high_price     NUMERIC(20,8)
  low_price      NUMERIC(20,8)
  close_price    NUMERIC(20,8)
  volume         NUMERIC(30,8)
  quote_volume   NUMERIC(30,8)
  num_trades     BIGINT
)
PRIMARY KEY: (symbol, timestamp)
```

### Data Coverage
- **Range**: 2025-11-16 15:16:20 → 2025-12-04 13:52:42 (18+ days)
- **Records**: 1,550,183 rows
- **Granularity**: Exactly 1 record per second (86,400/day)
- **Gaps**: None - complete coverage from Nov 17 onwards
- **15-min windows available**: ~1,700+ windows

---

## Phases

### Phase 1: Database Exploration
- [x] Connect to PostgreSQL
- [x] Analyze `binance_prices` schema and format
- [x] Determine data granularity and date range
- [x] Document findings

**Status**: ✅ Complete

**Notes**:
```
- Table has per-second OHLCV candles for BTCUSDT
- 1,550,183 records from Nov 16 - Dec 4, 2025
- No gaps in data (exactly 86,400 records/day)
- Will use close_price for our analysis
- ~1,700+ 15-minute windows available for training
```

---

### Phase 2: Data Model Design
- [x] Define `PricePoint` struct
- [x] Define `FifteenMinWindow` struct
- [x] Define `TimeBucket` enum/type
- [x] Define `PriceDeltaBucket` enum/type
- [x] Define `CellStats` struct with Wilson CI fields
- [x] Define `BetRecommendation` struct

**Status**: ✅ Complete

**Notes**:
```
- All structs defined in src/models.rs
- 13 price delta buckets (-6 to +5) with clear $ ranges
- 30 time buckets (30s intervals)
- CellStats includes Wilson CI bounds and Bayesian posteriors
```

---

### Phase 3: Historical Data Processing
- [x] Parse raw `binance_prices` data
- [x] Group data into 15-minute windows
- [x] Track price at every 30s interval within windows
- [x] Calculate delta from open price
- [x] Determine final outcome (UP/DOWN) for each window

**Status**: ✅ Complete

**Notes**:
```
- Implemented in src/processor.rs
- Windows aligned to :00, :15, :30, :45
- 30 snapshots per window (one per 30s bucket)
- Outcome determined by close_price >= open_price
```

---

### Phase 4: Build Probability Matrix
- [x] Initialize 30 × 13 matrix
- [x] Iterate through all historical windows
- [x] For each 30s snapshot, increment appropriate cell
- [x] Calculate raw probabilities (count_up / total)

**Status**: ✅ Complete

**Notes**:
```
- ProbabilityMatrix struct in src/models.rs
- populate_matrix() in src/processor.rs
- 390 total cells (30 time × 13 price delta)
```

---

### Phase 5: Statistical Significance
- [x] Implement Wilson Score CI function
- [x] Implement Bayesian Beta-Binomial posterior
- [x] Assign confidence levels to each cell
- [x] Flag cells with insufficient data
- [x] Add lower/upper bounds to CellStats

**Status**: ✅ Complete

**Notes**:
```
- src/stats.rs: wilson_score_interval(), beta_posterior()
- 4 confidence levels: Unreliable (<10), Weak (10-30), Moderate (30-100), Strong (100+)
- 95% Wilson CI for conservative probability bounds
```

---

### Phase 6: Edge Detection
- [x] Implement market implied probability conversion
- [x] Compare historical P(UP) to market implied
- [x] Calculate edge percentage
- [x] Implement threshold logic (e.g., edge > 5%)

**Status**: ✅ Complete

**Notes**:
```
- src/edge.rs: get_recommendation(), calculate_edge()
- Uses Wilson lower bound for conservative edge calculation
- 5% min edge for Moderate/Strong, 15% for Weak confidence
```

---

### Phase 7: Kelly Bet Sizing
- [x] Implement Kelly formula
- [x] Apply fractional Kelly (0.25x recommended)
- [x] Scale by confidence level
- [x] Cap maximum bet size

**Status**: ✅ Complete

**Notes**:
```
- src/edge.rs: calculate_kelly_fraction()
- Fractional Kelly: 10% (Weak), 25% (Moderate), 50% (Strong)
- Capped at 10% of bankroll maximum
```

---

### Phase 8: Output & Reporting
- [x] Export matrix as JSON
- [x] Export matrix as CSV
- [x] Generate summary statistics
- [x] Create human-readable report

**Status**: ✅ Complete

**Notes**:
```
- src/output.rs: export_to_json(), export_to_csv(), generate_report()
- Matrix visualization with confidence indicators
- Identifies most biased cells for UP/DOWN
```

---

### Phase 9: Bot Integration API
- [x] Create `get_recommendation()` function
- [x] Input: time_elapsed, price_delta, market_price, bankroll
- [x] Output: should_bet, direction, edge, confidence, bet_size
- [x] Add real-time price delta bucket lookup

**Status**: ✅ Complete

**Notes**:
```
- CLI with 'build', 'query', and 'stats' commands
- Query command: cargo run -- query -t 420 -p 34.0 -m 0.45 -b 1000
- Returns full recommendation with EV calculation
```

---

### Phase 10: Testing & Validation
- [ ] Split data into train/test sets
- [ ] Backtest on held-out data
- [ ] Validate Wilson CI coverage
- [ ] Simulate P&L with Kelly sizing
- [ ] Check for overfitting

**Status**: 🔲 Pending

**Notes**:
```
- Run 'cargo run -- build' first to generate matrix
- Then validate against held-out data
```

---

## File Structure

```
bot/
├── PROJECT_PLAN.md          # This file
├── Cargo.toml               # Rust dependencies
├── Dockerfile               # Production container (Qovery-ready)
├── docker-compose.yml       # Local development setup
├── .env.example             # Environment variables template
├── config/
│   └── bot_config.yaml      # Bot settings (aggressiveness, risk, timing)
├── src/
│   ├── main.rs              # CLI entry point (build, query, stats commands)
│   ├── db.rs                # Database connection & queries
│   ├── models.rs            # Data structures (CellStats, ProbabilityMatrix, etc.)
│   ├── processor.rs         # Parse prices, build windows, populate matrix
│   ├── stats.rs             # Wilson CI, Bayesian Beta-Binomial
│   ├── edge.rs              # Edge detection + Kelly criterion
│   ├── output.rs            # JSON/CSV export, reports
│   └── bot/                 # Live trading bot
│       ├── main.rs          # Bot entry point (0.5s polling loop)
│       ├── mod.rs           # Module declarations
│       ├── config.rs        # Config file loader
│       ├── binance.rs       # Binance price API client
│       ├── polymarket.rs    # Polymarket CLOB API client
│       └── strategy.rs      # Decision making logic
└── output/
    ├── matrix.json          # Full matrix with all stats
    ├── matrix.csv           # Spreadsheet-friendly format
    └── report.txt           # Human-readable summary
```

---

## References

- [Wilson Score Interval - Wikipedia](https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval)
- [Kelly Criterion - Wikipedia](https://en.wikipedia.org/wiki/Kelly_criterion)
- [Beta-Binomial Model - Bayes Rules](https://www.bayesrulesbook.com/chapter-3)
- [Statistical Power in Backtesting - QuestDB](https://questdb.com/glossary/statistical-power-analysis-in-backtesting-models/)

---

## Changelog

| Date | Phase | Changes |
|------|-------|---------|
| 2025-12-04 | Setup | Created project plan |
| 2025-12-04 | Phase 1 | ✅ Database exploration complete - found 1.5M rows, 1-second granularity, no gaps |
| 2025-12-04 | Phases 2-9 | ✅ Full implementation complete - Rust app builds successfully |
| 2025-12-04 | Bot | ✅ Live trading bot with 0.5s polling, config system, Dockerfile |

## Usage

### Matrix Builder (CLI)
```bash
# Build the probability matrix from historical data
cargo run -- build

# Query a specific situation
cargo run -- query -t 420 -p 34.0 -m 0.45 -b 1000
# -t: time elapsed in seconds (0-899)
# -p: price delta from window open ($)
# -m: Polymarket price for UP (0.01-0.99)
# -b: your bankroll ($)

# Show statistics from saved matrix
cargo run -- stats
```

### Live Trading Bot
```bash
# Copy environment template and fill in your values
cp .env.example .env

# Run the bot locally
cargo run --bin btc-bot

# Or with Docker
docker build -t btc-bot .
docker run --env-file .env btc-bot

# Or with Docker Compose
docker-compose up -d
docker-compose logs -f
```

### Configuration
Edit `config/bot_config.yaml` to adjust:
- **Polling**: interval_ms (500ms default)
- **Edge thresholds**: min_edge_strong (5%), min_edge_moderate (7%), min_edge_weak (15%)
- **Bet sizing**: kelly_fraction (25%), max_bet_pct (10%), max_bet_usdc ($100)
- **Timing**: min_seconds_elapsed (60), min_seconds_remaining (15)
- **Risk**: max_bets_per_window (1), daily_loss_limit_pct (10%)

See comments in the config file for CONSERVATIVE / MODERATE / AGGRESSIVE presets.

