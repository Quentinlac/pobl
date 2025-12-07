use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Bot configuration loaded from YAML file
#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    pub polling: PollingConfig,
    pub edge: EdgeConfig,
    pub betting: BettingConfig,
    pub timing: TimingConfig,
    pub price_filters: PriceFilterConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    pub markets: MarketsConfig,
    #[serde(default)]
    pub cooldown: CooldownConfig,
    #[serde(default)]
    pub terminal_strategy: TerminalStrategyConfig,
    #[serde(default)]
    pub exit_strategy: ExitStrategyConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PollingConfig {
    /// How often to fetch order book (ms)
    pub interval_ms: u64,
    /// How often to fetch BTC price (ms)
    pub price_fetch_interval_ms: u64,
    /// API request timeout (ms)
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeConfig {
    /// Min edge for strong confidence (n >= 100)
    pub min_edge_strong: f64,
    /// Min edge for moderate confidence (30 <= n < 100)
    pub min_edge_moderate: f64,
    /// Min edge for weak confidence (10 <= n < 30)
    pub min_edge_weak: f64,
    /// Allow betting on unreliable cells
    pub allow_unreliable: bool,
    /// Min edge for unreliable cells
    pub min_edge_unreliable: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BettingConfig {
    /// Fraction of Kelly criterion to use
    pub kelly_fraction: f64,
    /// Multipliers per confidence level
    pub confidence_multipliers: ConfidenceMultipliers,
    /// Max bet as % of bankroll
    pub max_bet_pct: f64,
    /// Minimum bet in USDC
    pub min_bet_usdc: f64,
    /// Maximum bet in USDC (absolute cap)
    pub max_bet_usdc: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfidenceMultipliers {
    pub strong: f64,
    pub moderate: f64,
    pub weak: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimingConfig {
    /// Don't bet before this many seconds into window
    pub min_seconds_elapsed: u32,
    /// Don't bet after this many seconds remaining
    pub min_seconds_remaining: u32,
    /// Minimum samples required in the time bucket
    pub min_samples_in_bucket: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PriceFilterConfig {
    /// Skip if price delta exceeds this
    pub max_price_delta: f64,
    /// Skip if price change % exceeds this
    pub max_price_change_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    /// Order type: "market" or "limit"
    pub order_type: String,
    /// For limit orders: offset from best price (cents)
    pub limit_price_offset_cents: f64,
    /// Max slippage for market orders
    pub max_slippage_pct: f64,
    /// Retry failed orders
    pub retry_on_failure: bool,
    /// Max retries
    pub max_retries: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    /// Max bets per 15-min window
    pub max_bets_per_window: u32,
    /// Stop trading if daily loss exceeds this %
    pub daily_loss_limit_pct: f64,
    /// Reduce bet by this factor after each loss
    pub loss_reduction_factor: f64,
    /// Wins needed to reset loss reduction
    pub consecutive_wins_to_reset: u32,
    /// Max open positions at once
    pub max_open_positions: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketsConfig {
    /// Token ID for BTC UP outcome (overridden by env)
    pub btc_up_token_id: String,
    /// Token ID for BTC DOWN outcome (overridden by env)
    pub btc_down_token_id: String,
    /// Condition ID for the market
    pub condition_id: String,
    /// Minimum liquidity required (USDC)
    pub min_liquidity_usdc: f64,
    /// Maximum spread to accept
    pub max_spread_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CooldownConfig {
    /// Minimum seconds between bet attempts
    #[serde(default = "default_cooldown_seconds")]
    pub min_seconds_between_bets: u32,
    /// Seconds between detailed log entries (to reduce spam)
    #[serde(default = "default_log_cooldown")]
    pub log_cooldown_seconds: u32,
}

fn default_cooldown_seconds() -> u32 { 30 }
fn default_log_cooldown() -> u32 { 5 }

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            min_seconds_between_bets: 0,  // No cooldown by default (buy every 500ms if edge)
            log_cooldown_seconds: 5,
        }
    }
}

/// Terminal strategy config (original strategy - hold to settlement)
#[derive(Debug, Clone, Deserialize)]
pub struct TerminalStrategyConfig {
    /// Enable terminal strategy
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum BUY edge for terminal strategy (our_prob - ask) / ask
    #[serde(default = "default_terminal_min_edge")]
    pub min_edge: f64,
    /// Minimum SELL edge for terminal strategy (bid - our_prob) / bid
    /// Positive = market paying more than we think token is worth
    #[serde(default = "default_terminal_min_sell_edge")]
    pub min_sell_edge: f64,
    /// Minimum profit percentage before allowing sell (e.g., 0.05 = 5%)
    /// Only sell if (current_bid - entry_price) / entry_price >= this value
    #[serde(default = "default_terminal_min_profit_before_sell")]
    pub min_profit_before_sell: f64,
    /// Max bet in USDC for terminal strategy
    #[serde(default = "default_terminal_max_bet")]
    pub max_bet_usdc: f64,
    /// Max bets per window for terminal strategy
    #[serde(default = "default_terminal_max_bets")]
    pub max_bets_per_window: u32,
    /// Don't buy in last N seconds (terminal)
    #[serde(default = "default_terminal_min_remaining")]
    pub min_seconds_remaining: u32,
    /// Cooldown between terminal bets (seconds)
    #[serde(default = "default_terminal_cooldown")]
    pub cooldown_seconds: u32,
    /// Take-profit settings for terminal positions
    #[serde(default)]
    pub take_profit: TakeProfitConfig,
}

/// Time-based take-profit configuration for terminal strategy
#[derive(Debug, Clone, Deserialize)]
pub struct TakeProfitConfig {
    /// Enable take-profit for terminal positions
    #[serde(default)]
    pub enabled: bool,
    /// Take-profit targets based on time remaining
    #[serde(default = "default_take_profit_targets")]
    pub targets: Vec<TakeProfitTarget>,
}

/// A single take-profit target
#[derive(Debug, Clone, Deserialize)]
pub struct TakeProfitTarget {
    /// Maximum seconds elapsed in window for this target to apply
    /// e.g., 300 means this target applies when elapsed < 300s (first 5 min)
    pub max_seconds: u32,
    /// Profit percentage required to trigger take-profit (e.g., 0.50 = +50%)
    pub profit_pct: f64,
}

fn default_take_profit_targets() -> Vec<TakeProfitTarget> {
    vec![
        TakeProfitTarget { max_seconds: 300, profit_pct: 0.50 },  // First 5 min: +50%
        TakeProfitTarget { max_seconds: 600, profit_pct: 0.30 },  // 5-10 min: +30%
        TakeProfitTarget { max_seconds: 900, profit_pct: 0.15 },  // 10-15 min: +15%
    ]
}

impl Default for TakeProfitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            targets: default_take_profit_targets(),
        }
    }
}

impl TakeProfitConfig {
    /// Get the profit target for a given time elapsed in the window
    /// Returns None if take-profit is disabled or no target applies
    pub fn get_target_for_time(&self, seconds_elapsed: u32) -> Option<f64> {
        if !self.enabled {
            return None;
        }

        // Targets should be sorted by max_seconds ascending
        // Find the first target where seconds_elapsed < max_seconds
        for target in &self.targets {
            if seconds_elapsed < target.max_seconds {
                return Some(target.profit_pct);
            }
        }

        // If past all targets, use the last one (most lenient)
        self.targets.last().map(|t| t.profit_pct)
    }
}

fn default_terminal_min_edge() -> f64 { 0.10 }              // 10% buy edge required
fn default_terminal_min_sell_edge() -> f64 { 0.10 }         // 10% sell edge required
fn default_terminal_min_profit_before_sell() -> f64 { 0.05 } // 5% profit required before selling
fn default_terminal_max_bet() -> f64 { 30.0 }               // $30 max
fn default_terminal_max_bets() -> u32 { 1 }                 // 1 per window
fn default_terminal_min_remaining() -> u32 { 15 }           // Can buy until last 15s
fn default_terminal_cooldown() -> u32 { 30 }                // 30s between terminal bets

impl Default for TerminalStrategyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_edge: 0.10,
            min_sell_edge: 0.10,
            min_profit_before_sell: 0.05,  // 5% profit required before selling
            max_bet_usdc: 30.0,
            max_bets_per_window: 1,
            min_seconds_remaining: 15,
            cooldown_seconds: 30,
            take_profit: TakeProfitConfig::default(),
        }
    }
}

/// Exit strategy config (new strategy - sell at target)
#[derive(Debug, Clone, Deserialize)]
pub struct ExitStrategyConfig {
    /// Enable exit strategy (sell before settlement)
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum EV return for exit strategy
    #[serde(default = "default_exit_min_edge")]
    pub min_edge: f64,
    /// Max bet in USDC for exit strategy
    #[serde(default = "default_exit_max_bet")]
    pub max_bet_usdc: f64,
    /// Max bets per window for exit strategy
    #[serde(default = "default_exit_max_bets")]
    pub max_bets_per_window: u32,
    /// Don't buy in last N seconds (exit strategy needs time for targets)
    #[serde(default = "default_exit_min_remaining")]
    pub min_seconds_remaining: u32,
    /// Only use Strong confidence cells from first-passage matrix
    #[serde(default = "default_true")]
    pub only_strong_confidence: bool,
    /// Cooldown between exit strategy bets (seconds)
    #[serde(default = "default_exit_cooldown")]
    pub cooldown_seconds: u32,
}

fn default_exit_min_edge() -> f64 { 0.05 }          // 5% edge required
fn default_exit_max_bet() -> f64 { 5.0 }            // $5 max
fn default_exit_max_bets() -> u32 { 10 }            // 10 per window
fn default_exit_min_remaining() -> u32 { 300 }      // No buy in last 5 min
fn default_exit_cooldown() -> u32 { 10 }            // 10s between exit bets

impl Default for ExitStrategyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_edge: 0.05,
            max_bet_usdc: 5.0,
            max_bets_per_window: 10,
            min_seconds_remaining: 300,
            only_strong_confidence: true,
            cooldown_seconds: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,
    /// Log every price check
    pub log_price_checks: bool,
    /// Log skipped opportunities
    pub log_skipped_opportunities: bool,
    /// Log order book snapshots
    pub log_order_book: bool,
    /// Log decision reasoning
    #[serde(default = "default_true")]
    pub log_decision_reasoning: bool,
}

fn default_true() -> bool { true }

impl BotConfig {
    /// Load configuration from YAML file
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: BotConfig = serde_yaml::from_str(&contents)
            .with_context(|| "Failed to parse YAML configuration")?;

        Ok(config)
    }

    /// Load with environment variable overrides
    pub fn load_with_env(path: &Path) -> Result<Self> {
        let mut config = Self::load(path)?;

        // Override with environment variables
        if let Ok(val) = std::env::var("BOT_POLLING_INTERVAL_MS") {
            config.polling.interval_ms = val.parse().unwrap_or(config.polling.interval_ms);
        }
        if let Ok(val) = std::env::var("BOT_MIN_EDGE_STRONG") {
            config.edge.min_edge_strong = val.parse().unwrap_or(config.edge.min_edge_strong);
        }
        if let Ok(val) = std::env::var("BOT_KELLY_FRACTION") {
            config.betting.kelly_fraction = val.parse().unwrap_or(config.betting.kelly_fraction);
        }
        if let Ok(val) = std::env::var("BOT_MAX_BET_PCT") {
            config.betting.max_bet_pct = val.parse().unwrap_or(config.betting.max_bet_pct);
        }
        if let Ok(val) = std::env::var("BOT_DAILY_LOSS_LIMIT_PCT") {
            config.risk.daily_loss_limit_pct = val.parse().unwrap_or(config.risk.daily_loss_limit_pct);
        }

        // Market token IDs from environment (required)
        if let Ok(val) = std::env::var("POLYMARKET_BTC_UP_TOKEN") {
            config.markets.btc_up_token_id = val;
        }
        if let Ok(val) = std::env::var("POLYMARKET_BTC_DOWN_TOKEN") {
            config.markets.btc_down_token_id = val;
        }
        if let Ok(val) = std::env::var("POLYMARKET_BTC_CONDITION_ID") {
            config.markets.condition_id = val;
        }

        Ok(config)
    }

    /// Get minimum edge for a given confidence level
    pub fn min_edge_for_confidence(&self, confidence: &str) -> Option<f64> {
        match confidence {
            "Strong" => Some(self.edge.min_edge_strong),
            "Moderate" => Some(self.edge.min_edge_moderate),
            "Weak" => Some(self.edge.min_edge_weak),
            "Unreliable" => {
                if self.edge.allow_unreliable {
                    Some(self.edge.min_edge_unreliable)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Get confidence multiplier for bet sizing
    pub fn confidence_multiplier(&self, confidence: &str) -> f64 {
        match confidence {
            "Strong" => self.betting.confidence_multipliers.strong,
            "Moderate" => self.betting.confidence_multipliers.moderate,
            "Weak" => self.betting.confidence_multipliers.weak,
            _ => 0.0,
        }
    }
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            polling: PollingConfig {
                interval_ms: 500,
                price_fetch_interval_ms: 500,
                request_timeout_ms: 2000,
            },
            edge: EdgeConfig {
                min_edge_strong: 0.05,
                min_edge_moderate: 0.07,
                min_edge_weak: 0.15,
                allow_unreliable: false,
                min_edge_unreliable: 0.30,
            },
            betting: BettingConfig {
                kelly_fraction: 0.25,
                confidence_multipliers: ConfidenceMultipliers {
                    strong: 1.0,
                    moderate: 0.6,
                    weak: 0.3,
                },
                max_bet_pct: 0.10,
                min_bet_usdc: 1.0,
                max_bet_usdc: 5.0,  // $5 position size
            },
            timing: TimingConfig {
                min_seconds_elapsed: 60,
                min_seconds_remaining: 300,  // Don't buy in last 5 minutes
                min_samples_in_bucket: 30,
            },
            price_filters: PriceFilterConfig {
                max_price_delta: 500.0,
                max_price_change_pct: 1.0,
            },
            execution: ExecutionConfig {
                order_type: "market".to_string(),
                limit_price_offset_cents: 1.0,
                max_slippage_pct: 0.02,
                retry_on_failure: false,
                max_retries: 1,
            },
            risk: RiskConfig {
                max_bets_per_window: 10,  // Allow up to 10 trades per window
                daily_loss_limit_pct: 0.10,
                loss_reduction_factor: 0.75,
                consecutive_wins_to_reset: 2,
                max_open_positions: 10,  // Match max_bets_per_window
            },
            markets: MarketsConfig {
                btc_up_token_id: String::new(),
                btc_down_token_id: String::new(),
                condition_id: String::new(),
                min_liquidity_usdc: 200.0,
                max_spread_pct: 0.05,
            },
            cooldown: CooldownConfig::default(),
            terminal_strategy: TerminalStrategyConfig::default(),
            exit_strategy: ExitStrategyConfig::default(),
            logging: LoggingConfig {
                level: "info".to_string(),
                log_price_checks: true,
                log_skipped_opportunities: true,
                log_order_book: true,
                log_decision_reasoning: true,
            },
        }
    }
}
