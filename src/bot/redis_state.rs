//! Redis-based shared state for multi-pod coordination
//!
//! This module handles:
//! - Shared position tracking across pods
//! - Bet counters per window
//! - Distributed locking to prevent duplicate orders

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Redis URL - hardcoded for simplicity
const REDIS_URL: &str = "rediss://:4NJI916gkYUu3Dhh0osLDBHaMIzNUu-6@zd5119e64-redis.z216d71b1.prm.sh:6379";

/// Key prefixes
const POSITIONS_KEY: &str = "btc_bot:positions";
const BET_COUNTER_PREFIX: &str = "btc_bot:bets:";
const TRADE_LOCK_KEY: &str = "btc_bot:trade_lock";
const LAST_BET_TIME_PREFIX: &str = "btc_bot:last_bet:";

/// Position stored in Redis (serializable version)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisPosition {
    pub position_id: String,
    pub token_id: String,
    pub direction: String,      // "Up" or "Down"
    pub entry_price: f64,
    pub shares: f64,
    pub entry_time_bucket: u8,
    pub entry_delta_bucket: i8,
    pub exit_target: f64,
    pub window_start_ts: i64,   // Unix timestamp
    pub sell_pending: bool,
    pub strategy_type: String,
    pub entry_seconds_elapsed: u32,
}

/// Redis shared state manager
pub struct RedisState {
    client: redis::Client,
}

impl RedisState {
    /// Connect to Redis with timeout
    pub async fn connect() -> Result<Self> {
        use std::time::Duration;
        use tokio::time::timeout;

        eprintln!("[redis] Creating client for: {}", &REDIS_URL[..50]);
        let client = redis::Client::open(REDIS_URL)
            .context("Failed to create Redis client")?;

        // Test connection with 10s timeout
        eprintln!("[redis] Getting connection (10s timeout)...");
        let mut conn = match timeout(Duration::from_secs(10), client.get_multiplexed_async_connection()).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                eprintln!("[redis] Connection error: {}", e);
                anyhow::bail!("Redis connection failed: {}", e);
            }
            Err(_) => {
                eprintln!("[redis] Connection TIMEOUT after 10s");
                anyhow::bail!("Redis connection timeout after 10s");
            }
        };

        eprintln!("[redis] Sending PING...");
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Redis PING failed")?;

        eprintln!("[redis] PING successful!");
        info!("Connected to Redis for shared state");
        Ok(Self { client })
    }

    /// Get a connection
    async fn conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
            .context("Failed to get Redis connection")
    }

    // ========== POSITIONS ==========

    /// Get all open positions
    pub async fn get_positions(&self) -> Result<Vec<RedisPosition>> {
        let mut conn = self.conn().await?;
        let data: Option<String> = conn.get(POSITIONS_KEY).await?;

        match data {
            Some(json) => {
                let positions: Vec<RedisPosition> = serde_json::from_str(&json)?;
                Ok(positions)
            }
            None => Ok(vec![])
        }
    }

    /// Save all positions
    async fn save_positions(&self, positions: &[RedisPosition]) -> Result<()> {
        let mut conn = self.conn().await?;
        let json = serde_json::to_string(positions)?;
        conn.set(POSITIONS_KEY, json).await?;
        Ok(())
    }

    /// Add a new position
    pub async fn add_position(&self, position: RedisPosition) -> Result<()> {
        let mut positions = self.get_positions().await?;
        positions.push(position.clone());
        self.save_positions(&positions).await?;
        info!("Redis: Added position {} ({} {})",
            &position.position_id[..8], position.direction, position.shares);
        Ok(())
    }

    /// Remove a position by ID
    pub async fn remove_position(&self, position_id: &str) -> Result<bool> {
        let mut positions = self.get_positions().await?;
        let initial_len = positions.len();
        positions.retain(|p| p.position_id != position_id);

        if positions.len() < initial_len {
            self.save_positions(&positions).await?;
            info!("Redis: Removed position {}", &position_id[..8]);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark position as sell pending
    pub async fn mark_sell_pending(&self, position_id: &str) -> Result<()> {
        let mut positions = self.get_positions().await?;
        for p in &mut positions {
            if p.position_id == position_id {
                p.sell_pending = true;
            }
        }
        self.save_positions(&positions).await?;
        Ok(())
    }

    /// Get position count
    pub async fn position_count(&self) -> Result<usize> {
        let positions = self.get_positions().await?;
        Ok(positions.len())
    }

    /// Check if any positions have pending sells
    pub async fn has_pending_sells(&self) -> Result<bool> {
        let positions = self.get_positions().await?;
        Ok(positions.iter().any(|p| p.sell_pending))
    }

    /// Clear positions for a new window
    pub async fn clear_positions_for_window(&self, window_start_ts: i64) -> Result<()> {
        let mut positions = self.get_positions().await?;
        let initial_len = positions.len();

        // Keep only positions from the current window
        positions.retain(|p| p.window_start_ts == window_start_ts);

        if positions.len() < initial_len {
            self.save_positions(&positions).await?;
            info!("Redis: Cleared {} old positions", initial_len - positions.len());
        }
        Ok(())
    }

    // ========== BET COUNTERS ==========

    /// Get bet count for a window and strategy
    pub async fn get_bet_count(&self, window_start_ts: i64, strategy: &str) -> Result<u32> {
        let mut conn = self.conn().await?;
        let key = format!("{}{}:{}", BET_COUNTER_PREFIX, window_start_ts, strategy);
        let count: Option<u32> = conn.get(&key).await?;
        Ok(count.unwrap_or(0))
    }

    /// Increment bet count for a window and strategy
    pub async fn increment_bet_count(&self, window_start_ts: i64, strategy: &str) -> Result<u32> {
        let mut conn = self.conn().await?;
        let key = format!("{}{}:{}", BET_COUNTER_PREFIX, window_start_ts, strategy);
        let count: u32 = conn.incr(&key, 1).await?;
        // Set expiry to 20 minutes (longer than a window)
        let _: () = conn.expire(&key, 1200).await?;
        debug!("Redis: {} bets for {} in window {}", count, strategy, window_start_ts);
        Ok(count)
    }

    // ========== COOLDOWN ==========

    /// Record when a bet was placed for cooldown tracking
    pub async fn record_bet_time(&self, strategy: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let key = format!("{}{}", LAST_BET_TIME_PREFIX, strategy);
        let now = Utc::now().timestamp();
        conn.set(&key, now).await?;
        let _: () = conn.expire(&key, 1200).await?;
        Ok(())
    }

    /// Get seconds since last bet for a strategy
    pub async fn seconds_since_last_bet(&self, strategy: &str) -> Result<Option<u64>> {
        let mut conn = self.conn().await?;
        let key = format!("{}{}", LAST_BET_TIME_PREFIX, strategy);
        let last_bet: Option<i64> = conn.get(&key).await?;

        match last_bet {
            Some(ts) => {
                let now = Utc::now().timestamp();
                Ok(Some((now - ts) as u64))
            }
            None => Ok(None)
        }
    }

    // ========== TRADE LOCK ==========

    /// Try to acquire trade lock (prevents duplicate orders across pods)
    /// Returns true if lock acquired, false if another pod has it
    pub async fn try_acquire_trade_lock(&self, lock_id: &str, ttl_ms: u64) -> Result<bool> {
        let mut conn = self.conn().await?;

        // Use SET NX EX pattern for atomic lock
        let result: Option<String> = redis::cmd("SET")
            .arg(TRADE_LOCK_KEY)
            .arg(lock_id)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut conn)
            .await?;

        Ok(result.is_some())
    }

    /// Release trade lock (only if we own it)
    pub async fn release_trade_lock(&self, lock_id: &str) -> Result<()> {
        let mut conn = self.conn().await?;

        // Only delete if we own the lock
        let current: Option<String> = conn.get(TRADE_LOCK_KEY).await?;
        if current.as_deref() == Some(lock_id) {
            let _: () = conn.del(TRADE_LOCK_KEY).await?;
        }
        Ok(())
    }

    /// Check if trade lock is held
    pub async fn is_trade_locked(&self) -> Result<bool> {
        let mut conn = self.conn().await?;
        let exists: bool = conn.exists(TRADE_LOCK_KEY).await?;
        Ok(exists)
    }
}
