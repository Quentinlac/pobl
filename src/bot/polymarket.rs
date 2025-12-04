use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::debug;

/// Polymarket CLOB API client
pub struct PolymarketClient {
    client: Client,
    clob_url: String,
    gamma_url: String,
}

/// Current BTC 15-minute market info
#[derive(Debug, Clone)]
pub struct Btc15mMarket {
    pub slug: String,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub window_end: DateTime<Utc>,
}

/// Order book response from CLOB API
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBook {
    pub market: String,
    pub asset_id: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBookLevel {
    pub price: String,
    pub size: String,
}

/// Market info from Gamma API
#[derive(Debug, Clone, Deserialize)]
pub struct MarketInfo {
    pub id: String,
    pub condition_id: String,
    pub question: String,
    pub outcomes: Vec<String>,
    pub tokens: Vec<TokenInfo>,
    pub end_date_iso: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenInfo {
    pub token_id: String,
    pub outcome: String,
    pub price: Option<f64>,
}

/// Price quote for a token
#[derive(Debug, Clone)]
pub struct PriceQuote {
    pub token_id: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid_price: f64,
    pub spread: f64,
    pub spread_pct: f64,
    pub bid_liquidity: f64,
    pub ask_liquidity: f64,
}

/// Order to be placed
#[derive(Debug, Clone, Serialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub price: String,
    pub size: String,
    pub side: String, // "BUY" or "SELL"
    pub fee_rate_bps: String,
    pub nonce: String,
    pub signature: String,
    pub owner: String,
    pub order_type: String, // "GTC", "FOK", "GTD"
}

/// Order response
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    pub success: bool,
    pub order_id: Option<String>,
    pub error_msg: Option<String>,
    pub status: Option<String>,
}

impl PolymarketClient {
    pub fn new(timeout_ms: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            clob_url: std::env::var("POLYMARKET_CLOB_URL")
                .unwrap_or_else(|_| "https://clob.polymarket.com".to_string()),
            gamma_url: std::env::var("POLYMARKET_GAMMA_URL")
                .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string()),
        })
    }

    /// Fetch order book for a token
    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBook> {
        let url = format!("{}/book?token_id={}", self.clob_url, token_id);

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch order book")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Order book request failed: {} - {}", status, text));
        }

        let book: OrderBook = response.json().await
            .context("Failed to parse order book response")?;

        Ok(book)
    }

    /// Get price quote from order book
    pub fn get_price_quote(&self, book: &OrderBook) -> Result<PriceQuote> {
        let best_bid = book.bids.first()
            .map(|l| l.price.parse::<f64>().unwrap_or(0.0))
            .unwrap_or(0.0);

        let best_ask = book.asks.first()
            .map(|l| l.price.parse::<f64>().unwrap_or(1.0))
            .unwrap_or(1.0);

        let mid_price = (best_bid + best_ask) / 2.0;
        let spread = best_ask - best_bid;
        let spread_pct = if mid_price > 0.0 { spread / mid_price } else { 1.0 };

        // Calculate total liquidity at top 5 levels
        let bid_liquidity: f64 = book.bids.iter()
            .take(5)
            .map(|l| {
                let price = l.price.parse::<f64>().unwrap_or(0.0);
                let size = l.size.parse::<f64>().unwrap_or(0.0);
                price * size
            })
            .sum();

        let ask_liquidity: f64 = book.asks.iter()
            .take(5)
            .map(|l| {
                let price = l.price.parse::<f64>().unwrap_or(0.0);
                let size = l.size.parse::<f64>().unwrap_or(0.0);
                price * size
            })
            .sum();

        Ok(PriceQuote {
            token_id: book.asset_id.clone(),
            best_bid,
            best_ask,
            mid_price,
            spread,
            spread_pct,
            bid_liquidity,
            ask_liquidity,
        })
    }

    /// Search for active BTC 15-minute markets
    pub async fn find_btc_15min_markets(&self) -> Result<Vec<MarketInfo>> {
        let url = format!(
            "{}/markets?active=true&closed=false&limit=100",
            self.gamma_url
        );

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch markets")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Markets request failed: {} - {}", status, text));
        }

        let markets: Vec<MarketInfo> = response.json().await
            .context("Failed to parse markets response")?;

        // Filter for BTC 15-minute markets
        let btc_markets: Vec<MarketInfo> = markets.into_iter()
            .filter(|m| {
                let q = m.question.to_lowercase();
                (q.contains("btc") || q.contains("bitcoin"))
                    && (q.contains("15") || q.contains("fifteen"))
                    && (q.contains("up") || q.contains("down") || q.contains("higher") || q.contains("lower"))
            })
            .collect();

        Ok(btc_markets)
    }

    /// Get market by condition ID
    pub async fn get_market(&self, condition_id: &str) -> Result<MarketInfo> {
        let url = format!("{}/markets/{}", self.gamma_url, condition_id);

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch market")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Market request failed: {} - {}", status, text));
        }

        let market: MarketInfo = response.json().await
            .context("Failed to parse market response")?;

        Ok(market)
    }

    /// Get both UP and DOWN order books
    pub async fn get_both_books(&self, up_token: &str, down_token: &str) -> Result<(OrderBook, OrderBook)> {
        let (up_book, down_book) = tokio::join!(
            self.get_order_book(up_token),
            self.get_order_book(down_token)
        );

        Ok((up_book?, down_book?))
    }

    /// Get the current BTC 15-minute market (fetches tokens dynamically)
    pub async fn get_current_btc_15m_market(&self) -> Result<Btc15mMarket> {
        // Calculate current window end timestamp
        let now = Utc::now().timestamp();
        let window_end_ts = ((now / 900) + 1) * 900;
        let window_end = DateTime::from_timestamp(window_end_ts, 0)
            .ok_or_else(|| anyhow!("Invalid timestamp"))?;

        // Generate slug
        let slug = format!("btc-updown-15m-{}", window_end_ts);

        debug!("Fetching BTC 15m market: {}", slug);

        // Step 1: Get condition ID from Gamma API (events endpoint)
        let gamma_url = format!("{}/events?slug={}", self.gamma_url, slug);

        let response = self.client
            .get(&gamma_url)
            .send()
            .await
            .context("Failed to fetch event from Gamma API")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Gamma API request failed: {} - {}", status, text));
        }

        #[derive(Deserialize)]
        struct GammaEvent {
            markets: Vec<GammaMarket>,
        }

        #[derive(Deserialize)]
        struct GammaMarket {
            #[serde(rename = "conditionId")]
            condition_id: String,
            // These are JSON-encoded strings, not arrays!
            #[serde(rename = "clobTokenIds")]
            clob_token_ids: Option<String>,
            outcomes: Option<String>,
        }

        let events: Vec<GammaEvent> = response.json().await
            .context("Failed to parse Gamma API response")?;

        if events.is_empty() || events[0].markets.is_empty() {
            return Err(anyhow!("No market found for slug: {}", slug));
        }

        let market = &events[0].markets[0];
        let condition_id = &market.condition_id;

        // Parse the JSON-encoded string arrays
        let (up_token_id, down_token_id) = if let (Some(tokens_str), Some(outcomes_str)) =
            (&market.clob_token_ids, &market.outcomes)
        {
            // Parse JSON strings into vectors
            let tokens: Vec<String> = serde_json::from_str(tokens_str)
                .context("Failed to parse clobTokenIds JSON")?;
            let outcomes: Vec<String> = serde_json::from_str(outcomes_str)
                .context("Failed to parse outcomes JSON")?;

            if tokens.len() >= 2 && outcomes.len() >= 2 {
                // Match tokens to outcomes (Up at index 0, Down at index 1)
                let up_idx = outcomes.iter().position(|o| o.to_lowercase() == "up").unwrap_or(0);
                let down_idx = outcomes.iter().position(|o| o.to_lowercase() == "down").unwrap_or(1);
                (tokens[up_idx].clone(), tokens[down_idx].clone())
            } else {
                return Err(anyhow!("Invalid tokens/outcomes in Gamma response"));
            }
        } else {
            // Fallback: fetch from CLOB API
            let clob_url = format!("{}/markets/{}", self.clob_url, condition_id);

            let response = self.client
                .get(&clob_url)
                .send()
                .await
                .context("Failed to fetch market from CLOB API")?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(anyhow!("CLOB API request failed: {} - {}", status, text));
            }

            #[derive(Deserialize)]
            struct ClobMarket {
                tokens: Vec<ClobToken>,
            }

            #[derive(Deserialize)]
            struct ClobToken {
                token_id: String,
                outcome: String,
            }

            let clob_market: ClobMarket = response.json().await
                .context("Failed to parse CLOB API response")?;

            let up_token = clob_market.tokens.iter()
                .find(|t| t.outcome.to_lowercase() == "yes" || t.outcome.to_lowercase() == "up")
                .ok_or_else(|| anyhow!("UP token not found"))?;

            let down_token = clob_market.tokens.iter()
                .find(|t| t.outcome.to_lowercase() == "no" || t.outcome.to_lowercase() == "down")
                .ok_or_else(|| anyhow!("DOWN token not found"))?;

            (up_token.token_id.clone(), down_token.token_id.clone())
        };

        debug!(
            "Found BTC 15m market: condition={}, up={}, down={}",
            condition_id, up_token_id, down_token_id
        );

        Ok(Btc15mMarket {
            slug,
            condition_id: condition_id.clone(),
            up_token_id,
            down_token_id,
            window_end,
        })
    }
}

/// Calculate implied probability from price
pub fn price_to_probability(price: f64) -> f64 {
    price.clamp(0.0, 1.0)
}

/// Calculate price from probability
pub fn probability_to_price(prob: f64) -> f64 {
    prob.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_to_probability() {
        assert_eq!(price_to_probability(0.5), 0.5);
        assert_eq!(price_to_probability(0.0), 0.0);
        assert_eq!(price_to_probability(1.0), 1.0);
        assert_eq!(price_to_probability(1.5), 1.0); // clamped
        assert_eq!(price_to_probability(-0.1), 0.0); // clamped
    }
}
