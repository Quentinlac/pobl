//! Polymarket CLOB order execution
//!
//! Handles EIP-712 signing and order submission to the Polymarket CLOB API.

use anyhow::{anyhow, Context, Result};
use base64::{Engine as _, engine::general_purpose::{STANDARD as BASE64, URL_SAFE as BASE64_URL}};
use hmac::{Hmac, Mac};
use reqwest::Client;
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sha3::{Digest, Keccak256};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// CTF Exchange contract address on Polygon
const CTF_EXCHANGE: &str = "4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

/// CLOB operator address (taker) - without 0x prefix
const CLOB_OPERATOR: &str = "F629eBBa8e4f121BDec77B2f9ED0e9fa28acbdc0";

/// Order side enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

/// Signature type enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureType {
    /// EOA wallet - signer owns the maker address
    Eoa = 0,
    /// Magic/Email wallet - signer is authorized for a different maker address
    Poly = 1,
}

/// Order type for submission
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderType {
    /// Good-Till-Cancelled
    GTC,
    /// Fill-Or-Kill (market order, full execution or cancel)
    FOK,
    /// Good-Till-Date (expires at specified timestamp)
    GTD,
}

// ============================================================================
// FOK Decimal Precision Helpers
// ============================================================================
// FOK orders have strict decimal requirements:
//   - makerAmount: max 2 decimal places
//   - takerAmount: max 4 decimal places
//
// For BUY:  makerAmount=USDC, takerAmount=shares
// For SELL: makerAmount=shares, takerAmount=USDC
//
// Key constraint: price × shares must produce USDC with max 2 decimals
// ============================================================================

/// Round shares DOWN to ensure makerAmount (USDC) has max 2 decimals for FOK BUY
/// Returns (rounded_shares, exact_usdc) where exact_usdc = price × rounded_shares
pub fn round_for_fok_buy(price: f64, amount_usdc: f64) -> (f64, f64) {
    // 1. Round price to 0.01 tick (cents)
    let rounded_price = (price * 100.0).floor() / 100.0;

    // 2. Calculate initial shares
    let shares = amount_usdc / rounded_price;

    // 3. Round shares DOWN to ensure price × shares has max 2 decimals
    // We need price × shares = XX.XX (2 decimals)
    // Strategy: iterate down from floor(shares) until we get valid USDC
    let mut rounded_shares = (shares * 100.0).floor() / 100.0;

    // 4. Calculate exact USDC and ensure it has max 2 decimals
    let mut exact_usdc = rounded_price * rounded_shares;
    exact_usdc = (exact_usdc * 100.0).round() / 100.0;  // Round to 2 decimals

    // 5. Verify: recalculate shares from exact_usdc to ensure consistency
    // This handles edge cases where rounding causes issues
    if exact_usdc > 0.0 && rounded_price > 0.0 {
        let final_shares = exact_usdc / rounded_price;
        // Round shares to 4 decimals (takerAmount precision)
        rounded_shares = (final_shares * 10000.0).floor() / 10000.0;
    }

    (rounded_shares, exact_usdc)
}

/// Round shares DOWN to 2 decimals for FOK SELL
/// Returns (rounded_shares, exact_usdc) where exact_usdc = price × rounded_shares
pub fn round_for_fok_sell(price: f64, shares: f64) -> (f64, f64) {
    // 1. Round price to 0.01 tick (cents)
    let rounded_price = (price * 100.0).floor() / 100.0;

    // 2. Round shares to 2 decimals (makerAmount for SELL)
    let rounded_shares = (shares * 100.0).floor() / 100.0;

    // 3. Calculate exact USDC and round to 4 decimals (takerAmount)
    let exact_usdc = rounded_price * rounded_shares;
    let exact_usdc = (exact_usdc * 10000.0).round() / 10000.0;

    (rounded_shares, exact_usdc)
}

/// Limit order size to available liquidity
/// Returns min(requested, available) rounded appropriately for FOK
pub fn limit_to_liquidity(requested_usdc: f64, price: f64, available_shares: f64) -> f64 {
    let max_usdc_from_liquidity = price * available_shares;
    let limited = requested_usdc.min(max_usdc_from_liquidity);
    // Round down to 2 decimals
    (limited * 100.0).floor() / 100.0
}

/// CTF Exchange Order structure
#[derive(Debug, Clone)]
pub struct Order {
    /// Salt as i64 (small number like Go SDK)
    pub salt: i64,
    pub maker: [u8; 20],
    pub signer: [u8; 20],
    pub taker: [u8; 20],
    pub token_id: [u8; 32],
    pub maker_amount: [u8; 32],
    pub taker_amount: [u8; 32],
    pub expiration: [u8; 32],
    pub nonce: [u8; 32],
    pub fee_rate_bps: [u8; 32],
    pub side: Side,
    pub signature_type: SignatureType,
    pub signature: Vec<u8>,
}

/// Signed order ready for submission
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrder {
    /// Salt as JSON number (i64)
    pub salt: i64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// Order request payload
#[derive(Debug, Clone, Serialize)]
struct OrderRequest {
    order: SignedOrder,
    owner: String,
    #[serde(rename = "orderType")]
    order_type: OrderType,
}

/// API credentials
#[derive(Debug, Clone)]
pub struct ApiCredentials {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}

/// API credential response
#[derive(Debug, Deserialize)]
struct ApiKeyResponse {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

/// Order response
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    pub success: bool,
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    #[serde(rename = "errorMsg")]
    pub error_msg: Option<String>,
}

/// Cancel order response
#[derive(Debug, Deserialize)]
pub struct CancelResponse {
    /// List of successfully cancelled order IDs
    pub canceled: Vec<String>,
    /// Map of order IDs to reasons why they couldn't be cancelled
    #[serde(default)]
    pub not_canceled: std::collections::HashMap<String, String>,
}

/// Order status from API
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
    Unknown,
}

impl<'de> serde::Deserialize<'de> for OrderStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Case-insensitive matching
        match s.to_uppercase().as_str() {
            "OPEN" | "LIVE" | "ACTIVE" => Ok(OrderStatus::Open),
            "FILLED" | "MATCHED" => Ok(OrderStatus::Filled),
            "CANCELLED" | "CANCELED" => Ok(OrderStatus::Cancelled),
            other => {
                tracing::warn!("Unknown order status from API: '{}'", other);
                Ok(OrderStatus::Unknown)
            }
        }
    }
}

/// Order details response
#[derive(Debug, Deserialize)]
pub struct OrderDetails {
    pub id: String,
    pub status: OrderStatus,
    #[serde(default)]
    pub size_matched: String,
    #[serde(default)]
    pub original_size: String,
    #[serde(default)]
    pub price: String,
    pub side: String,
}

/// Polymarket order executor
pub struct Executor {
    client: Client,
    clob_url: String,
    secp: Secp256k1<secp256k1::All>,
    secret_key: SecretKey,
    /// Signer address (derived from private key)
    wallet_address: [u8; 20],
    /// Funder address (Polymarket profile where USDC is held)
    /// If None, uses wallet_address
    funder_address: [u8; 20],
    /// Signature type (EOA=0, Poly/Magic=1)
    signature_type: SignatureType,
    credentials: Option<ApiCredentials>,
    nonce: u64,
}

/// Convert u64 to big-endian 32-byte array
fn u64_to_bytes32(val: u64) -> [u8; 32] {
    let mut result = [0u8; 32];
    result[24..32].copy_from_slice(&val.to_be_bytes());
    result
}

/// Convert u128 to big-endian 32-byte array
fn u128_to_bytes32(val: u128) -> [u8; 32] {
    let mut result = [0u8; 32];
    result[16..32].copy_from_slice(&val.to_be_bytes());
    result
}

/// Parse a decimal string token ID to bytes32
fn token_id_to_bytes32(token_id: &str) -> Result<[u8; 32]> {
    // Token IDs are large decimal numbers, convert to bytes
    let mut result = [0u8; 32];

    // Parse as u128 chunks since the number is very large
    // Actually, these are uint256 values, we need to handle them as strings
    // For now, we'll use a simple approach - parse digits and convert
    let bytes = decimal_to_bytes(token_id)?;
    let start = 32 - bytes.len().min(32);
    result[start..].copy_from_slice(&bytes[..bytes.len().min(32)]);
    Ok(result)
}

/// Convert decimal string to bytes (big-endian)
fn decimal_to_bytes(s: &str) -> Result<Vec<u8>> {
    // For very large numbers, we need to do manual conversion
    let mut result = vec![0u8];

    for c in s.chars() {
        if !c.is_ascii_digit() {
            return Err(anyhow!("Invalid decimal character"));
        }
        let digit = c.to_digit(10).unwrap() as u8;

        // Multiply by 10 and add digit
        let mut carry = digit as u16;
        for byte in result.iter_mut().rev() {
            let val = (*byte as u16) * 10 + carry;
            *byte = (val & 0xFF) as u8;
            carry = val >> 8;
        }
        while carry > 0 {
            result.insert(0, (carry & 0xFF) as u8);
            carry >>= 8;
        }
    }

    Ok(result)
}

/// Get public address from secret key
fn get_address(secp: &Secp256k1<secp256k1::All>, secret_key: &SecretKey) -> [u8; 20] {
    let public_key = PublicKey::from_secret_key(secp, secret_key);
    let public_key_bytes = &public_key.serialize_uncompressed()[1..]; // Remove 0x04 prefix

    let mut hasher = Keccak256::new();
    hasher.update(public_key_bytes);
    let hash = hasher.finalize();

    let mut address = [0u8; 20];
    address.copy_from_slice(&hash[12..32]);
    address
}

impl Executor {
    /// Create a new executor from private key
    pub async fn new(private_key: &str, clob_url: Option<String>) -> Result<Self> {
        let private_key = private_key.strip_prefix("0x").unwrap_or(private_key);
        let key_bytes = hex::decode(private_key)
            .context("Failed to decode private key")?;

        let secp = Secp256k1::new();
        let secret_key = SecretKey::from_slice(&key_bytes)
            .context("Invalid private key")?;

        let wallet_address = get_address(&secp, &secret_key);
        info!("Executor initialized for signer: 0x{}", hex::encode(wallet_address));

        // Check for separate funder address (for Magic/email wallets)
        let (funder_address, signature_type) = if let Ok(funder_str) = std::env::var("POLYMARKET_WALLET_ADDRESS") {
            let funder_str = funder_str.strip_prefix("0x").unwrap_or(&funder_str);
            let funder_bytes = hex::decode(funder_str)
                .context("Invalid POLYMARKET_WALLET_ADDRESS")?;
            if funder_bytes.len() != 20 {
                return Err(anyhow!("POLYMARKET_WALLET_ADDRESS must be 20 bytes"));
            }
            let mut arr = [0u8; 20];
            arr.copy_from_slice(&funder_bytes);

            // If funder != signer, use Poly signature type (Magic/email wallet)
            if arr != wallet_address {
                info!("Using Magic/email wallet mode (signatureType=1)");
                info!("  Funder: 0x{}", hex::encode(arr));
                info!("  Signer: 0x{}", hex::encode(wallet_address));
                (arr, SignatureType::Poly)
            } else {
                (arr, SignatureType::Eoa)
            }
        } else {
            // Default: funder = signer (EOA wallet)
            (wallet_address, SignatureType::Eoa)
        };

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let clob_url = clob_url.unwrap_or_else(|| {
            std::env::var("POLYMARKET_CLOB_URL")
                .unwrap_or_else(|_| "https://clob.polymarket.com".to_string())
        });

        let mut executor = Self {
            client,
            clob_url,
            secp,
            secret_key,
            wallet_address,
            funder_address,
            signature_type,
            credentials: None,
            nonce: 0,
        };

        // Derive API credentials
        executor.derive_api_key().await?;

        Ok(executor)
    }

    /// Derive API credentials using L1 authentication
    async fn derive_api_key(&mut self) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();

        // Create L1 auth signature
        let signature = self.create_l1_auth_signature(&timestamp, 0)?;

        let url = format!("{}/auth/derive-api-key", self.clob_url);

        let response = self.client
            .get(&url)
            .header("POLY_ADDRESS", format!("0x{}", hex::encode(self.wallet_address)))
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .header("POLY_NONCE", "0")
            .send()
            .await
            .context("Failed to derive API key")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to derive API key: {} - {}", status, text));
        }

        let creds: ApiKeyResponse = response.json().await
            .context("Failed to parse API key response")?;

        debug!("API key: {}", creds.api_key);
        debug!("API secret (first 20 chars): {}...", &creds.secret[..20.min(creds.secret.len())]);
        info!("API credentials derived successfully");

        self.credentials = Some(ApiCredentials {
            key: creds.api_key,
            secret: creds.secret,
            passphrase: creds.passphrase,
        });

        Ok(())
    }

    /// Create L1 authentication signature (EIP-712)
    fn create_l1_auth_signature(&self, timestamp: &str, nonce: u64) -> Result<String> {
        // ClobAuth EIP-712 domain
        let domain_separator = self.compute_clob_auth_domain();

        // ClobAuth message
        let message_str = "This message attests that I control the given wallet";
        let struct_hash = self.compute_clob_auth_struct_hash(
            &self.wallet_address,
            timestamp,
            nonce,
            message_str,
        );

        // EIP-712 hash
        let digest = self.compute_eip712_hash(&domain_separator, &struct_hash);

        // Sign with secp256k1
        let msg = Message::from_digest(digest);
        let sig = self.secp.sign_ecdsa_recoverable(&msg, &self.secret_key);
        let (rec_id, sig_bytes) = sig.serialize_compact();

        // Build r, s, v format (65 bytes)
        let mut sig_with_v = Vec::with_capacity(65);
        sig_with_v.extend_from_slice(&sig_bytes);
        sig_with_v.push(27 + rec_id.to_i32() as u8); // v = 27 + recovery_id

        Ok(format!("0x{}", hex::encode(&sig_with_v)))
    }

    /// Compute ClobAuth domain separator
    fn compute_clob_auth_domain(&self) -> [u8; 32] {
        let type_hash = keccak256(b"EIP712Domain(string name,string version,uint256 chainId)");
        let name_hash = keccak256(b"ClobAuthDomain");
        let version_hash = keccak256(b"1");

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&type_hash);
        encoded.extend_from_slice(&name_hash);
        encoded.extend_from_slice(&version_hash);
        encoded.extend_from_slice(&u64_to_bytes32(POLYGON_CHAIN_ID));

        keccak256(&encoded)
    }

    /// Compute ClobAuth struct hash
    fn compute_clob_auth_struct_hash(
        &self,
        address: &[u8; 20],
        timestamp: &str,
        nonce: u64,
        message: &str,
    ) -> [u8; 32] {
        let type_hash = keccak256(
            b"ClobAuth(address address,string timestamp,uint256 nonce,string message)"
        );

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&type_hash);
        // address is padded to 32 bytes
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(address);
        encoded.extend_from_slice(&keccak256(timestamp.as_bytes()));
        encoded.extend_from_slice(&u64_to_bytes32(nonce));
        encoded.extend_from_slice(&keccak256(message.as_bytes()));

        keccak256(&encoded)
    }

    /// Compute final EIP-712 hash
    fn compute_eip712_hash(&self, domain_separator: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
        let mut encoded = Vec::with_capacity(66);
        encoded.extend_from_slice(&[0x19, 0x01]);
        encoded.extend_from_slice(domain_separator);
        encoded.extend_from_slice(struct_hash);
        keccak256(&encoded)
    }

    /// Compute Order domain separator for CTF Exchange
    fn compute_order_domain(&self) -> [u8; 32] {
        let type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        );

        let name_hash = keccak256(b"Polymarket CTF Exchange");
        let version_hash = keccak256(b"1");
        let contract = hex::decode(CTF_EXCHANGE).unwrap();

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&type_hash);
        encoded.extend_from_slice(&name_hash);
        encoded.extend_from_slice(&version_hash);
        encoded.extend_from_slice(&u64_to_bytes32(POLYGON_CHAIN_ID));
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(&contract);

        keccak256(&encoded)
    }

    /// Compute Order struct hash
    fn compute_order_struct_hash(&self, order: &Order) -> [u8; 32] {
        let type_hash = keccak256(
            b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)"
        );

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&type_hash);
        // Convert i64 salt to 32-byte big-endian for EIP-712
        let mut salt_bytes = [0u8; 32];
        salt_bytes[24..].copy_from_slice(&order.salt.to_be_bytes());
        encoded.extend_from_slice(&salt_bytes);
        // Addresses are padded to 32 bytes
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(&order.maker);
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(&order.signer);
        encoded.extend_from_slice(&[0u8; 12]);
        encoded.extend_from_slice(&order.taker);
        encoded.extend_from_slice(&order.token_id);
        encoded.extend_from_slice(&order.maker_amount);
        encoded.extend_from_slice(&order.taker_amount);
        encoded.extend_from_slice(&order.expiration);
        encoded.extend_from_slice(&order.nonce);
        encoded.extend_from_slice(&order.fee_rate_bps);
        // side and signatureType are uint8, padded to 32 bytes
        let mut side_bytes = [0u8; 32];
        side_bytes[31] = order.side as u8;
        encoded.extend_from_slice(&side_bytes);
        let mut sig_type_bytes = [0u8; 32];
        sig_type_bytes[31] = order.signature_type as u8;
        encoded.extend_from_slice(&sig_type_bytes);

        keccak256(&encoded)
    }

    /// Sign an order
    fn sign_order(&self, order: &mut Order) -> Result<()> {
        let domain = self.compute_order_domain();
        let struct_hash = self.compute_order_struct_hash(order);
        let digest = self.compute_eip712_hash(&domain, &struct_hash);

        // Sign with secp256k1
        let msg = Message::from_digest(digest);
        let sig = self.secp.sign_ecdsa_recoverable(&msg, &self.secret_key);
        let (rec_id, sig_bytes) = sig.serialize_compact();

        let mut sig_with_v = Vec::with_capacity(65);
        sig_with_v.extend_from_slice(&sig_bytes);
        sig_with_v.push(27 + rec_id.to_i32() as u8);

        order.signature = sig_with_v;
        Ok(())
    }

    /// Create HMAC signature for API request
    fn create_hmac_signature(
        &self,
        secret: &str,
        timestamp: &str,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<String> {
        let message = format!("{}{}{}{}", timestamp, method, path, body);
        debug!("HMAC message: {} chars, starts with: {}...", message.len(), &message[..80.min(message.len())]);

        // Secret is URL-safe base64 encoded
        let secret_bytes = BASE64_URL.decode(secret)
            .context("Failed to decode API secret")?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
            .context("Failed to create HMAC")?;
        mac.update(message.as_bytes());

        let result = mac.finalize();
        // Output should be URL-safe base64 encoded
        let sig = BASE64_URL.encode(result.into_bytes());
        debug!("HMAC signature: {}", sig);
        Ok(sig)
    }

    /// Create a buy order
    ///
    /// # Arguments
    /// * `expiration_secs` - Optional expiration in seconds from now for GTD orders
    pub fn create_buy_order(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
        expiration_secs: Option<u64>,
    ) -> Result<Order> {
        // For BUY: makerAmount = USDC to spend, takerAmount = shares to receive
        // The API requires:
        // 1. Price must be on 0.01 tick (e.g., 0.49, 0.50, 0.51)
        // 2. makerAmount max 4 decimal places
        // 3. takerAmount max 2 decimal places (for BUY)
        // 4. makerAmount MUST equal price * takerAmount exactly

        // 1. Round price to 0.01 tick (cents)
        let rounded_price = (price * 100.0).floor() / 100.0;

        // 2. Calculate shares from the requested USDC amount
        let shares = amount_usdc / rounded_price;

        // 3. Round shares to 2 decimal places (takerAmount max 2 decimals for BUY)
        let rounded_shares = (shares * 100.0).floor() / 100.0;

        // 4. Recalculate exact makerAmount = price * shares (this is what Polymarket expects)
        let exact_usdc = rounded_price * rounded_shares;

        // 5. Convert to 6-decimal representation
        // makerAmount = exact_usdc with up to 4 decimals (multiply by 10000, then by 100 for 6 decimals)
        // takerAmount = shares with 2 decimals (multiply by 100, then by 10000 for 6 decimals)
        let maker_amount = ((exact_usdc * 10000.0).round() as u128) * 100;
        let taker_amount = (rounded_shares * 100.0) as u128 * 10000;

        info!("ORDER CALC: input_usdc=${:.2}, input_price={:.4}", amount_usdc, price);
        info!("ORDER CALC: rounded_price={:.4}, exact_usdc=${:.4}, shares={:.2}",
            rounded_price, exact_usdc, rounded_shares);
        info!("ORDER CALC: maker_amount={} (${:.2} USDC), taker_amount={} ({:.4} shares)",
            maker_amount, maker_amount as f64 / 1_000_000.0,
            taker_amount, taker_amount as f64 / 1_000_000.0);

        self.create_order(token_id, maker_amount, taker_amount, Side::Buy, expiration_secs)
    }

    /// Create an order
    ///
    /// # Arguments
    /// * `expiration_secs` - Optional expiration in seconds from now. If provided, order will
    ///   expire after this many seconds. Note: Polymarket has a 1-minute security threshold,
    ///   so the actual expiration is set to `now + 60 + expiration_secs`.
    fn create_order(
        &mut self,
        token_id: &str,
        maker_amount: u128,
        taker_amount: u128,
        side: Side,
        expiration_secs: Option<u64>,
    ) -> Result<Order> {
        // Random salt (small number like Go SDK: time.Now().UnixNano() % 1_000_000_000)
        let salt = (SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos() % 1_000_000_000) as i64;

        // Taker must be zero address for public orders (anyone can fill)
        let taker_arr = [0u8; 20];

        // Expiration: 0 for GTC/FOK orders, timestamp for GTD
        // Polymarket requires a 60-second security threshold minimum
        let expiration: u64 = match expiration_secs {
            Some(secs) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                now + 60 + secs // Add 60s security threshold required by Polymarket
            }
            None => 0,
        };

        // Nonce: always 0 for Polymarket orders (they use salt for uniqueness)
        let nonce: u64 = 0;

        Ok(Order {
            salt,
            maker: self.funder_address,   // Where funds are (Polymarket profile)
            signer: self.wallet_address,  // Who signs (private key wallet)
            taker: taker_arr,
            token_id: token_id_to_bytes32(token_id)?,
            maker_amount: u128_to_bytes32(maker_amount),
            taker_amount: u128_to_bytes32(taker_amount),
            expiration: u64_to_bytes32(expiration),
            nonce: u64_to_bytes32(nonce),
            fee_rate_bps: [0u8; 32],
            side,
            signature_type: self.signature_type,
            signature: Vec::new(),
        })
    }

    /// Convert bytes32 to decimal string
    fn bytes32_to_decimal(bytes: &[u8; 32]) -> String {
        // Skip leading zeros and convert to decimal
        let mut result = String::from("0");
        for &byte in bytes.iter() {
            // Multiply result by 256 and add byte
            let mut carry = byte as u32;
            let mut new_result = String::new();
            let chars: Vec<char> = result.chars().collect();

            for c in chars.iter().rev() {
                let digit = c.to_digit(10).unwrap();
                let val = digit * 256 + carry;
                carry = val / 10;
                new_result.insert(0, std::char::from_digit(val % 10, 10).unwrap());
            }

            while carry > 0 {
                new_result.insert(0, std::char::from_digit(carry % 10, 10).unwrap());
                carry /= 10;
            }

            result = new_result;
        }

        // Remove leading zeros, but keep at least "0"
        let trimmed = result.trim_start_matches('0');
        if trimmed.is_empty() {
            "0".to_string()
        } else {
            trimmed.to_string()
        }
    }

    /// Convert Order to SignedOrder for JSON serialization
    fn to_signed_order(&self, order: &Order) -> SignedOrder {
        SignedOrder {
            salt: order.salt,
            maker: format!("0x{}", hex::encode(order.maker)),
            signer: format!("0x{}", hex::encode(order.signer)),
            taker: format!("0x{}", hex::encode(order.taker)),
            token_id: Self::bytes32_to_decimal(&order.token_id),
            maker_amount: Self::bytes32_to_decimal(&order.maker_amount),
            taker_amount: Self::bytes32_to_decimal(&order.taker_amount),
            expiration: Self::bytes32_to_decimal(&order.expiration),
            nonce: Self::bytes32_to_decimal(&order.nonce),
            fee_rate_bps: Self::bytes32_to_decimal(&order.fee_rate_bps),
            side: if order.side == Side::Buy { "BUY" } else { "SELL" }.to_string(),
            signature_type: order.signature_type as u8,
            signature: format!("0x{}", hex::encode(&order.signature)),
        }
    }

    /// Submit an order to the CLOB
    pub async fn submit_order(
        &self,
        order: &Order,
        order_type: OrderType,
    ) -> Result<OrderResponse> {
        let creds = self.credentials.as_ref()
            .ok_or_else(|| anyhow!("API credentials not initialized"))?;

        let signed_order = self.to_signed_order(order);
        let request = OrderRequest {
            order: signed_order,
            owner: creds.key.clone(),
            order_type,
        };

        let body = serde_json::to_string(&request)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();

        let path = "/order";
        let signature = self.create_hmac_signature(
            &creds.secret,
            &timestamp,
            "POST",
            path,
            &body,
        )?;

        let url = format!("{}{}", self.clob_url, path);

        info!("Submitting order to {}", url);
        info!("Request body: {}", body);
        debug!("Headers: POLY_ADDRESS={}, POLY_API_KEY={}, POLY_TIMESTAMP={}",
            format!("0x{}", hex::encode(self.wallet_address)),
            &creds.key,
            &timestamp);

        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("POLY_ADDRESS", format!("0x{}", hex::encode(self.wallet_address)))
            .header("POLY_API_KEY", &creds.key)
            .header("POLY_PASSPHRASE", &creds.passphrase)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .body(body.clone())
            .send()
            .await
            .context("Failed to submit order")?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            warn!("Order submission failed: {} - {}", status, text);
            return Err(anyhow!("Order submission failed: {} - {}", status, text));
        }

        let result: OrderResponse = serde_json::from_str(&text)
            .context("Failed to parse order response")?;

        if result.success {
            info!("Order submitted successfully: {:?}", result.order_id);
        } else {
            warn!("Order rejected: {:?}", result.error_msg);
        }

        Ok(result)
    }

    /// Execute a market buy order with GTD (Good-Till-Date) - 1 second expiration
    pub async fn market_buy(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
    ) -> Result<OrderResponse> {
        // Use GTD with 1 second expiration for better fill rates
        // Note: Polymarket adds 60s security threshold, so actual expiration is ~61s
        let mut order = self.create_buy_order(token_id, price, amount_usdc, Some(1))?;
        self.sign_order(&mut order)?;
        self.submit_order(&order, OrderType::GTD).await
    }

    /// Execute a FOK (Fill-Or-Kill) buy order
    ///
    /// FOK orders execute immediately in full or are cancelled entirely.
    /// Faster than GTD but may fail if liquidity is insufficient.
    ///
    /// Returns (OrderResponse, actual_usdc, actual_shares) where actual values
    /// reflect the rounded amounts that were actually submitted.
    pub async fn fok_buy(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
    ) -> Result<(OrderResponse, f64, f64)> {
        // Use FOK precision helpers to ensure valid decimal places
        let (rounded_shares, exact_usdc) = round_for_fok_buy(price, amount_usdc);

        if rounded_shares <= 0.0 || exact_usdc < 0.01 {
            return Err(anyhow!("FOK buy: amount too small after rounding"));
        }

        info!("FOK BUY: requested=${:.2} at {:.2}¢ → actual=${:.2} for {:.4} shares",
            amount_usdc, price * 100.0, exact_usdc, rounded_shares);

        // Create order with exact amounts (no expiration for FOK)
        let mut order = self.create_buy_order(token_id, price, exact_usdc, None)?;
        self.sign_order(&mut order)?;

        let response = self.submit_order(&order, OrderType::FOK).await?;
        Ok((response, exact_usdc, rounded_shares))
    }

    /// Execute a FOK buy with liquidity check
    ///
    /// Will only buy up to the available liquidity at the given price.
    /// Returns (OrderResponse, actual_usdc, actual_shares)
    pub async fn fok_buy_with_liquidity(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
        available_shares: f64,
    ) -> Result<(OrderResponse, f64, f64)> {
        // Limit to available liquidity
        let limited_usdc = limit_to_liquidity(amount_usdc, price, available_shares);

        if limited_usdc < 0.01 {
            return Err(anyhow!("Insufficient liquidity: requested ${:.2}, available {:.2} shares at {:.2}¢",
                amount_usdc, available_shares, price * 100.0));
        }

        if limited_usdc < amount_usdc {
            info!("FOK BUY: limited by liquidity ${:.2} → ${:.2} ({:.2} shares available)",
                amount_usdc, limited_usdc, available_shares);
        }

        self.fok_buy(token_id, price, limited_usdc).await
    }

    /// Execute a market buy order with custom expiration
    pub async fn market_buy_with_expiration(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
        expiration_secs: u64,
    ) -> Result<OrderResponse> {
        let mut order = self.create_buy_order(token_id, price, amount_usdc, Some(expiration_secs))?;
        self.sign_order(&mut order)?;
        self.submit_order(&order, OrderType::GTD).await
    }

    /// Create a sell order
    ///
    /// For SELL: makerAmount = shares we're selling, takerAmount = USDC we want to receive
    pub fn create_sell_order(
        &mut self,
        token_id: &str,
        price: f64,
        shares: f64,
        expiration_secs: Option<u64>,
    ) -> Result<Order> {
        // For SELL: makerAmount = shares, takerAmount = USDC
        // Price must be on 0.01 tick (e.g., 0.49, 0.50, 0.51)

        // 1. Round price to 0.01 tick (cents)
        let rounded_price = (price * 100.0).floor() / 100.0;

        // 2. Round shares to 2 decimal places
        let rounded_shares = (shares * 100.0).floor() / 100.0;

        // 3. Compute exact takerAmount (USDC) = rounded_price × rounded_shares
        // Round to 4 decimal places to avoid floating point errors
        let taker_amount_f64 = (rounded_price * rounded_shares * 10000.0).round() / 10000.0;

        // 4. Convert to 6-decimal representation (use round() to avoid truncation errors)
        let maker_amount = (rounded_shares * 1_000_000.0).round() as u128;  // shares we sell
        let taker_amount = (taker_amount_f64 * 1_000_000.0).round() as u128; // USDC we receive

        info!("SELL ORDER CALC: input_shares={:.2}, input_price={:.4}", shares, price);
        info!("SELL ORDER CALC: rounded_price={:.4}, rounded_shares={:.2}",
            rounded_price, rounded_shares);
        info!("SELL ORDER CALC: maker_amount={} ({:.2} shares), taker_amount={} (${:.2} USDC)",
            maker_amount, maker_amount as f64 / 1_000_000.0,
            taker_amount, taker_amount as f64 / 1_000_000.0);

        self.create_order(token_id, maker_amount, taker_amount, Side::Sell, expiration_secs)
    }

    /// Execute a market sell order with GTD (Good-Till-Date) - 1 second expiration
    pub async fn market_sell(
        &mut self,
        token_id: &str,
        price: f64,
        shares: f64,
    ) -> Result<OrderResponse> {
        // Use GTD with 1 second expiration for better fill rates
        // Note: Polymarket adds 60s security threshold, so actual expiration is ~61s
        let mut order = self.create_sell_order(token_id, price, shares, Some(1))?;
        self.sign_order(&mut order)?;
        self.submit_order(&order, OrderType::GTD).await
    }

    /// Execute a FOK (Fill-Or-Kill) sell order
    ///
    /// FOK orders execute immediately in full or are cancelled entirely.
    /// Returns (OrderResponse, actual_shares, actual_usdc) where actual values
    /// reflect the rounded amounts that were actually submitted.
    pub async fn fok_sell(
        &mut self,
        token_id: &str,
        price: f64,
        shares: f64,
    ) -> Result<(OrderResponse, f64, f64)> {
        // Use FOK precision helpers to ensure valid decimal places
        let (rounded_shares, exact_usdc) = round_for_fok_sell(price, shares);

        if rounded_shares < 0.01 {
            return Err(anyhow!("FOK sell: shares too small after rounding"));
        }

        info!("FOK SELL: requested={:.4} shares at {:.2}¢ → actual={:.2} shares for ${:.4}",
            shares, price * 100.0, rounded_shares, exact_usdc);

        // Create order with exact amounts (no expiration for FOK)
        let mut order = self.create_sell_order(token_id, price, rounded_shares, None)?;
        self.sign_order(&mut order)?;

        let response = self.submit_order(&order, OrderType::FOK).await?;
        Ok((response, rounded_shares, exact_usdc))
    }

    /// Execute a FOK sell with liquidity check
    ///
    /// Will only sell up to the available bid liquidity.
    /// Returns (OrderResponse, actual_shares, actual_usdc)
    pub async fn fok_sell_with_liquidity(
        &mut self,
        token_id: &str,
        price: f64,
        shares: f64,
        available_bid_shares: f64,
    ) -> Result<(OrderResponse, f64, f64)> {
        // Limit to available bid liquidity
        let limited_shares = shares.min(available_bid_shares);
        let (rounded_shares, _) = round_for_fok_sell(price, limited_shares);

        if rounded_shares < 0.01 {
            return Err(anyhow!("Insufficient bid liquidity: requested {:.2} shares, available {:.2}",
                shares, available_bid_shares));
        }

        if limited_shares < shares {
            info!("FOK SELL: limited by liquidity {:.2} → {:.2} shares ({:.2} available)",
                shares, limited_shares, available_bid_shares);
        }

        self.fok_sell(token_id, price, rounded_shares).await
    }

    /// Get order details by ID
    pub async fn get_order(&self, order_id: &str) -> Result<OrderDetails> {
        let creds = self.credentials.as_ref()
            .ok_or_else(|| anyhow!("API credentials not initialized"))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();

        let path = format!("/data/order/{}", order_id);

        let signature = self.create_hmac_signature(
            &creds.secret,
            &timestamp,
            "GET",
            &path,
            "",
        )?;

        let url = format!("{}{}", self.clob_url, path);

        let response = self.client
            .get(&url)
            .header("POLY_ADDRESS", format!("0x{}", hex::encode(self.wallet_address)))
            .header("POLY_API_KEY", &creds.key)
            .header("POLY_PASSPHRASE", &creds.passphrase)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .send()
            .await
            .context("Failed to get order")?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("Failed to get order: {} - {}", status, text));
        }

        // Debug log the raw response to diagnose status parsing
        debug!("Order API response: {}", text);

        let order: OrderDetails = serde_json::from_str(&text)
            .context("Failed to parse order response")?;

        debug!("Parsed order status: {:?}, size_matched: {}", order.status, order.size_matched);

        Ok(order)
    }

    /// Cancel an order by ID
    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelResponse> {
        let creds = self.credentials.as_ref()
            .ok_or_else(|| anyhow!("API credentials not initialized"))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .to_string();

        let path = "/order";
        let body = format!(r#"{{"orderID":"{}"}}"#, order_id);

        let signature = self.create_hmac_signature(
            &creds.secret,
            &timestamp,
            "DELETE",
            path,
            &body,
        )?;

        let url = format!("{}{}", self.clob_url, path);

        debug!("Cancelling order: {}", order_id);

        let response = self.client
            .delete(&url)
            .header("Content-Type", "application/json")
            .header("POLY_ADDRESS", format!("0x{}", hex::encode(self.wallet_address)))
            .header("POLY_API_KEY", &creds.key)
            .header("POLY_PASSPHRASE", &creds.passphrase)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .body(body)
            .send()
            .await
            .context("Failed to cancel order")?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            warn!("Order cancellation failed: {} - {}", status, text);
            return Err(anyhow!("Order cancellation failed: {} - {}", status, text));
        }

        let result: CancelResponse = serde_json::from_str(&text)
            .context("Failed to parse cancel response")?;

        if !result.canceled.is_empty() {
            info!("Order cancelled: {}", order_id);
        } else if let Some(reason) = result.not_canceled.get(order_id) {
            debug!("Order not cancelled ({}): {}", order_id, reason);
        }

        Ok(result)
    }

    /// Get signer wallet address as hex string
    pub fn wallet_address(&self) -> String {
        format!("0x{}", hex::encode(self.wallet_address))
    }

    /// Get funder address (where USDC is held) as hex string
    pub fn funder_address(&self) -> String {
        format!("0x{}", hex::encode(self.funder_address))
    }
}

/// Keccak256 hash helper
fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}
