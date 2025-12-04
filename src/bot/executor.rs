//! Polymarket CLOB order execution
//!
//! Handles EIP-712 signing and order submission to the Polymarket CLOB API.

use anyhow::{anyhow, Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
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
    Eoa = 0,
}

/// Order type for submission
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderType {
    /// Good-Till-Cancelled
    GTC,
    /// Fill-Or-Kill (market order, full execution or cancel)
    FOK,
}

/// CTF Exchange Order structure
#[derive(Debug, Clone)]
pub struct Order {
    pub salt: [u8; 32],
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
    pub salt: String,
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

/// Polymarket order executor
pub struct Executor {
    client: Client,
    clob_url: String,
    secp: Secp256k1<secp256k1::All>,
    secret_key: SecretKey,
    wallet_address: [u8; 20],
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
        info!("Executor initialized for wallet: 0x{}", hex::encode(wallet_address));

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
        encoded.extend_from_slice(&order.salt);
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

        let secret_bytes = BASE64.decode(secret)
            .context("Failed to decode API secret")?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
            .context("Failed to create HMAC")?;
        mac.update(message.as_bytes());

        let result = mac.finalize();
        Ok(BASE64.encode(result.into_bytes()))
    }

    /// Create a buy order
    pub fn create_buy_order(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
    ) -> Result<Order> {
        // For BUY: makerAmount = USDC to spend, takerAmount = shares to receive
        let shares = amount_usdc / price;

        // Polymarket uses 1e6 scaling for amounts
        let maker_amount = (amount_usdc * 1_000_000.0) as u128;
        let taker_amount = (shares * 1_000_000.0) as u128;

        self.create_order(token_id, maker_amount, taker_amount, Side::Buy)
    }

    /// Create an order
    fn create_order(
        &mut self,
        token_id: &str,
        maker_amount: u128,
        taker_amount: u128,
        side: Side,
    ) -> Result<Order> {
        // Random salt
        let mut salt = [0u8; 32];
        for i in 0..32 {
            salt[i] = rand::random();
        }

        let taker = hex::decode(CLOB_OPERATOR).unwrap();
        let mut taker_arr = [0u8; 20];
        taker_arr.copy_from_slice(&taker);

        // Expiration: 1 hour from now
        let expiration = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() + 3600;

        self.nonce += 1;

        Ok(Order {
            salt,
            maker: self.wallet_address,
            signer: self.wallet_address,
            taker: taker_arr,
            token_id: token_id_to_bytes32(token_id)?,
            maker_amount: u128_to_bytes32(maker_amount),
            taker_amount: u128_to_bytes32(taker_amount),
            expiration: u64_to_bytes32(expiration),
            nonce: u64_to_bytes32(self.nonce),
            fee_rate_bps: [0u8; 32],
            side,
            signature_type: SignatureType::Eoa,
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
            salt: Self::bytes32_to_decimal(&order.salt),
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
            .as_millis()
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

        debug!("Submitting order to {}", url);

        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("POLY_ADDRESS", format!("0x{}", hex::encode(self.wallet_address)))
            .header("POLY_API_KEY", &creds.key)
            .header("POLY_PASSPHRASE", &creds.passphrase)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", &timestamp)
            .body(body)
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

    /// Execute a market buy order (FOK)
    pub async fn market_buy(
        &mut self,
        token_id: &str,
        price: f64,
        amount_usdc: f64,
    ) -> Result<OrderResponse> {
        let mut order = self.create_buy_order(token_id, price, amount_usdc)?;
        self.sign_order(&mut order)?;
        self.submit_order(&order, OrderType::FOK).await
    }

    /// Get wallet address as hex string
    pub fn wallet_address(&self) -> String {
        format!("0x{}", hex::encode(self.wallet_address))
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
