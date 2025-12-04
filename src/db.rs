use anyhow::Result;
use chrono::{DateTime, Utc};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::Decimal;
use tokio_postgres::{Client, NoTls, Row};

use crate::models::PricePoint;

/// Database configuration
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub use_tls: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            host: "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com".to_string(),
            port: 5432,
            user: "qoveryadmin".to_string(),
            password: "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp".to_string(),
            database: "polymarket".to_string(),
            use_tls: true,
        }
    }
}

/// Connect to the PostgreSQL database
pub async fn connect(config: &DbConfig) -> Result<Client> {
    let connection_string = format!(
        "host={} port={} user={} password={} dbname={}",
        config.host, config.port, config.user, config.password, config.database
    );

    let client = if config.use_tls {
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true) // For RDS, we accept the cert
            .build()?;
        let connector = MakeTlsConnector::new(connector);

        let (client, connection) = tokio_postgres::connect(&connection_string, connector).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Database connection error: {}", e);
            }
        });

        client
    } else {
        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Database connection error: {}", e);
            }
        });

        client
    };

    Ok(client)
}

/// Fetch all BTC price data from the database
/// Returns data ordered by timestamp ascending
pub async fn fetch_all_prices(client: &Client) -> Result<Vec<PricePoint>> {
    let query = r#"
        SELECT timestamp, close_price
        FROM binance_prices
        WHERE symbol = 'BTCUSDT'
        ORDER BY timestamp ASC
    "#;

    let rows = client.query(query, &[]).await?;

    let prices: Vec<PricePoint> = rows
        .iter()
        .map(|row| {
            let timestamp: DateTime<Utc> = row.get(0);
            let close_price: Decimal = row.get(1);
            PricePoint {
                timestamp,
                close_price,
            }
        })
        .collect();

    Ok(prices)
}

/// Fetch price data for a specific date range
pub async fn fetch_prices_range(
    client: &Client,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<PricePoint>> {
    let query = r#"
        SELECT timestamp, close_price
        FROM binance_prices
        WHERE symbol = 'BTCUSDT'
          AND timestamp >= $1
          AND timestamp <= $2
        ORDER BY timestamp ASC
    "#;

    let rows = client.query(query, &[&start, &end]).await?;

    let prices: Vec<PricePoint> = rows
        .iter()
        .map(|row| {
            let timestamp: DateTime<Utc> = row.get(0);
            let close_price: Decimal = row.get(1);
            PricePoint {
                timestamp,
                close_price,
            }
        })
        .collect();

    Ok(prices)
}

/// Get the date range of available data
pub async fn get_data_range(client: &Client) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let query = r#"
        SELECT MIN(timestamp), MAX(timestamp)
        FROM binance_prices
        WHERE symbol = 'BTCUSDT'
    "#;

    let row = client.query_one(query, &[]).await?;
    let min: DateTime<Utc> = row.get(0);
    let max: DateTime<Utc> = row.get(1);

    Ok((min, max))
}

/// Get total count of price records
pub async fn get_price_count(client: &Client) -> Result<i64> {
    let query = r#"
        SELECT COUNT(*)
        FROM binance_prices
        WHERE symbol = 'BTCUSDT'
    "#;

    let row = client.query_one(query, &[]).await?;
    let count: i64 = row.get(0);

    Ok(count)
}

#[cfg(test)]
mod tests {
    // Integration tests would go here
    // They require a live database connection
}
