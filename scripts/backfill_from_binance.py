#!/usr/bin/env python3
"""
Backfill chainlink_prices table with historical Binance BTC/USDT data.

Binance provides free historical kline data. We'll fetch 1-minute candles
and insert them into chainlink_prices to have 6 months of history.
"""

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import requests
import psycopg2
from psycopg2.extras import execute_values

# Database configuration
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "database": "polymarket",
}

# Binance API
BINANCE_API = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def get_earliest_chainlink_timestamp(conn):
    """Get the earliest timestamp in chainlink_prices to avoid gaps."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(timestamp) FROM chainlink_prices WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def fetch_binance_klines(start_time_ms: int, end_time_ms: int, interval: str = "1m") -> list:
    """
    Fetch klines from Binance API.

    Binance returns max 1000 candles per request.
    """
    all_klines = []
    current_start = start_time_ms

    while current_start < end_time_ms:
        params = {
            "symbol": SYMBOL,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_time_ms,
            "limit": 1000,
        }

        try:
            resp = requests.get(BINANCE_API, params=params, timeout=30)
            resp.raise_for_status()
            klines = resp.json()

            if not klines:
                break

            all_klines.extend(klines)

            # Next batch starts after last candle
            last_close_time = klines[-1][6]  # Close time of last candle
            current_start = last_close_time + 1

            # Progress
            progress_pct = (current_start - start_time_ms) / (end_time_ms - start_time_ms) * 100
            print(f"    Fetched {len(all_klines):,} candles ({progress_pct:.1f}%)", end="\r")

            time.sleep(0.1)  # Rate limiting

        except Exception as e:
            print(f"\n    Error: {e}")
            time.sleep(1)
            continue

    print()
    return all_klines


def parse_klines(klines: list) -> list:
    """
    Parse Binance klines into price records.

    Kline format:
    [
      0: Open time (ms),
      1: Open,
      2: High,
      3: Low,
      4: Close,
      5: Volume,
      6: Close time (ms),
      7: Quote asset volume,
      8: Number of trades,
      ...
    ]
    """
    prices = []

    for k in klines:
        try:
            # Use close time as timestamp (end of candle)
            timestamp = datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc)

            prices.append({
                "timestamp": timestamp,
                "open": Decimal(k[1]),
                "high": Decimal(k[2]),
                "low": Decimal(k[3]),
                "close": Decimal(k[4]),
                "volume": Decimal(k[5]),
                "quote_volume": Decimal(k[7]),
                "num_trades": int(k[8]),
            })
        except Exception as e:
            continue

    return prices


def insert_prices(conn, prices, source_label="BINANCE"):
    """Insert prices into chainlink_prices table."""
    if not prices:
        return 0

    cursor = conn.cursor()

    # Prepare data - use BTCUSD to match Chainlink data
    data = []
    for p in prices:
        data.append((
            "BTCUSD",           # symbol (matching Chainlink)
            p["timestamp"],
            p["open"],
            p["high"],
            p["low"],
            p["close"],
            p["volume"],
            p["quote_volume"],
            p["num_trades"],
            Decimal(0),         # round_id (0 for Binance data)
        ))

    # Insert with ON CONFLICT - don't overwrite existing Chainlink data
    query = """
        INSERT INTO chainlink_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price,
         volume, quote_volume, num_trades, round_id)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO NOTHING
    """

    execute_values(cursor, query, data, page_size=1000)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    return inserted


def get_stats(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            MIN(close_price) as min_price,
            MAX(close_price) as max_price,
            COUNT(CASE WHEN round_id = 0 THEN 1 END) as binance_count,
            COUNT(CASE WHEN round_id > 0 THEN 1 END) as chainlink_count
        FROM chainlink_prices
        WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()
    cursor.close()
    return {
        "total": result[0],
        "earliest": result[1],
        "latest": result[2],
        "min_price": result[3],
        "max_price": result[4],
        "binance_count": result[5],
        "chainlink_count": result[6],
    }


def main():
    print("=" * 65)
    print("  BACKFILL CHAINLINK_PRICES FROM BINANCE HISTORICAL DATA")
    print("=" * 65)
    print()

    # Connect to database
    print("Connecting to database...")
    conn = get_db_connection()

    # Get earliest Chainlink timestamp to avoid gaps
    earliest_chainlink = get_earliest_chainlink_timestamp(conn)

    if earliest_chainlink:
        print(f"Earliest Chainlink data: {earliest_chainlink}")
        end_time = earliest_chainlink
    else:
        print("No Chainlink data found, fetching up to now")
        end_time = datetime.now(timezone.utc)

    # Calculate 6 months back
    start_time = end_time - timedelta(days=180)

    print(f"\nFetching Binance data from {start_time.date()} to {end_time.date()}")
    print(f"This is approximately {(end_time - start_time).days} days of data")

    # Convert to milliseconds for Binance API
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Fetch 1-minute candles from Binance
    print("\nFetching 1-minute candles from Binance API...")
    klines = fetch_binance_klines(start_ms, end_ms, interval="1m")

    print(f"Total candles fetched: {len(klines):,}")

    if not klines:
        print("No data fetched!")
        conn.close()
        return

    # Parse klines
    print("\nParsing candle data...")
    prices = parse_klines(klines)
    print(f"Parsed {len(prices):,} price records")

    # Show sample
    if prices:
        print(f"\nDate range: {prices[0]['timestamp']} to {prices[-1]['timestamp']}")
        print(f"Price range: ${prices[0]['close']:,.2f} to ${prices[-1]['close']:,.2f}")

    # Insert into database
    print("\nInserting into chainlink_prices...")
    inserted = insert_prices(conn, prices)
    print(f"Inserted {inserted:,} new records (duplicates skipped)")

    # Show final stats
    stats = get_stats(conn)
    print()
    print("=" * 65)
    print("                    FINAL DATABASE STATS")
    print("=" * 65)
    print(f"  Total rows:       {stats['total']:,}")
    print(f"  From Binance:     {stats['binance_count']:,} (round_id=0)")
    print(f"  From Chainlink:   {stats['chainlink_count']:,} (round_id>0)")
    print(f"  Earliest:         {stats['earliest']}")
    print(f"  Latest:           {stats['latest']}")
    print(f"  Price range:      ${stats['min_price']:,.2f} to ${stats['max_price']:,.2f}")
    print("=" * 65)

    # Check for gaps
    print("\nChecking for gaps...")
    cursor = conn.cursor()
    cursor.execute("""
        WITH time_diffs AS (
            SELECT
                timestamp,
                LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
                EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp))) as gap_seconds
            FROM chainlink_prices
            WHERE symbol = 'BTCUSD'
        )
        SELECT COUNT(*) as large_gaps
        FROM time_diffs
        WHERE gap_seconds > 300  -- gaps > 5 minutes
    """)
    large_gaps = cursor.fetchone()[0]
    cursor.close()

    if large_gaps > 0:
        print(f"  Warning: {large_gaps} gaps > 5 minutes found")
    else:
        print("  No significant gaps found!")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
