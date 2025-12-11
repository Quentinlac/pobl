#!/usr/bin/env python3
"""
Backfill polymarket_trades table with individual trades from BTC 15-minute markets.

Uses the data-api.polymarket.com/trades endpoint which provides per-second resolution.
"""

import time
import json
from datetime import datetime, timezone
from decimal import Decimal
import requests
import psycopg2
from psycopg2.extras import execute_values
import argparse

# Database configuration
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "database": "polymarket",
}

# Polymarket APIs
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# Rate limiting
REQUEST_DELAY = 0.15  # seconds between requests


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def get_existing_windows(conn) -> set:
    """Get list of windows already in database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT window_timestamp FROM polymarket_trades
    """)
    result = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return result


def get_window_timestamps(days_back: int = 28) -> list:
    """Generate all 15-minute window timestamps for the past N days."""
    now = int(time.time())
    current_window = (now // 900) * 900

    # Go back N days
    start_window = current_window - (days_back * 24 * 60 * 60)

    windows = []
    ts = start_window
    while ts <= current_window:
        windows.append(ts)
        ts += 900

    return windows


def fetch_market_info(window_timestamp: int) -> dict | None:
    """Fetch market info including condition ID."""
    slug = f"btc-updown-15m-{window_timestamp}"
    url = f"{GAMMA_API}/markets?slug={slug}"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        markets = resp.json()

        if not markets:
            return None

        market = markets[0]
        return {
            "condition_id": market.get("conditionId"),
            "slug": slug,
        }

    except Exception as e:
        return None


def fetch_trades(condition_id: str, limit: int = 10000) -> list:
    """Fetch all trades for a market."""
    url = f"{DATA_API}/trades"
    params = {
        "market": condition_id,
        "limit": limit,
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"    Error fetching trades: {e}")
        return []


def fetch_window_trades(window_timestamp: int) -> dict | None:
    """Fetch all trades for a window."""
    # Get market info
    market_info = fetch_market_info(window_timestamp)
    if not market_info or not market_info.get("condition_id"):
        return None

    time.sleep(REQUEST_DELAY)

    # Fetch trades
    trades = fetch_trades(market_info["condition_id"])
    if not trades:
        return None

    return {
        "window_timestamp": window_timestamp,
        "condition_id": market_info["condition_id"],
        "slug": market_info["slug"],
        "trades": trades,
    }


def insert_trades(conn, window_data: dict) -> int:
    """Insert trades for a window."""
    if not window_data or not window_data.get("trades"):
        return 0

    cursor = conn.cursor()
    data = []

    window_ts = window_data["window_timestamp"]
    condition_id = window_data["condition_id"]
    slug = window_data["slug"]

    for trade in window_data["trades"]:
        try:
            tx_hash = trade.get("transactionHash", "")
            if not tx_hash:
                continue

            ts = datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc)

            data.append((
                tx_hash,
                condition_id,
                window_ts,
                slug,
                ts,
                trade.get("outcome", ""),
                trade.get("side", ""),
                Decimal(str(trade.get("price", 0))),
                Decimal(str(trade.get("size", 0))),
                trade.get("asset", ""),
                trade.get("proxyWallet", ""),
            ))
        except Exception as e:
            continue

    if not data:
        cursor.close()
        return 0

    query = """
        INSERT INTO polymarket_trades
        (transaction_hash, condition_id, window_timestamp, slug, timestamp,
         outcome, side, price, size, asset, proxy_wallet)
        VALUES %s
        ON CONFLICT (transaction_hash) DO NOTHING
    """

    execute_values(cursor, query, data, page_size=1000)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    return inserted


def get_stats(conn):
    """Get database statistics."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(DISTINCT window_timestamp) as num_windows,
            COUNT(*) as total_trades,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(*) FILTER (WHERE outcome = 'Up') as up_trades,
            COUNT(*) FILTER (WHERE outcome = 'Down') as down_trades,
            SUM(size) as total_volume
        FROM polymarket_trades
    """)
    result = cursor.fetchone()
    cursor.close()
    return {
        "num_windows": result[0],
        "total_trades": result[1],
        "earliest": result[2],
        "latest": result[3],
        "up_trades": result[4],
        "down_trades": result[5],
        "total_volume": result[6],
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill Polymarket trades")
    parser.add_argument("--days", type=int, default=28, help="Days to backfill (default: 28)")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip windows already in DB")
    args = parser.parse_args()

    print("=" * 70)
    print("  BACKFILL POLYMARKET TRADES - BTC 15-MINUTE UP/DOWN MARKETS")
    print("=" * 70)
    print()

    # Connect to database
    print("Connecting to database...")
    conn = get_db_connection()

    # Get existing windows
    existing_windows = get_existing_windows(conn) if args.skip_existing else set()
    print(f"Found {len(existing_windows)} windows already in database")

    # Generate window timestamps
    windows = get_window_timestamps(args.days)
    print(f"\nGenerating windows for past {args.days} days...")
    print(f"Total windows to process: {len(windows)}")

    # Filter out existing windows
    windows_to_fetch = [w for w in windows if w not in existing_windows]
    print(f"Windows to fetch (excluding existing): {len(windows_to_fetch)}")

    if not windows_to_fetch:
        print("\nNo new windows to fetch!")
        stats = get_stats(conn)
        print_stats(stats)
        conn.close()
        return

    # Process windows
    print(f"\nFetching trades data...")
    print("This may take a while...\n")

    total_inserted = 0
    fetched_count = 0
    no_data_count = 0

    start_time = time.time()

    for i, window_ts in enumerate(windows_to_fetch):
        window_dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
        progress = (i + 1) / len(windows_to_fetch) * 100

        # Fetch trades
        data = fetch_window_trades(window_ts)

        if data and data.get("trades"):
            # Insert into database
            inserted = insert_trades(conn, data)
            total_inserted += inserted
            fetched_count += 1

            trade_count = len(data["trades"])
            print(f"[{progress:5.1f}%] {window_dt.strftime('%Y-%m-%d %H:%M')} UTC - "
                  f"{trade_count} trades, inserted: {inserted}")
        else:
            no_data_count += 1
            print(f"[{progress:5.1f}%] {window_dt.strftime('%Y-%m-%d %H:%M')} UTC - no trades")

        time.sleep(REQUEST_DELAY)

        # Progress summary every 100 windows
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(windows_to_fetch) - i - 1) / rate / 60
            print(f"\n--- Progress: {i+1}/{len(windows_to_fetch)} windows, "
                  f"{total_inserted} trades inserted, ETA: {remaining:.1f} min ---\n")

    elapsed = time.time() - start_time

    # Final stats
    print()
    print("=" * 70)
    print("                         BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Time elapsed:         {elapsed/60:.1f} minutes")
    print(f"  Windows processed:    {len(windows_to_fetch)}")
    print(f"  Windows with trades:  {fetched_count}")
    print(f"  Windows no trades:    {no_data_count}")
    print(f"  Total trades inserted: {total_inserted}")
    print()

    stats = get_stats(conn)
    print_stats(stats)

    conn.close()
    print("\nDone!")


def print_stats(stats):
    print("=" * 70)
    print("                       DATABASE STATISTICS")
    print("=" * 70)
    print(f"  Total windows:        {stats['num_windows']:,}")
    print(f"  Total trades:         {stats['total_trades']:,}")
    print(f"  Up trades:            {stats['up_trades']:,}")
    print(f"  Down trades:          {stats['down_trades']:,}")
    if stats['total_volume']:
        print(f"  Total volume:         {stats['total_volume']:,.0f} shares")
    if stats['earliest']:
        print(f"  Earliest trade:       {stats['earliest']}")
    if stats['latest']:
        print(f"  Latest trade:         {stats['latest']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
