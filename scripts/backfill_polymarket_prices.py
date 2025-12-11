#!/usr/bin/env python3
"""
Backfill polymarket_prices table with historical BTC 15-minute UP/DOWN share prices.

Fetches price history for all 15-minute windows in the past N days.
Polymarket retains price history for approximately 28-30 days.
"""

import time
import json
from datetime import datetime, timezone
from decimal import Decimal
import requests
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# Database configuration
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "database": "polymarket",
}

# Polymarket API
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# Rate limiting
REQUEST_DELAY = 0.1  # seconds between requests
MAX_WORKERS = 3  # parallel workers for fetching


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def get_existing_windows(conn):
    """Get list of windows already in database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT window_timestamp FROM polymarket_prices
    """)
    result = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return result


def get_window_timestamps(days_back: int = 28):
    """Generate all 15-minute window timestamps for the past N days."""
    now = int(time.time())
    current_window = (now // 900) * 900  # Current window start

    # Go back N days
    start_window = current_window - (days_back * 24 * 60 * 60)

    windows = []
    ts = start_window
    while ts <= current_window:
        windows.append(ts)
        ts += 900  # 15 minutes

    return windows


def fetch_market_tokens(window_timestamp: int) -> dict | None:
    """Fetch UP/DOWN token IDs for a specific window."""
    slug = f"btc-updown-15m-{window_timestamp}"
    url = f"{GAMMA_API}/events?slug={slug}"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        events = resp.json()

        if not events or not events[0].get("markets"):
            return None

        market = events[0]["markets"][0]
        clob_token_ids = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))

        if len(clob_token_ids) < 2 or len(outcomes) < 2:
            return None

        # Match tokens to outcomes
        up_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "up"), 0)
        down_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)

        return {
            "up_token": clob_token_ids[up_idx],
            "down_token": clob_token_ids[down_idx],
            "slug": slug,
        }

    except Exception as e:
        return None


def fetch_price_history(token_id: str) -> list:
    """Fetch price history for a token."""
    url = f"{CLOB_API}/prices-history"
    params = {
        "market": token_id,
        "interval": "max",
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("history", [])
    except Exception as e:
        return []


def fetch_window_data(window_timestamp: int) -> dict | None:
    """Fetch all data for a single window."""
    # Get token IDs
    tokens = fetch_market_tokens(window_timestamp)
    if not tokens:
        return None

    time.sleep(REQUEST_DELAY)

    # Fetch price histories
    up_history = fetch_price_history(tokens["up_token"])
    time.sleep(REQUEST_DELAY)

    down_history = fetch_price_history(tokens["down_token"])
    time.sleep(REQUEST_DELAY)

    if not up_history and not down_history:
        return None

    return {
        "window_timestamp": window_timestamp,
        "up_token": tokens["up_token"],
        "down_token": tokens["down_token"],
        "up_history": up_history,
        "down_history": down_history,
    }


def insert_window_prices(conn, window_data: dict) -> int:
    """Insert price data for a window."""
    if not window_data:
        return 0

    cursor = conn.cursor()
    data = []

    window_ts = window_data["window_timestamp"]

    # Add UP prices
    for point in window_data["up_history"]:
        ts = datetime.fromtimestamp(point["t"], tz=timezone.utc)
        data.append((
            window_ts,
            "UP",
            window_data["up_token"],
            ts,
            Decimal(str(point["p"])),
        ))

    # Add DOWN prices
    for point in window_data["down_history"]:
        ts = datetime.fromtimestamp(point["t"], tz=timezone.utc)
        data.append((
            window_ts,
            "DOWN",
            window_data["down_token"],
            ts,
            Decimal(str(point["p"])),
        ))

    if not data:
        cursor.close()
        return 0

    query = """
        INSERT INTO polymarket_prices
        (window_timestamp, token_type, token_id, timestamp, price)
        VALUES %s
        ON CONFLICT (window_timestamp, token_type, timestamp) DO NOTHING
    """

    execute_values(cursor, query, data, page_size=1000)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()

    return inserted


def process_window(window_ts: int, existing_windows: set) -> tuple:
    """Process a single window - fetch and return data."""
    if window_ts in existing_windows:
        return (window_ts, None, "skipped")

    data = fetch_window_data(window_ts)
    if data:
        total_points = len(data.get("up_history", [])) + len(data.get("down_history", []))
        return (window_ts, data, f"fetched {total_points} points")
    else:
        return (window_ts, None, "no data")


def get_stats(conn):
    """Get database statistics."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(DISTINCT window_timestamp) as num_windows,
            COUNT(*) as total_rows,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(CASE WHEN token_type = 'UP' THEN 1 END) as up_count,
            COUNT(CASE WHEN token_type = 'DOWN' THEN 1 END) as down_count
        FROM polymarket_prices
    """)
    result = cursor.fetchone()
    cursor.close()
    return {
        "num_windows": result[0],
        "total_rows": result[1],
        "earliest": result[2],
        "latest": result[3],
        "up_count": result[4],
        "down_count": result[5],
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill Polymarket price history")
    parser.add_argument("--days", type=int, default=28, help="Days to backfill (default: 28)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Parallel workers (default: 3)")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip windows already in DB")
    args = parser.parse_args()

    print("=" * 70)
    print("  BACKFILL POLYMARKET PRICES - BTC 15-MINUTE UP/DOWN MARKETS")
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
    print(f"\nFetching data with {args.workers} workers...")
    print("This may take a while due to rate limiting...\n")

    total_inserted = 0
    fetched_count = 0
    no_data_count = 0
    error_count = 0

    start_time = time.time()

    # Process sequentially to respect rate limits
    for i, window_ts in enumerate(windows_to_fetch):
        window_dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
        progress = (i + 1) / len(windows_to_fetch) * 100

        # Fetch data
        data = fetch_window_data(window_ts)

        if data:
            # Insert into database
            inserted = insert_window_prices(conn, data)
            total_inserted += inserted
            fetched_count += 1

            up_pts = len(data.get("up_history", []))
            down_pts = len(data.get("down_history", []))
            print(f"[{progress:5.1f}%] {window_dt.strftime('%Y-%m-%d %H:%M')} UTC - "
                  f"UP: {up_pts} pts, DOWN: {down_pts} pts, inserted: {inserted}")
        else:
            no_data_count += 1
            print(f"[{progress:5.1f}%] {window_dt.strftime('%Y-%m-%d %H:%M')} UTC - no data available")

        # Progress summary every 100 windows
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(windows_to_fetch) - i - 1) / rate / 60
            print(f"\n--- Progress: {i+1}/{len(windows_to_fetch)} windows, "
                  f"{total_inserted} rows inserted, ETA: {remaining:.1f} min ---\n")

    elapsed = time.time() - start_time

    # Final stats
    print()
    print("=" * 70)
    print("                         BACKFILL COMPLETE")
    print("=" * 70)
    print(f"  Time elapsed:         {elapsed/60:.1f} minutes")
    print(f"  Windows processed:    {len(windows_to_fetch)}")
    print(f"  Windows with data:    {fetched_count}")
    print(f"  Windows no data:      {no_data_count}")
    print(f"  Total rows inserted:  {total_inserted}")
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
    print(f"  Total price points:   {stats['total_rows']:,}")
    print(f"  UP token points:      {stats['up_count']:,}")
    print(f"  DOWN token points:    {stats['down_count']:,}")
    if stats['earliest']:
        print(f"  Earliest data:        {stats['earliest']}")
    if stats['latest']:
        print(f"  Latest data:          {stats['latest']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
