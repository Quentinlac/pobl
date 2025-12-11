#!/usr/bin/env python3
"""
Sync Polymarket prices - fetches recent BTC 15-minute market prices.

This script is designed to run hourly to keep the polymarket_prices table
up to date with the latest data. It only fetches windows from the last
24 hours that are not already in the database.

Usage:
    python3 scripts/sync_polymarket_prices.py [--hours 24]
"""

import time
import json
from datetime import datetime, timezone
from decimal import Decimal
import requests
import psycopg2
from psycopg2.extras import execute_values
import argparse
import os

# Database configuration - from environment or defaults
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "user": os.environ.get("DB_USER", "qoveryadmin"),
    "password": os.environ.get("DB_PASSWORD", "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp"),
    "database": os.environ.get("DB_NAME", "polymarket"),
}

# Polymarket API
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# Rate limiting
REQUEST_DELAY = 0.1  # seconds between requests


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def get_existing_windows(conn, since_timestamp: int) -> set:
    """Get list of windows already in database since given timestamp."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT window_timestamp
        FROM polymarket_prices
        WHERE window_timestamp >= %s
    """, (since_timestamp,))
    result = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return result


def get_window_timestamps(hours_back: int = 24) -> list:
    """Generate all 15-minute window timestamps for the past N hours."""
    now = int(time.time())
    current_window = (now // 900) * 900  # Current window start

    # Go back N hours
    start_window = current_window - (hours_back * 60 * 60)

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


def main():
    parser = argparse.ArgumentParser(description="Sync Polymarket prices (hourly job)")
    parser.add_argument("--hours", type=int, default=24, help="Hours to sync (default: 24)")
    args = parser.parse_args()

    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting Polymarket price sync...")

    # Connect to database
    conn = get_db_connection()

    # Generate window timestamps
    windows = get_window_timestamps(args.hours)
    start_ts = min(windows) if windows else 0

    # Get existing windows
    existing_windows = get_existing_windows(conn, start_ts)

    # Filter out existing windows
    windows_to_fetch = [w for w in windows if w not in existing_windows]

    print(f"  Windows in range: {len(windows)}")
    print(f"  Already in DB: {len(existing_windows)}")
    print(f"  To fetch: {len(windows_to_fetch)}")

    if not windows_to_fetch:
        print("  No new windows to fetch!")
        conn.close()
        return

    # Fetch and insert
    total_inserted = 0
    fetched_count = 0

    for window_ts in windows_to_fetch:
        data = fetch_window_data(window_ts)

        if data:
            inserted = insert_window_prices(conn, data)
            total_inserted += inserted
            fetched_count += 1

            window_dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
            print(f"  Synced {window_dt.strftime('%Y-%m-%d %H:%M')} UTC - {inserted} rows")

    print(f"\n  Sync complete: {fetched_count} windows, {total_inserted} rows inserted")
    conn.close()


if __name__ == "__main__":
    main()
