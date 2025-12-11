#!/usr/bin/env python3
"""
Fast parallel backfill of Binance BTCUSDT 1-second kline data.
Uses concurrent requests to speed up data fetching.
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Database connection
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "polymarket",
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "sslmode": "require"
}

# Binance API
BINANCE_API = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1s"
LIMIT = 1000

# Parallelism settings
NUM_WORKERS = 10  # Parallel API requests
BATCH_SIZE = 5000  # Rows per DB insert

# Time range
END_TIME = datetime.now(timezone.utc)
START_TIME = END_TIME - timedelta(days=180)

# Thread-safe counters
lock = threading.Lock()
total_fetched = 0
total_inserted = 0


def fetch_klines(start_ms: int, end_ms: int) -> list:
    """Fetch klines from Binance API."""
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT
    }

    for attempt in range(3):
        try:
            response = requests.get(BINANCE_API, params=params, timeout=30)
            if response.status_code == 429:
                time.sleep(30)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            continue
    return []


def fetch_day(date: datetime) -> list:
    """Fetch all klines for a single day."""
    global total_fetched

    rows = []
    current = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
    end = current + timedelta(days=1)

    while current < end:
        start_ms = int(current.timestamp() * 1000)
        end_ms = int((current + timedelta(seconds=LIMIT)).timestamp() * 1000)

        klines = fetch_klines(start_ms, end_ms)

        if klines:
            for k in klines:
                ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
                rows.append((
                    SYMBOL, ts,
                    float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                    float(k[5]), float(k[7]), int(k[8])
                ))
            last_ts = datetime.fromtimestamp(klines[-1][0] / 1000, tz=timezone.utc)
            current = last_ts + timedelta(seconds=1)
        else:
            current += timedelta(seconds=LIMIT)

        time.sleep(0.02)  # Small delay to avoid rate limits

    with lock:
        total_fetched += len(rows)

    return rows


def insert_batch(conn, rows: list) -> int:
    """Insert batch of rows."""
    if not rows:
        return 0

    sql = """
        INSERT INTO binance_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, num_trades)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def find_missing_days(conn, start_time, end_time) -> list:
    """Find days with missing or incomplete data."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(timestamp) as date, COUNT(*) as rows
            FROM binance_prices
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY DATE(timestamp)
        """, (SYMBOL, start_time, end_time))

        existing = {row[0]: row[1] for row in cur.fetchall()}

    missing = []
    current = start_time.date()
    end = end_time.date()

    while current <= end:
        if current not in existing or existing[current] < 80000:
            missing.append(current)
        current += timedelta(days=1)

    return missing


def main():
    print(f"Fast Parallel Backfill - Binance {SYMBOL} 1-second data")
    print(f"Target range: {START_TIME.date()} to {END_TIME.date()}")
    print(f"Using {NUM_WORKERS} parallel workers")
    print()

    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected to database")

    # Find missing days
    print("Scanning for gaps...")
    missing_days = find_missing_days(conn, START_TIME, END_TIME)

    if not missing_days:
        print("No gaps to fill!")
        conn.close()
        return

    print(f"Found {len(missing_days)} days to backfill")
    print(f"First: {missing_days[0]}, Last: {missing_days[-1]}")
    print()

    global total_fetched, total_inserted
    start_time = time.time()
    completed_days = 0
    pending_rows = []

    # Process days in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(fetch_day, day): day for day in missing_days}

        for future in as_completed(futures):
            day = futures[future]
            try:
                rows = future.result()
                pending_rows.extend(rows)
                completed_days += 1

                # Insert when we have enough rows
                if len(pending_rows) >= BATCH_SIZE:
                    inserted = insert_batch(conn, pending_rows)
                    total_inserted += inserted
                    pending_rows = []

                # Progress
                elapsed = time.time() - start_time
                pct = completed_days / len(missing_days) * 100
                rate = total_fetched / elapsed if elapsed > 0 else 0
                remaining = (len(missing_days) - completed_days) * (elapsed / completed_days) if completed_days > 0 else 0

                print(f"\r[{pct:5.1f}%] Day {completed_days}/{len(missing_days)} ({day}) | "
                      f"Fetched: {total_fetched:,} | Inserted: {total_inserted:,} | "
                      f"Rate: {rate:.0f}/s | ETA: {remaining/60:.0f}min", end="", flush=True)

            except Exception as e:
                print(f"\nError processing {day}: {e}")

    # Insert remaining rows
    if pending_rows:
        inserted = insert_batch(conn, pending_rows)
        total_inserted += inserted

    print()
    print()
    print("=" * 60)
    print("COMPLETE!")
    print(f"Total fetched: {total_fetched:,}")
    print(f"Total inserted: {total_inserted:,}")
    print(f"Time: {(time.time() - start_time)/60:.1f} minutes")
    print("=" * 60)

    # Verify
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM binance_prices WHERE symbol = %s
        """, (SYMBOL,))
        count, min_ts, max_ts = cur.fetchone()
        print(f"\nDatabase now has {count:,} rows")
        print(f"Range: {min_ts} to {max_ts}")

    conn.close()


if __name__ == "__main__":
    main()
