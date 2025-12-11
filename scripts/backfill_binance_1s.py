#!/usr/bin/env python3
"""
Backfill 6 months of Binance BTCUSDT 1-second kline data into PostgreSQL.

Usage:
    python3 scripts/backfill_binance_1s.py

Features:
- Fetches 1-second klines from Binance API
- Uses ON CONFLICT DO NOTHING to skip duplicates
- Shows progress and can be resumed
- Rate limited to avoid API bans
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
import time
import sys

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
INTERVAL = "1s"  # 1-second klines
LIMIT = 1000  # Max per request

# Time range: 6 months back from now (timezone-aware UTC)
END_TIME = datetime.now(timezone.utc)
START_TIME = END_TIME - timedelta(days=180)


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
                # Rate limited - wait and retry
                print("Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching klines: {e}, attempt {attempt + 1}/3")
            time.sleep(5)

    return []


def parse_kline(kline: list) -> tuple:
    """Parse Binance kline to database row."""
    # Kline format: [open_time, open, high, low, close, volume, close_time,
    #                quote_volume, num_trades, taker_buy_base, taker_buy_quote, ignore]
    timestamp = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
    return (
        SYMBOL,
        timestamp,
        float(kline[1]),  # open
        float(kline[2]),  # high
        float(kline[3]),  # low
        float(kline[4]),  # close
        float(kline[5]),  # volume
        float(kline[7]),  # quote_volume
        int(kline[8])     # num_trades
    )


def insert_batch(conn, rows: list) -> int:
    """Insert batch of rows, skipping duplicates."""
    if not rows:
        return 0

    sql = """
        INSERT INTO binance_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, num_trades)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def get_earliest_timestamp(conn) -> datetime:
    """Get the earliest timestamp we have in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(timestamp) FROM binance_prices WHERE symbol = %s
        """, (SYMBOL,))
        result = cur.fetchone()[0]
        return result if result else END_TIME


def find_gaps(conn, start_time, end_time) -> list:
    """Find gaps in the data where we're missing more than 1 hour."""
    with conn.cursor() as cur:
        # Get distinct dates with data
        cur.execute("""
            SELECT DATE(timestamp) as date, COUNT(*) as rows
            FROM binance_prices
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY DATE(timestamp)
            ORDER BY date
        """, (SYMBOL, start_time, end_time))

        dates_with_data = {row[0]: row[1] for row in cur.fetchall()}

    gaps = []
    current = start_time.date()
    end = end_time.date()

    while current <= end:
        if current not in dates_with_data or dates_with_data[current] < 80000:  # Less than ~90% coverage
            # Find end of gap
            gap_start = datetime.combine(current, datetime.min.time()).replace(tzinfo=timezone.utc)
            gap_end = gap_start + timedelta(days=1)
            gaps.append((gap_start, gap_end))
        current += timedelta(days=1)

    return gaps


def main():
    print(f"Backfilling Binance {SYMBOL} 1-second data")
    print(f"Target range: {START_TIME} to {END_TIME}")
    print(f"That's approximately {(END_TIME - START_TIME).days} days = ~{(END_TIME - START_TIME).total_seconds():,.0f} rows")
    print()

    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected to database")

    # Find gaps in the data
    print("Scanning for gaps...")
    gaps = find_gaps(conn, START_TIME, END_TIME)

    if not gaps:
        print("No gaps to fill - data is complete!")
        conn.close()
        return

    print(f"Found {len(gaps)} days with missing/incomplete data")
    print(f"First gap: {gaps[0][0].date()}, Last gap: {gaps[-1][0].date()}")

    # Process each gap
    total_days = len(gaps)
    current_start = gaps[0][0]
    target_end = gaps[-1][1]

    print(f"Backfilling from {current_start.date()} to {target_end.date()}")
    print(f"Estimated rows to fetch: {(target_end - current_start).total_seconds():,.0f}")
    print()

    total_fetched = 0
    total_inserted = 0
    start_time = time.time()

    while current_start < target_end:
        # Calculate batch end (1000 seconds forward)
        batch_end = min(current_start + timedelta(seconds=LIMIT), target_end)

        # Fetch from Binance
        start_ms = int(current_start.timestamp() * 1000)
        end_ms = int(batch_end.timestamp() * 1000)

        klines = fetch_klines(start_ms, end_ms)

        if not klines:
            print(f"No data returned for {current_start}, skipping...")
            current_start = batch_end
            time.sleep(0.1)
            continue

        # Parse and insert
        rows = [parse_kline(k) for k in klines]
        inserted = insert_batch(conn, rows)

        total_fetched += len(rows)
        total_inserted += inserted

        # Progress
        elapsed = time.time() - start_time
        progress = (current_start - START_TIME).total_seconds() / (target_end - START_TIME).total_seconds() * 100
        rows_per_sec = total_fetched / elapsed if elapsed > 0 else 0
        remaining_rows = (target_end - current_start).total_seconds()
        eta_seconds = remaining_rows / rows_per_sec if rows_per_sec > 0 else 0

        print(f"\r[{progress:5.1f}%] {current_start.strftime('%Y-%m-%d %H:%M')} | "
              f"Fetched: {total_fetched:,} | Inserted: {total_inserted:,} | "
              f"Rate: {rows_per_sec:.0f}/s | ETA: {eta_seconds/60:.0f}min", end="")

        # Move forward
        if klines:
            # Jump to after the last kline we received
            last_timestamp = datetime.fromtimestamp(klines[-1][0] / 1000, tz=timezone.utc)
            current_start = last_timestamp + timedelta(seconds=1)
        else:
            current_start = batch_end

        # Rate limiting - Binance allows ~1200 requests/min, be conservative
        time.sleep(0.05)

    print()
    print()
    print("=" * 60)
    print(f"COMPLETE!")
    print(f"Total fetched: {total_fetched:,}")
    print(f"Total inserted: {total_inserted:,}")
    print(f"Duplicates skipped: {total_fetched - total_inserted:,}")
    print(f"Time elapsed: {(time.time() - start_time)/60:.1f} minutes")
    print("=" * 60)

    # Verify final count
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
