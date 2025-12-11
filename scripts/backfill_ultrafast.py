#!/usr/bin/env python3
"""
Ultra-fast async backfill using aiohttp for maximum parallelism.
Target: Complete 6 months in ~20 minutes.
"""

import asyncio
import aiohttp
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
import time
import sys

# Database
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "polymarket",
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "sslmode": "require"
}

# Binance
BINANCE_API = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1s"
LIMIT = 1000

# Parallelism - aggressive but within Binance limits
MAX_CONCURRENT = 50  # Concurrent API requests
BATCH_INSERT_SIZE = 10000  # Rows per DB insert

# Time range
END_TIME = datetime.now(timezone.utc)
START_TIME = END_TIME - timedelta(days=180)

# Stats
stats = {"fetched": 0, "inserted": 0, "errors": 0}


async def fetch_chunk(session: aiohttp.ClientSession, start_ms: int, semaphore: asyncio.Semaphore) -> list:
    """Fetch a single 1000-second chunk."""
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": start_ms + LIMIT * 1000,
        "limit": LIMIT
    }

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(BINANCE_API, params=params, timeout=30) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(10)
                        continue
                    if resp.status == 200:
                        data = await resp.json()
                        return data
                    await asyncio.sleep(1)
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
        stats["errors"] += 1
        return []


def parse_klines(klines: list) -> list:
    """Parse klines to DB rows."""
    rows = []
    for k in klines:
        ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        rows.append((
            SYMBOL, ts,
            float(k[1]), float(k[2]), float(k[3]), float(k[4]),
            float(k[5]), float(k[7]), int(k[8])
        ))
    return rows


def insert_rows(conn, rows: list) -> int:
    """Insert rows to DB."""
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


def find_missing_chunks(conn, start_time, end_time) -> list:
    """Find all missing 1000-second chunks."""
    # Get existing data coverage per day
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(timestamp), COUNT(*)
            FROM binance_prices
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY DATE(timestamp)
        """, (SYMBOL, start_time, end_time))
        existing = {row[0]: row[1] for row in cur.fetchall()}

    # Generate all chunks for missing/incomplete days
    chunks = []
    current_day = start_time.date()
    end_day = end_time.date()

    while current_day <= end_day:
        # If day has < 80000 rows (allowing some tolerance), fetch it
        if current_day not in existing or existing[current_day] < 80000:
            day_start = datetime.combine(current_day, datetime.min.time()).replace(tzinfo=timezone.utc)
            # Generate 87 chunks per day (86400 / 1000)
            for i in range(87):
                chunk_start = day_start + timedelta(seconds=i * LIMIT)
                chunks.append(int(chunk_start.timestamp() * 1000))
        current_day += timedelta(days=1)

    return chunks


async def process_chunks(chunks: list, conn) -> None:
    """Process all chunks with maximum parallelism."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    pending_rows = []

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Process in batches to manage memory
        batch_size = 500  # chunks per batch
        total_chunks = len(chunks)

        for batch_start in range(0, total_chunks, batch_size):
            batch_end = min(batch_start + batch_size, total_chunks)
            batch_chunks = chunks[batch_start:batch_end]

            # Fetch all chunks in this batch concurrently
            tasks = [fetch_chunk(session, chunk_ms, semaphore) for chunk_ms in batch_chunks]
            results = await asyncio.gather(*tasks)

            # Parse results
            for klines in results:
                if klines:
                    rows = parse_klines(klines)
                    pending_rows.extend(rows)
                    stats["fetched"] += len(rows)

            # Insert when we have enough
            if len(pending_rows) >= BATCH_INSERT_SIZE:
                inserted = insert_rows(conn, pending_rows)
                stats["inserted"] += inserted
                pending_rows = []

            # Progress
            pct = batch_end / total_chunks * 100
            print(f"\r[{pct:5.1f}%] Chunks: {batch_end}/{total_chunks} | "
                  f"Fetched: {stats['fetched']:,} | Inserted: {stats['inserted']:,} | "
                  f"Errors: {stats['errors']}", end="", flush=True)

    # Insert remaining
    if pending_rows:
        inserted = insert_rows(conn, pending_rows)
        stats["inserted"] += inserted


def verify_no_gaps(conn, start_time, end_time) -> bool:
    """Verify there are no gaps in the data."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(timestamp) as date, COUNT(*) as rows
            FROM binance_prices
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY DATE(timestamp)
            ORDER BY date
        """, (SYMBOL, start_time, end_time))

        results = cur.fetchall()

    print("\n\nData coverage by day:")
    gaps = []
    current = start_time.date()
    end = end_time.date()
    date_counts = {row[0]: row[1] for row in results}

    while current <= end:
        count = date_counts.get(current, 0)
        if count < 80000:
            gaps.append((current, count))
        current += timedelta(days=1)

    if gaps:
        print(f"Found {len(gaps)} days with incomplete data:")
        for date, count in gaps[:10]:
            print(f"  {date}: {count:,} rows (expected ~86,400)")
        return False
    else:
        print("All days have complete data (80,000+ rows each)")
        return True


async def main():
    print("=" * 60)
    print("ULTRA-FAST BACKFILL - Binance BTCUSDT 1-second data")
    print(f"Target: {START_TIME.date()} to {END_TIME.date()}")
    print(f"Concurrent requests: {MAX_CONCURRENT}")
    print("=" * 60)
    print()

    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected to database")

    # Find missing chunks
    print("Scanning for missing data...")
    chunks = find_missing_chunks(conn, START_TIME, END_TIME)

    if not chunks:
        print("No missing data - checking for gaps anyway...")
        verify_no_gaps(conn, START_TIME, END_TIME)
        conn.close()
        return

    total_expected = len(chunks) * LIMIT
    print(f"Found {len(chunks):,} chunks to fetch (~{total_expected:,} rows)")
    print()

    start_time = time.time()

    # Process all chunks
    await process_chunks(chunks, conn)

    elapsed = time.time() - start_time
    rate = stats["fetched"] / elapsed if elapsed > 0 else 0

    print()
    print()
    print("=" * 60)
    print("COMPLETE!")
    print(f"Fetched: {stats['fetched']:,} rows")
    print(f"Inserted: {stats['inserted']:,} rows")
    print(f"Errors: {stats['errors']}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Rate: {rate:,.0f} rows/second")
    print("=" * 60)

    # Verify no gaps
    print("\nVerifying data completeness...")
    complete = verify_no_gaps(conn, START_TIME, END_TIME)

    if not complete:
        print("\nRe-running to fill gaps...")
        conn.close()
        # Recursively call to fill any remaining gaps
        await main()
    else:
        # Final count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM binance_prices WHERE symbol = %s", (SYMBOL,))
            count, min_ts, max_ts = cur.fetchone()
            print(f"\nFinal: {count:,} rows from {min_ts} to {max_ts}")
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
