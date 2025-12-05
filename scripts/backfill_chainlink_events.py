#!/usr/bin/env python3
"""
Backfill Chainlink BTC/USD historical data by querying AnswerUpdated events.

This approach queries blockchain events which are preserved in archive nodes,
bypassing the phase boundary issues with getRoundData.
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

# Chainlink BTC/USD on Polygon - the aggregator proxy
POLYGON_BTC_USD_PROXY = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# AnswerUpdated event signature: AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"

# RPC endpoints with archive access
ARCHIVE_RPCS = [
    "https://wider-clean-scion.matic.quiknode.pro/147dd05f2eb43a0db2a87bb0a2bbeaf13780fc71/",  # QuickNode
    "https://polygon-mainnet.g.alchemy.com/v2/demo",  # Alchemy demo
    "https://rpc.ankr.com/polygon",                   # Ankr free
]


def get_working_rpc():
    """Find a working RPC endpoint."""
    for rpc in ARCHIVE_RPCS:
        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }, timeout=10)
            if resp.status_code == 200 and "result" in resp.json():
                print(f"  Using RPC: {rpc}")
                return rpc
        except:
            continue
    raise Exception("No working RPC found")


def get_block_by_timestamp(rpc: str, target_timestamp: int) -> int:
    """Binary search to find the block number closest to a timestamp."""
    # Get current block
    resp = requests.post(rpc, json={
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }, timeout=10)
    high = int(resp.json()["result"], 16)
    low = 1

    # Get current block timestamp to estimate
    resp = requests.post(rpc, json={
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(high), False],
        "id": 1
    }, timeout=10)
    current_ts = int(resp.json()["result"]["timestamp"], 16)

    # Polygon has ~2 second blocks
    blocks_back = (current_ts - target_timestamp) // 2
    estimated_block = max(1, high - blocks_back)

    # Binary search refinement
    low = max(1, estimated_block - 1000000)
    high = min(high, estimated_block + 100000)

    print(f"  Searching for block at timestamp {target_timestamp}...")
    print(f"  Initial range: {low} to {high}")

    while low < high:
        mid = (low + high) // 2

        resp = requests.post(rpc, json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(mid), False],
            "id": 1
        }, timeout=10)

        result = resp.json().get("result")
        if not result:
            low = mid + 1
            continue

        block_ts = int(result["timestamp"], 16)

        if block_ts < target_timestamp:
            low = mid + 1
        else:
            high = mid

        time.sleep(0.1)

    print(f"  Found block {low}")
    return low


def fetch_events_in_range(rpc: str, from_block: int, to_block: int, contract: str) -> list:
    """Fetch AnswerUpdated events in a block range."""
    events = []

    # QuickNode paid plan: 10,000 blocks max per query
    chunk_size = 9999  # Stay just under limit
    current = from_block
    total_blocks = to_block - from_block
    query_count = 0

    print(f"    Total queries needed: ~{total_blocks // chunk_size + 1}")

    while current <= to_block:
        end_block = min(current + chunk_size - 1, to_block)
        query_count += 1

        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [{
                    "fromBlock": hex(current),
                    "toBlock": hex(end_block),
                    "address": contract,
                    "topics": [ANSWER_UPDATED_TOPIC]
                }],
                "id": query_count
            }, timeout=120)

            result = resp.json()

            if "error" in result:
                error_msg = str(result.get("error", {}))
                print(f"\n    Error at block {current}: {error_msg[:100]}")
                # Skip this chunk and continue
                current = end_block + 1
                time.sleep(1)
                continue

            logs = result.get("result", [])
            events.extend(logs)

            progress = (current - from_block) / total_blocks * 100
            print(f"    Query {query_count}: {progress:.1f}% done, {len(events)} events found", end="\r")

        except requests.exceptions.Timeout:
            print(f"\n    Timeout at block {current}, retrying...")
            time.sleep(2)
            continue
        except Exception as e:
            print(f"\n    Exception: {e}")
            current = end_block + 1
            continue

        current = end_block + 1
        time.sleep(0.15)  # Rate limiting - ~6 req/sec

    print()
    return events


def parse_events(events: list, decimals: int = 8) -> list:
    """Parse AnswerUpdated events into price records."""
    prices = []

    for event in events:
        try:
            # Topics: [event_sig, current (price), roundId]
            # Data: updatedAt

            # Price is in topics[1] (indexed int256)
            price_hex = event["topics"][1]
            price_raw = int(price_hex, 16)
            # Handle signed int256
            if price_raw > 2**255:
                price_raw -= 2**256
            price = Decimal(price_raw) / Decimal(10 ** decimals)

            # Round ID in topics[2]
            round_id = int(event["topics"][2], 16)

            # Timestamp in data
            data = event["data"]
            timestamp = int(data, 16)

            # Block number for reference
            block_num = int(event["blockNumber"], 16)

            if price > 0 and timestamp > 0:
                prices.append({
                    "price": price,
                    "timestamp": timestamp,
                    "round_id": round_id,
                    "block": block_num,
                })
        except Exception as e:
            continue

    return prices


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def insert_prices(conn, prices):
    """Insert prices into chainlink_prices table."""
    if not prices:
        return 0

    cursor = conn.cursor()

    data = []
    for p in prices:
        dt = datetime.fromtimestamp(p["timestamp"], tz=timezone.utc)
        data.append((
            "BTCUSD",
            dt,
            p["price"],
            p["price"],
            p["price"],
            p["price"],
            Decimal(0),
            Decimal(0),
            0,
            Decimal(p["round_id"]),
        ))

    # Deduplicate by timestamp
    seen = set()
    unique = []
    for row in data:
        if row[1] not in seen:
            seen.add(row[1])
            unique.append(row)

    query = """
        INSERT INTO chainlink_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price,
         volume, quote_volume, num_trades, round_id)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            close_price = EXCLUDED.close_price,
            round_id = EXCLUDED.round_id
    """

    execute_values(cursor, query, unique, page_size=1000)
    conn.commit()
    cursor.close()

    return len(unique)


def get_stats(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp),
               MIN(close_price), MAX(close_price)
        FROM chainlink_prices WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()
    cursor.close()
    return result


def main():
    print("=" * 65)
    print("  CHAINLINK BTC/USD BACKFILL VIA EVENTS")
    print("=" * 65)
    print()

    # Get working RPC
    print("Finding working RPC...")
    rpc = get_working_rpc()

    # Calculate block range for 6 months
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=180)

    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())

    print(f"\nTarget time range: {start_time.date()} to {end_time.date()}")

    # Find block numbers
    print("\nFinding block range...")
    from_block = get_block_by_timestamp(rpc, start_ts)
    to_block = get_block_by_timestamp(rpc, end_ts)

    print(f"Block range: {from_block:,} to {to_block:,} ({to_block - from_block:,} blocks)")

    # Fetch events
    print(f"\nFetching AnswerUpdated events...")
    events = fetch_events_in_range(rpc, from_block, to_block, POLYGON_BTC_USD_PROXY)

    print(f"\nTotal events fetched: {len(events)}")

    if not events:
        print("No events found!")
        return

    # Parse events
    print("Parsing events...")
    prices = parse_events(events)
    print(f"Parsed {len(prices)} valid prices")

    if prices:
        prices.sort(key=lambda x: x["timestamp"])
        first = datetime.fromtimestamp(prices[0]["timestamp"], tz=timezone.utc)
        last = datetime.fromtimestamp(prices[-1]["timestamp"], tz=timezone.utc)
        print(f"Date range: {first} to {last}")
        print(f"Price range: ${min(p['price'] for p in prices):,.2f} to ${max(p['price'] for p in prices):,.2f}")

    # Insert into database
    print("\nConnecting to database...")
    conn = get_db_connection()

    print("Inserting prices...")
    count = insert_prices(conn, prices)
    print(f"Inserted {count} records")

    # Stats
    stats = get_stats(conn)
    print()
    print("=" * 65)
    print("                  DATABASE STATS")
    print("=" * 65)
    print(f"  Total rows:   {stats[0]:,}")
    if stats[1]:
        print(f"  Earliest:     {stats[1]}")
        print(f"  Latest:       {stats[2]}")
        print(f"  Price range:  ${stats[3]:,.2f} to ${stats[4]:,.2f}")
    print("=" * 65)

    conn.close()


if __name__ == "__main__":
    main()
