#!/usr/bin/env python3
"""
Backfill Chainlink BTC/USD historical data from The Graph subgraph.

Uses the chainlink-feeds package to fetch historical price data
and inserts it into the chainlink_prices PostgreSQL table.
"""

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values

# Try chainlink-feeds, fall back to direct GraphQL if needed
try:
    from chainlink_feeds import ChainlinkFeeds
    HAS_CHAINLINK_FEEDS = True
except ImportError:
    HAS_CHAINLINK_FEEDS = False
    import requests

# Database configuration
DB_CONFIG = {
    "host": "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com",
    "port": 5432,
    "user": "qoveryadmin",
    "password": "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp",
    "database": "polymarket",
}

# Chainlink BTC/USD on Polygon
POLYGON_BTC_USD_FEED = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# The Graph subgraph endpoint for Chainlink on Polygon
CHAINLINK_SUBGRAPH_URL = "https://gateway.thegraph.com/api/subgraphs/id/E7S5M9nYsVqUhvB8FXnVKmpw1BFXbtVY7LCwgjSkogUo"

# Alternative: Use the decentralized subgraph endpoint
# You may need an API key from The Graph Studio
SUBGRAPH_ENDPOINTS = [
    # Polygon Chainlink subgraph on decentralized network
    "https://api.thegraph.com/subgraphs/name/openpredict/chainlink-prices-subgraph",
    # Alternative endpoints
    "https://api.thegraph.com/subgraphs/name/tomafrench/chainlink",
]


def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def fetch_prices_graphql(feed_address: str, start_timestamp: int, end_timestamp: int):
    """
    Fetch historical prices directly from The Graph subgraph using GraphQL.
    """
    # GraphQL query to get price rounds
    query = """
    query GetPrices($feed: String!, $startTime: BigInt!, $endTime: BigInt!, $first: Int!, $skip: Int!) {
        prices(
            where: {
                feed: $feed,
                timestamp_gte: $startTime,
                timestamp_lte: $endTime
            },
            first: $first,
            skip: $skip,
            orderBy: timestamp,
            orderDirection: asc
        ) {
            id
            price
            timestamp
            roundId
        }
    }
    """

    all_prices = []
    skip = 0
    batch_size = 1000

    for endpoint in SUBGRAPH_ENDPOINTS:
        try:
            print(f"  Trying endpoint: {endpoint}")

            while True:
                variables = {
                    "feed": feed_address.lower(),
                    "startTime": str(start_timestamp),
                    "endTime": str(end_timestamp),
                    "first": batch_size,
                    "skip": skip,
                }

                response = requests.post(
                    endpoint,
                    json={"query": query, "variables": variables},
                    timeout=30,
                )

                if response.status_code != 200:
                    print(f"    HTTP {response.status_code}: {response.text[:200]}")
                    break

                data = response.json()

                if "errors" in data:
                    print(f"    GraphQL errors: {data['errors']}")
                    break

                prices = data.get("data", {}).get("prices", [])

                if not prices:
                    break

                all_prices.extend(prices)
                print(f"    Fetched {len(all_prices)} prices so far...")

                if len(prices) < batch_size:
                    break

                skip += batch_size
                time.sleep(0.2)  # Rate limiting

            if all_prices:
                return all_prices

        except Exception as e:
            print(f"    Error with endpoint: {e}")
            continue

    return all_prices


def fetch_prices_rpc_iterative(days_back: int = 180):
    """
    Fetch prices by iterating through RPC calls.
    This is a fallback if subgraph doesn't work.
    """
    from web3 import Web3

    POLYGON_RPC = "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))

    # ABI for getRoundData
    ABI = [
        {
            "inputs": [],
            "name": "latestRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"name": "_roundId", "type": "uint80"}],
            "name": "getRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function"
        },
    ]

    contract = w3.eth.contract(
        address=Web3.to_checksum_address(POLYGON_BTC_USD_FEED),
        abi=ABI
    )

    decimals = contract.functions.decimals().call()
    latest = contract.functions.latestRoundData().call()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    cutoff_ts = int(cutoff.timestamp())

    prices = []
    round_id = latest[0]
    phase_id = round_id >> 64
    agg_round = round_id & 0xFFFFFFFFFFFFFFFF

    consecutive_failures = 0

    print(f"  Starting from round {round_id} (phase {phase_id})")

    while consecutive_failures < 500:
        try:
            data = contract.functions.getRoundData(round_id).call()
            rid, answer, started_at, updated_at, _ = data

            if answer > 0 and updated_at > 0:
                consecutive_failures = 0

                if updated_at < cutoff_ts:
                    print(f"  Reached cutoff at {datetime.fromtimestamp(updated_at, tz=timezone.utc)}")
                    break

                prices.append({
                    "roundId": str(rid),
                    "price": str(answer),
                    "timestamp": str(updated_at),
                })

                if len(prices) % 500 == 0:
                    dt = datetime.fromtimestamp(updated_at, tz=timezone.utc)
                    print(f"    Fetched {len(prices)} prices, current: {dt}")
            else:
                consecutive_failures += 1
        except Exception:
            consecutive_failures += 1

        # Decrement round
        agg_round -= 1
        if agg_round < 1:
            phase_id -= 1
            if phase_id < 1:
                break
            agg_round = 100000
            print(f"  Switching to phase {phase_id}")
            consecutive_failures = 0

        round_id = (phase_id << 64) | agg_round

        if len(prices) % 100 == 0:
            time.sleep(0.05)

    return prices


def insert_prices(conn, prices, decimals=8):
    """Insert prices into chainlink_prices table."""
    if not prices:
        print("No prices to insert")
        return 0

    cursor = conn.cursor()

    # Prepare data
    data = []
    for p in prices:
        try:
            # Parse price (divide by 10^decimals)
            raw_price = int(p["price"])
            price = Decimal(raw_price) / Decimal(10 ** decimals)

            # Parse timestamp
            ts = int(p["timestamp"])
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)

            # Round ID for traceability
            round_id = Decimal(p.get("roundId", "0"))

            data.append((
                "BTCUSD",      # symbol
                dt,            # timestamp
                price,         # open_price
                price,         # high_price
                price,         # low_price
                price,         # close_price
                Decimal(0),    # volume
                Decimal(0),    # quote_volume
                0,             # num_trades
                round_id,      # round_id
            ))
        except Exception as e:
            print(f"  Skipping invalid price: {e}")
            continue

    if not data:
        print("No valid prices to insert")
        return 0

    # Deduplicate by timestamp (keep first occurrence)
    seen_timestamps = set()
    unique_data = []
    for row in data:
        ts = row[1]
        if ts not in seen_timestamps:
            seen_timestamps.add(ts)
            unique_data.append(row)

    print(f"  {len(data)} total, {len(unique_data)} unique timestamps")

    # Insert with upsert
    query = """
        INSERT INTO chainlink_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price,
         volume, quote_volume, num_trades, round_id)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            close_price = EXCLUDED.close_price,
            round_id = EXCLUDED.round_id
    """

    execute_values(cursor, query, unique_data, page_size=1000)
    conn.commit()
    cursor.close()

    return len(unique_data)


def get_stats(conn):
    """Get database statistics."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            MIN(close_price) as min_price,
            MAX(close_price) as max_price
        FROM chainlink_prices
        WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()
    cursor.close()
    return result


def main():
    print("=" * 65)
    print("    CHAINLINK BTC/USD HISTORICAL DATA BACKFILL")
    print("=" * 65)
    print()

    # Calculate time range (6 months back)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=180)

    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())

    print(f"Time range: {start_time.date()} to {end_time.date()}")
    print(f"Timestamps: {start_ts} to {end_ts}")
    print()

    # Try GraphQL subgraph first
    print("Method 1: Fetching from The Graph subgraph...")
    prices = fetch_prices_graphql(POLYGON_BTC_USD_FEED, start_ts, end_ts)

    if not prices:
        print("\nMethod 2: Falling back to RPC iteration...")
        prices = fetch_prices_rpc_iterative(days_back=180)

    if not prices:
        print("\nNo prices fetched from any source!")
        return

    print(f"\nTotal prices fetched: {len(prices)}")

    # Sort by timestamp
    prices.sort(key=lambda x: int(x["timestamp"]))

    if prices:
        first_ts = datetime.fromtimestamp(int(prices[0]["timestamp"]), tz=timezone.utc)
        last_ts = datetime.fromtimestamp(int(prices[-1]["timestamp"]), tz=timezone.utc)
        print(f"Date range: {first_ts} to {last_ts}")

    # Insert into database
    print("\nConnecting to database...")
    conn = get_db_connection()

    print("Inserting prices...")
    count = insert_prices(conn, prices)
    print(f"Inserted {count} records")

    # Show stats
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
