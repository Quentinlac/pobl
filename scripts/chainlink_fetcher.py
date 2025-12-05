#!/usr/bin/env python3
"""
Chainlink BTC/USD Price Fetcher for Polygon

Fetches historical and live price data from Chainlink oracle on Polygon mainnet
and stores it in PostgreSQL with the same structure as binance_prices.

Usage:
    python chainlink_fetcher.py --historical --days 180  # Fetch 6 months of history
    python chainlink_fetcher.py --latest                  # Fetch latest prices (for cron)
"""

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from web3 import Web3

# ============================================================================
# Configuration
# ============================================================================

# Polygon RPC endpoints (free public RPCs)
POLYGON_RPCS = [
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://polygon-mainnet.public.blastapi.io",
    "https://polygon.llamarpc.com",
]

# Chainlink BTC/USD Price Feed on Polygon Mainnet
BTC_USD_FEED = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# AggregatorV3Interface ABI (minimal)
AGGREGATOR_ABI = [
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
    {
        "inputs": [],
        "name": "description",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "phaseId",
        "outputs": [{"name": "", "type": "uint16"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"name": "", "type": "uint16"}],
        "name": "phaseAggregators",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Database configuration
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "user": os.environ.get("DB_USER", "qoveryadmin"),
    "password": os.environ.get("DB_PASSWORD", "xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp"),
    "database": os.environ.get("DB_NAME", "polymarket"),
}

# ============================================================================
# Web3 Connection
# ============================================================================

def get_web3_connection():
    """Try multiple RPC endpoints until one works."""
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 30}))
            if w3.is_connected():
                print(f"Connected to {rpc}")
                return w3
        except Exception as e:
            print(f"Failed to connect to {rpc}: {e}")
            continue
    raise ConnectionError("Could not connect to any Polygon RPC")


def get_contract(w3):
    """Get the Chainlink price feed contract."""
    return w3.eth.contract(
        address=Web3.to_checksum_address(BTC_USD_FEED),
        abi=AGGREGATOR_ABI
    )


# ============================================================================
# Chainlink Data Fetching
# ============================================================================

def get_latest_round(contract):
    """Get the latest round data."""
    round_id, answer, started_at, updated_at, answered_in_round = contract.functions.latestRoundData().call()
    decimals = contract.functions.decimals().call()

    return {
        "round_id": round_id,
        "price": Decimal(answer) / Decimal(10 ** decimals),
        "started_at": datetime.fromtimestamp(started_at, tz=timezone.utc),
        "updated_at": datetime.fromtimestamp(updated_at, tz=timezone.utc),
    }


def get_round_data(contract, round_id, decimals):
    """Get data for a specific round."""
    try:
        rid, answer, started_at, updated_at, answered_in_round = contract.functions.getRoundData(round_id).call()

        # Skip invalid rounds (answer = 0 or timestamps = 0)
        if answer <= 0 or updated_at == 0:
            return None

        return {
            "round_id": rid,
            "price": Decimal(answer) / Decimal(10 ** decimals),
            "started_at": datetime.fromtimestamp(started_at, tz=timezone.utc),
            "updated_at": datetime.fromtimestamp(updated_at, tz=timezone.utc),
        }
    except Exception as e:
        # Round doesn't exist or is invalid
        return None


def fetch_historical_rounds(contract, days_back=180, batch_size=100):
    """
    Fetch historical round data going back N days.

    Chainlink round IDs are structured as:
    - Upper 16 bits: phase ID
    - Lower 64 bits: aggregator round ID within that phase

    We iterate backwards from the latest round.
    """
    decimals = contract.functions.decimals().call()
    latest = get_latest_round(contract)

    print(f"Latest round: {latest['round_id']}")
    print(f"Latest price: ${latest['price']:,.2f}")
    print(f"Latest update: {latest['updated_at']}")

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    print(f"Fetching data back to: {cutoff_date}")

    rounds = []
    current_round_id = latest["round_id"]
    consecutive_failures = 0
    max_consecutive_failures = 1000  # Skip large gaps

    # Extract phase info
    phase_id = current_round_id >> 64
    aggregator_round = current_round_id & 0xFFFFFFFFFFFFFFFF

    print(f"Current phase: {phase_id}, aggregator round: {aggregator_round}")

    while True:
        round_data = get_round_data(contract, current_round_id, decimals)

        if round_data:
            consecutive_failures = 0

            # Check if we've gone back far enough
            if round_data["updated_at"] < cutoff_date:
                print(f"\nReached cutoff date at round {current_round_id}")
                break

            rounds.append(round_data)

            if len(rounds) % 100 == 0:
                print(f"Fetched {len(rounds)} rounds, current date: {round_data['updated_at']}")
        else:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                # Try previous phase
                if phase_id > 1:
                    phase_id -= 1
                    # Start from a high aggregator round in the previous phase
                    aggregator_round = 100000
                    current_round_id = (phase_id << 64) | aggregator_round
                    print(f"\nSwitching to phase {phase_id}")
                    consecutive_failures = 0
                    continue
                else:
                    print(f"\nReached beginning of data at round {current_round_id}")
                    break

        # Decrement round ID
        aggregator_round -= 1
        if aggregator_round < 1:
            # Move to previous phase
            if phase_id > 1:
                phase_id -= 1
                aggregator_round = 100000  # Start high and let failures guide us
                print(f"\nMoving to phase {phase_id}")
            else:
                break

        current_round_id = (phase_id << 64) | aggregator_round

        # Rate limiting
        if len(rounds) % batch_size == 0:
            time.sleep(0.1)

    print(f"\nTotal rounds fetched: {len(rounds)}")
    return rounds


# ============================================================================
# Database Operations
# ============================================================================

def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"],
    )


def insert_prices(conn, rounds):
    """Insert price data into chainlink_prices table."""
    if not rounds:
        print("No data to insert")
        return 0

    # Prepare data for insertion
    # Since Chainlink only gives us a single price per update (not OHLCV),
    # we set open/high/low/close all to the same price
    data = []
    for r in rounds:
        data.append((
            "BTCUSD",  # symbol (Chainlink uses BTCUSD, not BTCUSDT)
            r["updated_at"],
            r["price"],  # open
            r["price"],  # high
            r["price"],  # low
            r["price"],  # close
            Decimal(0),  # volume (not available from Chainlink)
            Decimal(0),  # quote_volume
            0,           # num_trades
            r["round_id"],
        ))

    cursor = conn.cursor()

    # Use ON CONFLICT to handle duplicates
    query = """
        INSERT INTO chainlink_prices
        (symbol, timestamp, open_price, high_price, low_price, close_price,
         volume, quote_volume, num_trades, round_id)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            close_price = EXCLUDED.close_price,
            round_id = EXCLUDED.round_id
    """

    execute_values(cursor, query, data, page_size=1000)
    conn.commit()

    inserted = cursor.rowcount
    cursor.close()

    print(f"Inserted/updated {len(data)} price records")
    return len(data)


def get_latest_stored_timestamp(conn):
    """Get the most recent timestamp in the database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(timestamp) FROM chainlink_prices WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def get_stats(conn):
    """Get statistics about stored data."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(*) as total_rows,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            MIN(close_price) as min_price,
            MAX(close_price) as max_price
        FROM chainlink_prices
        WHERE symbol = 'BTCUSD'
    """)
    result = cursor.fetchone()
    cursor.close()

    return {
        "total_rows": result[0],
        "earliest": result[1],
        "latest": result[2],
        "min_price": result[3],
        "max_price": result[4],
    }


# ============================================================================
# Main Functions
# ============================================================================

def fetch_historical(days=180):
    """Fetch historical data and store in database."""
    print(f"=== Fetching {days} days of historical Chainlink BTC/USD data ===\n")

    w3 = get_web3_connection()
    contract = get_contract(w3)

    print(f"Contract: {BTC_USD_FEED}")
    print(f"Description: {contract.functions.description().call()}")
    print(f"Decimals: {contract.functions.decimals().call()}")
    print()

    rounds = fetch_historical_rounds(contract, days_back=days)

    if not rounds:
        print("No data fetched!")
        return

    # Sort by timestamp (oldest first)
    rounds.sort(key=lambda x: x["updated_at"])

    print(f"\nDate range: {rounds[0]['updated_at']} to {rounds[-1]['updated_at']}")
    print(f"Price range: ${min(r['price'] for r in rounds):,.2f} to ${max(r['price'] for r in rounds):,.2f}")

    # Insert into database
    conn = get_db_connection()
    try:
        insert_prices(conn, rounds)

        stats = get_stats(conn)
        print(f"\n=== Database Stats ===")
        print(f"Total rows: {stats['total_rows']:,}")
        print(f"Date range: {stats['earliest']} to {stats['latest']}")
        print(f"Price range: ${stats['min_price']:,.2f} to ${stats['max_price']:,.2f}")
    finally:
        conn.close()


def fetch_latest():
    """Fetch latest prices since last stored timestamp."""
    print("=== Fetching latest Chainlink BTC/USD data ===\n")

    conn = get_db_connection()
    latest_stored = get_latest_stored_timestamp(conn)

    if latest_stored:
        print(f"Last stored timestamp: {latest_stored}")
    else:
        print("No existing data, fetching last 24 hours")
        latest_stored = datetime.now(timezone.utc) - timedelta(days=1)

    w3 = get_web3_connection()
    contract = get_contract(w3)
    decimals = contract.functions.decimals().call()

    latest = get_latest_round(contract)
    print(f"Latest Chainlink price: ${latest['price']:,.2f} at {latest['updated_at']}")

    # Fetch rounds since last stored
    rounds = []
    current_round_id = latest["round_id"]
    consecutive_failures = 0

    while consecutive_failures < 100:
        round_data = get_round_data(contract, current_round_id, decimals)

        if round_data:
            consecutive_failures = 0

            if round_data["updated_at"] <= latest_stored:
                break

            rounds.append(round_data)
        else:
            consecutive_failures += 1

        # Decrement round ID
        phase_id = current_round_id >> 64
        aggregator_round = (current_round_id & 0xFFFFFFFFFFFFFFFF) - 1

        if aggregator_round < 1:
            break

        current_round_id = (phase_id << 64) | aggregator_round

    print(f"Found {len(rounds)} new rounds")

    if rounds:
        insert_prices(conn, rounds)

    stats = get_stats(conn)
    print(f"\n=== Database Stats ===")
    print(f"Total rows: {stats['total_rows']:,}")
    print(f"Latest: {stats['latest']}")

    conn.close()


def show_stats():
    """Show database statistics."""
    conn = get_db_connection()
    stats = get_stats(conn)
    conn.close()

    print("=== Chainlink Prices Database Stats ===")
    print(f"Total rows: {stats['total_rows']:,}")
    if stats['earliest']:
        print(f"Date range: {stats['earliest']} to {stats['latest']}")
        print(f"Price range: ${stats['min_price']:,.2f} to ${stats['max_price']:,.2f}")
    else:
        print("No data in database")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Chainlink BTC/USD Price Fetcher")
    parser.add_argument("--historical", action="store_true", help="Fetch historical data")
    parser.add_argument("--days", type=int, default=180, help="Days of history to fetch (default: 180)")
    parser.add_argument("--latest", action="store_true", help="Fetch latest prices since last update")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")

    args = parser.parse_args()

    if args.stats:
        show_stats()
    elif args.historical:
        fetch_historical(days=args.days)
    elif args.latest:
        fetch_latest()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
