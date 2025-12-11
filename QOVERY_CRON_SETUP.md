# Qovery Cron Job Setup

This guide explains how to set up the automatic cron jobs on Qovery for the BTC trading bot.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ binance_prices  │  │ matrix_snapshots│  │   bot_trades    │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└──────────▲─────────────────────▲─────────────────────▲──────────┘
           │                     │                     │
           │ reads               │ writes/reads        │ writes
           │                     │                     │
┌──────────┴──────────┐  ┌───────┴───────┐  ┌─────────┴─────────┐
│   Matrix Builder    │  │               │  │                   │
│   (Cron Job)        ├──►  DATABASE_URL ◄──┤   Trading Bot     │
│   hourly            │  │               │  │   (Service)       │
└─────────────────────┘  └───────────────┘  └───────────────────┘
```

## Setup Steps

### 1. Create the Cron Job in Qovery

In your Qovery project, create a new **Cron Job**:

- **Name**: `matrix-builder`
- **Source**: Same repository as the bot
- **Dockerfile path**: `Dockerfile`
- **Dockerfile target**: `matrix-builder`
- **Schedule**: `0 * * * *` (every hour at minute 0)

### 2. Configure Environment Variables

Set these environment variables for the cron job:

| Variable | Value | Description |
|----------|-------|-------------|
| `DATABASE_URL` | `postgresql://...` | Same PostgreSQL connection as the bot |
| `RUST_LOG` | `info` | Log level |

### 3. Verify the Bot Uses DATABASE_URL

Make sure your trading bot service has `DATABASE_URL` configured. The bot will:
1. Try to load the matrix from the database first
2. Fall back to `output/matrix.json` if DATABASE_URL is not set

### 4. Alternative Schedules

You can adjust the cron schedule based on your needs:

| Schedule | Expression | Use Case |
|----------|------------|----------|
| Every hour | `0 * * * *` | Default - good balance |
| Every 30 minutes | `*/30 * * * *` | Fresher data |
| Every 15 minutes | `*/15 * * * *` | Most up-to-date |
| Every 6 hours | `0 */6 * * *` | Lighter load |
| Once daily | `0 0 * * *` | Minimal updates |

## How It Works

1. **Cron job runs** on schedule
2. **Matrix builder**:
   - Connects to PostgreSQL
   - Fetches all BTC price data from `binance_prices`
   - Builds the probability matrix
   - Saves to `matrix_snapshots` table (marks previous as inactive)
   - Also saves to local files (JSON, CSV, report)
3. **Bot automatically uses latest matrix**:
   - On startup, loads from `matrix_snapshots` table
   - Uses the most recent `is_active = TRUE` snapshot

## Monitoring

Check the cron job logs in Qovery console to verify:
- Matrix is being built successfully
- Number of windows analyzed is increasing over time
- No database connection errors

## Troubleshooting

### "No matrix found in database"
- Run the matrix builder manually first: `./btc-probability-matrix build`
- Or wait for the first cron execution

### "Failed to connect to database"
- Verify DATABASE_URL is correct
- Check PostgreSQL is accessible from Qovery

### Matrix not updating
- Check cron job execution logs
- Verify the `matrix_snapshots` table has new rows

---

## Polymarket Prices Sync Job

This job syncs historical UP/DOWN share prices from Polymarket for analysis.

### Setup

Create a second **Cron Job** in Qovery:

- **Name**: `polymarket-prices-sync`
- **Source**: Same repository as the bot
- **Command**: `python3 scripts/sync_polymarket_prices.py --hours 24`
- **Schedule**: `30 * * * *` (every hour at minute 30)

### Environment Variables

| Variable | Value | Description |
|----------|-------|-------------|
| `DB_HOST` | `zd4409065-postgresql...` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `qoveryadmin` | Database user |
| `DB_PASSWORD` | `(your password)` | Database password |
| `DB_NAME` | `polymarket` | Database name |

### How It Works

1. **Cron job runs** every hour (30 minutes past)
2. **Sync script**:
   - Fetches all 15-minute windows from the last 24 hours
   - Skips windows already in `polymarket_prices` table
   - Fetches UP/DOWN token IDs from Gamma API
   - Fetches price history from CLOB API
   - Inserts new data into database
3. **Data available** in `polymarket_prices` table

### Manual Backfill

To backfill historical data (up to 28 days retained by Polymarket):

```bash
python3 scripts/backfill_polymarket_prices.py --days 28
```

### Database Schema

The `polymarket_prices` table stores:

| Column | Type | Description |
|--------|------|-------------|
| `window_timestamp` | BIGINT | Unix timestamp of 15-min window start |
| `token_type` | VARCHAR(4) | 'UP' or 'DOWN' |
| `token_id` | VARCHAR(100) | CLOB token ID |
| `timestamp` | TIMESTAMPTZ | When price was recorded |
| `price` | NUMERIC(10,6) | Share price (0-1) |

### Useful Views

- `v_polymarket_prices` - Prices with computed window times
- `v_polymarket_window_prices` - Combined UP/DOWN prices per window

### Example Queries

```sql
-- Recent price data
SELECT * FROM v_polymarket_prices
WHERE timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;

-- Combined UP/DOWN for a window
SELECT * FROM v_polymarket_window_prices
WHERE window_timestamp = 1765400400;

-- Average spread over time
SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    AVG(up_price) as avg_up,
    AVG(down_price) as avg_down
FROM v_polymarket_window_prices
GROUP BY 1 ORDER BY 1 DESC;
```
