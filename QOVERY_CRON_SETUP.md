# Qovery Cron Job Setup - Matrix Builder

This guide explains how to set up the automatic matrix rebuild cron job on Qovery.

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
