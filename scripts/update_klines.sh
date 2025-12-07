#!/bin/bash
# Hourly Binance klines update script
cd /Users/Lacointa/Documents/scriptsetcode/bot
./target/release/btc-probability-matrix binance update >> logs/klines_update.log 2>&1
