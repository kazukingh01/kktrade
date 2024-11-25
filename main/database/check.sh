#!/bin/bash
set -eu

# BS Once
ST_DATE=$(date -d "14 days ago" +"%Y-%m-%d")
ED_DATE=$(date -d "1  days ago" +"%Y-%m-%d")
echo "check: BASE once, from: $ST_DATE, to: $ED_DATE"
python check.py --fr $(date -d "$ST_DATE" +%Y%m%d) --to $(date -d "$ED_DATE" +%Y%m%d) --dbs bs --tbls binance_funding_rate,binance_long_short,binance_open_interest,binance_taker_volume,bitflyer_fundingrate,bitflyer_ticker,bybit_funding_rate,bybit_long_short,bybit_open_interest

# BS Daily
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")         # CURRENT
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d") # NEXT
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "check: BASE daily, from: $CR_DATE, to: $NX_DATE"
  python check.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --dbs bs --tbls bybit_ticker,binance_kline,bybit_kline,binance_executions,binance_orderbook,bitflyer_executions,bitflyer_orderbook,bybit_executions,bybit_orderbook
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# BK Daily
ST_DATE=$(date -d "18 days ago" +"%Y-%m-%d")
ED_DATE=$(date -d "6  days ago" +"%Y-%m-%d")
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")         # CURRENT
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d") # NEXT
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "check: BK daily, from: $CR_DATE, to: $NX_DATE"
  python check.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --dbs bk
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done
