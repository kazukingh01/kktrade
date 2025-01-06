#!/bin/bash
set -eu

ST_DATE="2019-01-01"
ED_DATE="2024-09-30"

# tx to 3s
python txs_to_ohlc.py --fr $(date -d "$ST_DATE" +%Y%m%d) --to $(date -d "$ED_DATE" +%Y%m%d) --sr 3 --itvl 3 --hours 0,6,12,18 --exmin 30 --nq 5 --update

# 3s to 120s, 2400s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 3 --tosr 120,1800 --itvls 120,1800 --type 1 --ndiv 10 --update
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# x5, x15 from 120s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 120 --tosr 120,120 --itvls 600,1800 --type 2 --ndiv 10 --update # 10m, 30m
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# x8, x48 from 1800s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 1800 --tosr 1800,1800 --itvls 14400,86400 --type 2 --ndiv 10 --update # 4h, 24h
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done
