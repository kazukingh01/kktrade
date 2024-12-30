#!/bin/bash
set -eu

ST_DATE="2019-01-01"
ED_DATE="2024-09-30"

# tx to 60s
python txs_to_ohlc.py --fr $(date -d "$ST_DATE" +%Y%m%d) --to $(date -d "$ED_DATE" +%Y%m%d) --sr 3 --itvl 3 --hours 0,6,12,18 --exmin 30 --nq 5 --update
# python txs_to_ohlc.py --fr $(date -d "$ST_DATE" +%Y%m%d) --to $(date -d "$ED_DATE" +%Y%m%d) --sr 60 --itvl 60 --hours 0,6,12,18 --update
# python txs_to_ohlc.py --fr $(date -d "$ST_DATE" +%Y%m%d) --to $(date -d "$ED_DATE" +%Y%m%d) --sr 5 --itvl 5 --exmin 5 --hours 0,12 --ohlc --update

# 60s to 120s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 60 --tosr 120 --itvls 120 --type 1 --update
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# x4, x4x5 from 120s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 120 --tosr 120 --itvls 480,2400 --type 2 --update # 8m, 40m
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# 60s to 2400s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 60 --tosr 2400 --itvls 2400 --type 1 --update
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done

# x6, x6x6 from 2400s
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 2400 --tosr 2400 --itvls 14400,86400 --type 2 --update # 4h, 24h
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done
