#!/bin/bash
set -eu

ST_DATE="2019-12-12"
ED_DATE="2022-01-01"

python txs_to_ohlc.py --fr ${ST_DATE} --to ${ED_DATE} --hours 0,6,12,18 --update
CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python ohlc_to_ohlc.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --frsr 60 --tosr 120 --itvls 120 --type 1 --update
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done
