#!/bin/bash
set -eu

ST_DATE="2019-01-01"
ED_DATE="2024-09-30"

CR_DATE=$(date -d "$ST_DATE" +"%Y-%m-%d")
NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
while [ "$(date -d "$CR_DATE" +%Y%m%d)" != "$(date -d "$ED_DATE + 1 day" +%Y%m%d)" ]; do
  echo "from: $CR_DATE, to: $NX_DATE"
  python create_data.py --fr $(date -d "$CR_DATE" +%Y%m%d) --to $(date -d "$NX_DATE" +%Y%m%d) --save ./data
  CR_DATE=$NX_DATE
  NX_DATE=$(date -d "$CR_DATE + 1 day" +"%Y-%m-%d")
done
