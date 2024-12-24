#!/bin/bash
set -eu

ST_DATE=$(date -d '3 minutes ago' -u +%Y%m%d%H%M%S)
ED_DATE=$(date -u +%Y%m%d%H%M%S)
# tx to 60s
python txs_to_ohlc.py  --fr $ST_DATE --to $ED_DATE --exmin 5 --update
# 60s to 120s
python ohlc_to_ohlc.py --fr $ST_DATE --to $ED_DATE --frsr 60 --tosr 120 --itvls 120 --type 1 --update
# x4, x4x5 from 120s
python ohlc_to_ohlc.py --fr $ST_DATE --to $ED_DATE --frsr 120 --tosr 120 --itvls 480,2400 --type 2 --update # 8m, 40m
