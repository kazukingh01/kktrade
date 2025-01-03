#!/bin/bash
set -eu

HOMETRADE="${HOME}/kktrade"
LOGDIR="${HOMETRADE}/main/log/"
LOGFILE="${LOGDIR}mart_online2.`date "+%Y%m%d"`.log"

ST_DATE=$(date -d '43 minutes ago' -u +%Y%m%d%H%M%S)
ED_DATE=$(date -u +%Y%m%d%H%M%S)

# 60s to 2400s
python ohlc_to_ohlc.py --fr $ST_DATE --to $ED_DATE --frsr 60 --tosr 2400 --itvls 2400 --type 1 --update >> ${LOGFILE} 2>&1
# x6, x6x6 from 2400s
python ohlc_to_ohlc.py --fr $ST_DATE --to $ED_DATE --frsr 2400 --tosr 2400 --itvls 14400,86400 --type 2 --update >> ${LOGFILE} 2>&1 # 4h, 24h
