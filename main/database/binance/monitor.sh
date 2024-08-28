#!/bin/bash
set -eu

VAR_NAME=${HOMETRADE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

EXCHANGE="binance"
COMMANDS=("getorderbook" "getexecutions" "getkline" "getfundingrate" "getopeninterest" "getlongshortratio" "gettakervolume")
source ${HOMETRADE}/main/database/monitor.base.sh
