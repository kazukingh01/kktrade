#!/bin/bash
set -eu

VAR_NAME=${HOMETRADE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

EXCHANGE="bybit"
COMMANDS=("getorderbook" "getexecutions" "getticker" "getkline" "getfundingrate" "getopeninterest" "getlongshortratio")
source ${HOMETRADE}/main/database/monitor.base.sh


