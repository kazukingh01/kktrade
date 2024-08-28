#!/bin/bash
set -eu

VAR_NAME=${EXCHANGE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

EXCHANGE="bybit"
COMMANDS=("getorderbook" "getexecutions" "getticker" "getkline")
source ${HOMETRADE}/main/database/monitor.base.sh
