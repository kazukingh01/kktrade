#!/bin/bash
set -eu

VAR_NAME=${EXCHANGE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

EXCHANGE="dukascopy"
COMMANDS=("getlastminutekline")
source ${HOMETRADE}/main/database/monitor.base.sh
