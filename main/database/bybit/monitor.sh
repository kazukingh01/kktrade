#!/bin/bash
set -eu

EXCHANGE="bybit"
cd ${HOMETRADE}/main/database/${EXCHANGE}
COMMANDS=("getorderbook" "getexecutions" "getticker" "getkline")
source ../monitor.base.sh
