#!/bin/bash
set -eu

EXCHANGE="dukascopy"
cd ${HOMETRADE}/main/database/${EXCHANGE}
COMMANDS=("getlastminutekline")
source ../monitor.base.sh
