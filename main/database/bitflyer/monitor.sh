#!/bin/bash
set -eu

EXCHANGE="bitflyer"
cd ${HOMETRADE}/main/database/${EXCHANGE}
COMMANDS=("getorderbook" "getexecutions" "getticker" "getfundingrate")
source ../monitor.base.sh
