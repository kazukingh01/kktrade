#!/bin/bash
set -eu

EXCHANGE="binance"
cd ${HOMETRADE}/main/database/${EXCHANGE}
COMMANDS=("getorderbook" "getexecutions" "getkline" "getfundingrate" "getopeninterest" "getlongshortratio" "gettakervolume")
source ../monitor.base.sh
