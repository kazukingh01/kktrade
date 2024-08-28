#!/bin/bash
set -eu

EXCHANGE="binance"
COMMANDS=("getorderbook" "getexecutions" "getkline" "getfundingrate" "getopeninterest" "getlongshortratio" "gettakervolume")
source ../monitor.base.sh
