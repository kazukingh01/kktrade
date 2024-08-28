#!/bin/bash
set -eu

EXCHANGE="bybit"
COMMANDS=("getorderbook" "getexecutions" "getticker" "getkline")
source ../monitor.base.sh
