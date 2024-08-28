#!/bin/bash
set -eu

EXCHANGE="bitflyer"
COMMANDS=("getorderbook" "getexecutions" "getticker" "getfundingrate")
source ../monitor.base.sh
