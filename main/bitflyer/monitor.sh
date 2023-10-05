#!/bin/bash

LOGDIR="/home/ubuntu/log/"
MODULE="/home/ubuntu/kktrade/main/bitflyer/bitflyer.py"
COMMANDS=("getboard" "getexecutions" "getticker")
PYTHON="/home/ubuntu/venv/bin/python"

mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    if ! ps aux | grep -v grep | grep python | grep "${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting..."
        nohup ${PYTHON} ${MODULE} ${COMMAND} >> ${LOGDIR}${COMMAND}.`date "+%Y%m%d"`.log &
    else
        echo "Process: ${COMMAND} is running."
    fi
done