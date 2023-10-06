#!/bin/bash

HOMEDIR="/home/ubuntu"
LOGDIR="${HOMEDIR}/log/"
MODULE="${HOMEDIR}/kktrade/main/bitflyer/getdata.py"
COMMANDS=("getboard" "getexecutions" "getticker")
PYTHON="${HOMEDIR}/venv/bin/python"

mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    if ! ps aux | grep -v grep | grep python | grep "${MODULE} ${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting..."
        nohup ${PYTHON} ${MODULE} ${COMMAND} >> ${LOGDIR}${COMMAND}.`date "+%Y%m%d"`.log 2>&1 &
    else
        echo "Process: ${COMMAND} is running."
    fi
done