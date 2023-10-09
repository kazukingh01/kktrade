#!/bin/bash

HOMEDIR="/home/ubuntu"
LOGDIR="${HOMEDIR}/kktrade/main/log/"
MODULE="${HOMEDIR}/kktrade/main/database/bitflyer/getdata.py"
COMMANDS=("getorderbook" "getexecutions" "getticker")
PYTHON="${HOMEDIR}/venv/bin/python"

mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    if ! ps aux | grep -v grep | grep python | grep "${MODULE} ${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting..."
        touch ${LOGDIR}${COMMAND}.`date "+%Y%m%d"`.log
        nohup ${PYTHON} ${MODULE} ${COMMAND} >> ${LOGDIR}${COMMAND}.`date "+%Y%m%d"`.log 2>&1 &
    else
        echo "Process: ${COMMAND} is running."
    fi
done