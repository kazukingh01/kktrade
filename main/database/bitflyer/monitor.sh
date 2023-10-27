#!/bin/bash

EXCHANGE="bitflyer"
HOMEDIR="/home/ubuntu"
LOGDIR="${HOMEDIR}/kktrade/main/log/"
MODULE="${HOMEDIR}/kktrade/main/database/${EXCHANGE}/getdata.py"
COMMANDS=("getorderbook" "getexecutions" "getticker")
PYTHON="${HOMEDIR}/venv/bin/python"

mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    LOGFILE="${LOGDIR}${EXCHANGE}_${COMMAND}.`date "+%Y%m%d"`.log"
    if ! ps aux | grep -v grep | grep python | grep "${MODULE} --fn ${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting..."
        touch ${LOGFILE}
        nohup ${PYTHON} ${MODULE} --fn ${COMMAND} --update >> ${LOGFILE} 2>&1 &
    else
        echo "Process: ${COMMAND} is running."
    fi
done