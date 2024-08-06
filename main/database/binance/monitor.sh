#!/bin/bash

EXCHANGE="binance"
HOMEDIR="${HOME}" # It's done by root user via cron.
LOGDIR="${HOMEDIR}/kktrade/main/log/"
MODULE="${HOMEDIR}/kktrade/main/database/${EXCHANGE}/getdata.py"
COMMANDS=("getorderbook" "getexecutions" "getkline" "getfundingrate" "getopeninterest" "getlongshortratio" "gettakervolume")
PYTHON="${HOMEDIR}/venv/bin/python"

mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    LOGFILE="${LOGDIR}${EXCHANGE}_${COMMAND}.`date "+%Y%m%d"`.log"
    if ! ps aux | grep -v grep | grep python | grep "${MODULE} --fn ${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting... Command: ${PYTHON} ${MODULE} --fn ${COMMAND} --update"
        touch ${LOGFILE}
        nohup ${PYTHON} ${MODULE} --fn ${COMMAND} --update >> ${LOGFILE} 2>&1 &
    else
        echo "Process: ${COMMAND} is running."
    fi
done