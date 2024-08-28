if [ -z "$EXCHANGE" ]; then
  exit 0
fi
if [ -z "$COMMANDS" ]; then
  exit 0
fi

LOGDIR="../../log/"
HOMETRADE="${HOME}/kktrade"
MODULE="${HOMETRADE}/main/database/${EXCHANGE}/getdata.py"

cd ${HOMETRADE}/main/database/${EXCHANGE}
mkdir -p ${LOGDIR}

for COMMAND in "${COMMANDS[@]}"; do
    LOGFILE="${LOGDIR}${EXCHANGE}_${COMMAND}.`date "+%Y%m%d"`.log"
    if ! ps aux | grep -v grep | grep python | grep "${MODULE} --fn ${COMMAND}" > /dev/null; then
        echo "Process: ${COMMAND} not found! Restarting... Command: python ${MODULE} --fn ${COMMAND} --update"
        touch ${LOGFILE}
        nohup python ${MODULE} --fn ${COMMAND} --update >> ${LOGFILE} 2>&1 &
    else
        echo "Process: ${COMMAND} is running."
    fi
done