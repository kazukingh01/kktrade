#!/bin/bash

VAR_NAME=${HOMETRADE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

cd ${HOMETRADE}/main/database
if ! ps aux | grep -v grep | grep uvicorn > /dev/null; then
    if ! command -v uvicorn &> /dev/null
    then
        # No found uvicorn
        nohup ${HOMETRADE}/venv/bin/uvicorn dbapi:app >/dev/null 2>&1 &
    else
        # found
        nohup uvicorn dbapi:app >/dev/null 2>&1 &
    fi
    sleep 5
fi
python dbapi.py --reconnect --logfilepath ../log/dbapi.`date "+%Y%m%d%H%M%S"`.log
