#!/bin/bash

VAR_NAME=${HOMETRADE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

cd ${HOMETRADE}/main/database
pkill python
sleep 5
python ${HOMETRADE}/main/database/dbapi.py --disconnect
sleep 5
pkill uvicorn
sleep 5
nohup uvicorn dbapi:app >/dev/null 2>&1 &
sleep 5
python dbapi.py --reconnect --logfilepath ../log/dbapi.log
