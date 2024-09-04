#!/bin/bash

VAR_NAME=${HOMETRADE:-}
if [ -z "$VAR_NAME" ]; then
  HOMETRADE="${HOME}/kktrade"
fi

# Check number of args
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 arg1"
  exit 1
fi

cd ${HOMETRADE}/main/database

case "$1" in
  1)
    echo "force to restart."
    pkill python
    sleep 5
    python ${HOMETRADE}/main/database/dbapi.py --disconnect
    sleep 5
    pkill uvicorn
    sleep 5
    nohup uvicorn dbapi:app >/dev/null 2>&1 &
    sleep 5
    python dbapi.py --reconnect --logfilepath ../log/dbapi.log
    ;;
  2)
    echo "Restart if there is no uvicorn process."
    if ! ps aux | grep -v grep | grep uvicorn > /dev/null; then
      echo "uvicorn process is not found. Restart..."
      nohup uvicorn dbapi:app >/dev/null 2>&1 &
      sleep 5
      python dbapi.py --reconnect --logfilepath ../log/dbapi.log
    else
      echo "uvicorn process is found."
    fi
    ;;
esac

