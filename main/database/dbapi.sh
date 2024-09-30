#!/bin/bash

# Check number of args
IP="127.0.0.1"
PORT=8000
if [ "$#" -eq 1 ]; then
  echo "[START]"
elif [ "$#" -eq 2 ]; then
  echo "[START]"
  PORT=$2
else
  echo "Usage: $0 procNo portNo"
  exit 1
fi

SCRIPT_DIR=$(cd $(dirname $0); pwd)
echo "change direcroty to ${SCRIPT_DIR} ..."
cd $SCRIPT_DIR

NOPROC=$(ps aux | grep -v grep | grep uvicorn | grep -- "--port ${PORT}" | awk '{print $2}' | head -n 1)

case "$1" in
  1)
    echo "force to restart. ${IP}:${PORT}"
    if [ -z "$NOPROC" ]; then
      echo "uvicorn process is not found."
    else
      python dbapi.py --ip ${IP} --port ${PORT} --disconnect
      sleep 5
      kill $NOPROC
      sleep 5
    fi
    nohup uvicorn dbapi:app --port ${PORT} >/dev/null 2>&1 &
    sleep 5
    python dbapi.py --ip ${IP} --port ${PORT} --reconnect --logfilepath ../log/dbapi.log
    ;;
  2)
    echo "Restart if there is no uvicorn process. ${IP}:${PORT}"
    if [ -z "$NOPROC" ]; then
      echo "uvicorn process is not found. Restart..."
      nohup uvicorn dbapi:app --port ${PORT} >/dev/null 2>&1 &
      sleep 5
      python dbapi.py --ip ${IP} --port ${PORT} --reconnect --logfilepath ../log/dbapi.log
    else
      echo "uvicorn process is found."
    fi
    ;;
esac

echo "[END]"
