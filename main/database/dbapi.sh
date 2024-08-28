#!/bin/bash

cd ${HOME}/kktrade/main/database
if ! ps aux | grep -v grep | grep uvicorn > /dev/null; then
    python dbapi.py --connect --logfilepath ../log/dbapi.`date "+%Y%m%d%H%M%S"`.log
else
    if ! command -v uvicorn &> /dev/null
    then
        # No found uvicorn
        ~/kktrade/venv/bin/uvicorn dbapi:app
    else
        # found
        uvicorn dbapi:app
    fi
fi
