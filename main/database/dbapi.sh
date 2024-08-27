#!/bin/bash

cd ${HOME}/kktrade/main/database
python dbapi.py --disconnect
sleep 5
pkill uvicorn
sleep 5
if ! command -v uvicorn &> /dev/null
then
    # No found uvicorn
    nohup ~/venv/bin/uvicorn dbapi:app > ../log/dbapi.`date "+%Y%m%d%H%M%S"`.log &
else
    # found
    nohup uvicorn dbapi:app > ../log/dbapi.`date "+%Y%m%d%H%M%S"`.log &
fi