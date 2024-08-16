#!/bin/bash

pkill uvicorn
if ! command -v uvicorn &> /dev/null
then
    # No found uvicorn
    nohup ~/venv/bin/uvicorn dbapi:app > ../log/dbapi.log &
else
    # found
    nohup uvicorn dbapi:app > ../log/dbapi.log &
fi