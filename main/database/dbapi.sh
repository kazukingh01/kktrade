#!/bin/bash

nohup uvicorn dbapi:app > ../log/dbapi.log &