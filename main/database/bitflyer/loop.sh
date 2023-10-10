#!/bin/bash

python getdata.py getall 20231001 20231012 30 200
for i in {1..1000}
do
   python getdata.py getall 20231001 20231012 30 200
done
