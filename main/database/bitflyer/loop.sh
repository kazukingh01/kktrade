#!/bin/bash

python getdata.py --fn getall --fr 20231001 --to 20231012 --sec 30 --cnt 200
for i in {1..1000}
do
   python getdata.py --fn getall --fr 20231001 --to 20231012 --sec 30 --sec 200
done
