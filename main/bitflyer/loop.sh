#!/bin/bash

python getdata.py getall 30 10000
for i in {1..100}
do
   python getdata.py getall 60 3
done
