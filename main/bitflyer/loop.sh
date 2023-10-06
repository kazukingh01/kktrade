#!/bin/bash

python getdata.py getall 30 10000
for i in {1..1000}
do
   python getdata.py getall 20230915 60 333
done
