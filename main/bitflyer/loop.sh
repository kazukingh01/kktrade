#!/bin/bash

python bitflyer.py getall 30 10000
for i in {1..100}
do
   python bitflyer.py getall 60 3
done
