#!/bin/bash
set -euxo pipefail

date_since="2020-10-23"
date_until="2020-12-31"

current_date="$date_since"
while [ "$(date -d "$current_date" +%s)" -le "$(date -d "$date_until" +%s)" ]; do
    echo $current_date
    python move_record.py --fr bybit_executions --to bybit_executions_`date -d "$current_date" +%Y` --since `date -d "$current_date" +%Y%m%d` --until `date -d "$current_date + 1 day" +%Y%m%d` --update --delete
    current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
done
