import datetime, argparse


parser = argparse.ArgumentParser()
parser.add_argument("--tbl", type=str, help="--tbl bybit_executions", required=True)
parser.add_argument("--fr", type=int, help="--fr 2020")
parser.add_argument("--to", type=int, help="--to 2099")
args   = parser.parse_args()
assert args.fr < args.to
assert 1900 <= args.fr <= 2099
assert 1900 <= args.to <= 2099
lists = [datetime.datetime(year, month, 1, 0, 0, 0, tzinfo=datetime.UTC) for year in range(args.fr, args.to, 1) for month in range(1, 13)] + [datetime.datetime(args.to, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)]
for i, date in enumerate(lists):
    if i == len(lists) - 1: break
    timestamp_fr = int(date.      timestamp())
    timestamp_to = int(lists[i+1].timestamp())
    # FROM <= VALUE < TO.
    sql = f"CREATE TABLE {args.tbl}_{date.strftime('%Y%m')} PARTITION OF {args.tbl} FOR VALUES FROM ({timestamp_fr}) TO ({timestamp_to});"
    print(sql)
