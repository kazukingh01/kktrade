import datetime, argparse


parser = argparse.ArgumentParser()
parser.add_argument("--tbl",  type=str, help="--tbl bybit_executions", required=True)
parser.add_argument("--fr",   type=int, help="--fr 2020")
parser.add_argument("--to",   type=int, help="--to 2099")
parser.add_argument("--db",   type=str, help="--db psgre", default="psgre")
parser.add_argument("--days", type=lambda x: [int(y.strip()) for y in x.split(",")], default="1,10,20")
parser.add_argument("--isunixtime", action='store_true', default=False)
args   = parser.parse_args()
assert args.fr < args.to
assert 1900 <= args.fr <= 2099
assert 1900 <= args.to <= 2099
assert args.db in ["mysql", "psgre"]
assert isinstance(args.days, list)
lists = [datetime.datetime(year, month, day, 0, 0, 0, tzinfo=datetime.UTC) for year in range(args.fr, args.to, 1) for month in range(1, 13) for day in args.days] + [datetime.datetime(args.to, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)]

for i, date in enumerate(lists):
    if i == len(lists) - 1: break
    timestamp_fr = int(date.      timestamp())
    timestamp_to = int(lists[i+1].timestamp())
    datetime_fr  = date
    datetime_to  = lists[i+1]
    if i == 0:
        if args.db == "psgre":
            if args.isunixtime:
                sql = f"CREATE TABLE {args.tbl}_00000000 PARTITION OF {args.tbl} FOR VALUES FROM (0) TO ({timestamp_fr});"
            else:
                sql = f"CREATE TABLE {args.tbl}_00000000 PARTITION OF {args.tbl} FOR VALUES FROM ('1970-01-01 00:00:00+00') TO ('{datetime_fr.strftime('%Y-%m-%d %H:%M:%S+00')}');"
        else:
            if args.isunixtime:
                sql = f"PARTITION {args.tbl}_00000000 VALUES LESS THAN ({timestamp_fr}),"
            else:
                sql = f"PARTITION {args.tbl}_00000000 VALUES LESS THAN ('{datetime_fr.strftime('%Y-%m-%d %H:%M:%S')}'),"
        print(sql)
    # FROM <= VALUE < TO.
    if args.db == "psgre":
        if args.isunixtime:
            sql = f"CREATE TABLE {args.tbl}_{date.strftime('%Y%m%d')} PARTITION OF {args.tbl} FOR VALUES FROM ({timestamp_fr}) TO ({timestamp_to});"
        else:
            sql = f"CREATE TABLE {args.tbl}_{date.strftime('%Y%m%d')} PARTITION OF {args.tbl} FOR VALUES FROM ('{datetime_fr.strftime('%Y-%m-%d %H:%M:%S+00')}') TO ('{datetime_to.strftime('%Y-%m-%d %H:%M:%S+00')}');"
    else:
        if args.isunixtime:
            sql = f"PARTITION {args.tbl}_{date.strftime('%Y%m%d')} VALUES LESS THAN ({timestamp_to}),"
        else:
            sql = f"PARTITION {args.tbl}_{date.strftime('%Y%m%d')} VALUES LESS THAN ('{datetime_to.strftime('%Y-%m-%d %H:%M:%S')}'),"
    print(sql)
    if i == len(lists) - 2:
        if args.db == "psgre":
            if args.isunixtime:
                sql = f"CREATE TABLE {args.tbl}_99999999 PARTITION OF {args.tbl} FOR VALUES FROM ({timestamp_to}) TO (9999999999);"
            else:
                sql = f"CREATE TABLE {args.tbl}_99999999 PARTITION OF {args.tbl} FOR VALUES FROM ('{datetime_to.strftime('%Y-%m-%d %H:%M:%S+00')}') TO ('2100-01-01 00:00:00+00');"
        else:
            if args.isunixtime:
                sql = f"PARTITION {args.tbl}_99999999 VALUES LESS THAN MAXVALUE"
            else:
                sql = f"PARTITION {args.tbl}_99999999 VALUES LESS THAN MAXVALUE"
        print(sql)

