import datetime, argparse
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str, help="table from")
    parser.add_argument("--to", type=str, help="table to")
    parser.add_argument("--since", type=str, help="since when. ex) 20200101")
    parser.add_argument("--until", type=str, help="until when. ex) 20200101")
    parser.add_argument("--update", action='store_true', default=False)
    parser.add_argument("--delete", action='store_true', default=False)
    parser.add_argument("--colname", type=str, default="unixtime")
    parser.add_argument("--times", type=int, default=1000)
    args = parser.parse_args()
    print(args)
    args.since = datetime.datetime.fromisoformat(args.since)
    args.until = datetime.datetime.fromisoformat(args.until)
    DB = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df = DB.select_sql(f"select * from {args.fr} where {args.colname} >= {int(args.since.timestamp() * args.times)} and {args.colname} < {int(args.until.timestamp() * args.times)};")
    if args.update:
        if df.shape[0] > 0:
            DB.execute_copy_from_df(df, args.to, system_colname_list=[], filename="./test.csv", n_jobs=8)
            if args.delete:
                DB.execute_sql(f"delete from {args.fr} where {args.colname} >= {int(args.since.timestamp() * args.times)} and {args.colname} < {int(args.until.timestamp() * args.times)};")