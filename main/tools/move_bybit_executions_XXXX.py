import argparse, datetime
import numpy as np
from tqdm import tqdm
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ipfr", type=str, help="db from", default=HOST)
    parser.add_argument("--ipto", type=str, help="db to", default=HOST)
    parser.add_argument("--portfr", type=str, help="db from", default=PORT)
    parser.add_argument("--portto", type=str, help="db to", default=PORT)
    parser.add_argument("--since", type=datetime.datetime.fromisoformat)
    parser.add_argument("--until", type=datetime.datetime.fromisoformat)
    parser.add_argument("--tbl", type=str, help="table name")
    parser.add_argument("--num", type=int, default=10000)
    args = parser.parse_args()
    print(args)
    DB_from = Psgre(f"host={args.ipfr} port={args.portfr} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    DB_to   = Psgre(f"host={args.ipto} port={args.portto} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    if args.since is not None and args.until is not None:
        assert args.since < args.until
        dfbase = DB_from.select_sql(f"select id from {args.tbl} where unixtime >= {int(args.since.timestamp()) * 1000} and unixtime <= {int(args.until.timestamp()) * 1000};")
        dfwk   = DB_to  .select_sql(f"select id from {args.tbl} where unixtime >= {int(args.since.timestamp()) * 1000} and unixtime <= {int(args.until.timestamp()) * 1000};")
    else:
        dfbase = DB_from.select_sql(f"select id from {args.tbl};")
        dfwk   = DB_to  .select_sql(f"select id from {args.tbl};")
    dfbase = dfbase.loc[~dfbase["id"].isin(dfwk["id"])]
    for index in tqdm(np.array_split(np.arange(dfbase.shape[0]), dfbase.shape[0] // args.num)):
        dfwk = dfbase.iloc[index].copy()
        sql  = f"select * from {args.tbl} where id in ('" + "','".join(dfwk["id"].tolist()) + "');"
        df_insert = DB_from.select_sql(sql)
        df_insert["side"] = df_insert["type"].map({"Buy": 0, "Sell": 1, "BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int)
        DB_to.execute_copy_from_df(df_insert, args.tbl, system_colname_list=[], filename="test.csv", n_jobs=4)
