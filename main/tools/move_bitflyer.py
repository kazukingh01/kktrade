import argparse, datetime
import numpy as np
import pandas as pd
from tqdm import tqdm
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME

PKEY = {
    "bitflyer_executions": ["symbol", "id"],
    "bitflyer_orderbook": ["unixtime"],
    "bitflyer_ticker": ["symbol", "tick_id"]
}


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
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    assert args.tbl is not None
    assert args.tbl in PKEY
    DB_from = Psgre(f"host={args.ipfr} port={args.portfr} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    DB_to   = Psgre(f"host={args.ipto} port={args.portto} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    if args.since is not None and args.until is not None:
        assert args.since < args.until
        dfbase = DB_from.select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl} where unixtime >= {int(args.since.timestamp())} and unixtime < {int(args.until.timestamp())};")
        dfwk   = DB_to  .select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl} where unixtime >= {int(args.since.timestamp())} and unixtime < {int(args.until.timestamp())};")
    else:
        dfbase = DB_from.select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl};")
        dfwk   = DB_to  .select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl};")
    if len(PKEY[args.tbl]) == 1:
        dfbase = dfbase.loc[~dfbase[PKEY[args.tbl][0]].isin(dfwk[PKEY[args.tbl][0]].unique())]
        dfbase = dfbase.groupby(PKEY[args.tbl][0]).first().reset_index(drop=False)
    else:
        dfbase["__work"] = ""
        dfwk[  "__work"] = ""
        for x in PKEY[args.tbl]:
            dfbase["__work"] = dfbase["__work"] + dfbase[x].astype(str) + ","
            dfwk[  "__work"] = dfwk[  "__work"] + dfwk[  x].astype(str) + ","
        dfbase = dfbase.loc[~dfbase["__work"].isin(dfwk["__work"].unique())]
        dfbase = dfbase.groupby("__work").first().reset_index(drop=True)
    for index in tqdm(np.array_split(np.arange(dfbase.shape[0]), dfbase.shape[0] // args.num)):
        dfwk = dfbase.iloc[index].copy()
        if len(PKEY[args.tbl]) == 1:
            sql  = f"select * from {args.tbl} where {PKEY[args.tbl][0]} in (" + ",".join(dfwk[PKEY[args.tbl][0]].astype(str).tolist()) + ");"
        else:
            sql  = f"select * from {args.tbl} where "
            for ndf in dfwk.values:
                sql += "("
                for i, x in enumerate(ndf):
                    if i > 0: sql += " and "
                    sql += f"{dfwk.columns[i]} = '{x}'" if isinstance(x, str) else f"{dfwk.columns[i]} = {x}"
                sql += ") or "
            sql = sql[:-4]
        df_insert = DB_from.select_sql(sql)
        df_insert.columns = df_insert.columns.str.replace("^best_", "", regex=True)
        if df_insert.columns.isin(["type"]).any():
            df_insert["side"] = df_insert["type"].map({"Buy": 0, "Sell": 1, "BUY": 0, "SELL": 1, "asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int) # nan = 板寄せ
        if args.update and df_insert.shape[0] > 0:
            DB_to.execute_copy_from_df(df_insert, args.tbl, system_colname_list=[], filename="test.csv", n_jobs=8)
