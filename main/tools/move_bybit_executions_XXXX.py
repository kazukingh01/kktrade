import argparse
import numpy as np
from tqdm import tqdm
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import PORT, USER, PASS, DBNAME


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str, help="db from")
    parser.add_argument("--to", type=str, help="db to")
    parser.add_argument("--tbl", type=str, help="table name")
    args = parser.parse_args()
    print(args)
    DB_from = Psgre(f"host={args.fr} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    DB_to   = Psgre(f"host={args.to} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    dfbase  = DB_from.select_sql(f"select id from {args.tbl};")
    dfwk    = DB_to  .select_sql(f"select id from {args.tbl};")
    dfbase  = dfbase.loc[~dfbase["id"].isin(dfwk["id"])]
    for index in tqdm(np.array_split(np.arange(dfbase.shape[0]), dfbase.shape[0] // 10000)):
        dfwk = dfbase.iloc[index].copy()
        sql  = f"select * from {args.tbl} where id in ('" + "','".join(dfwk["id"].tolist()) + "');"
        df_insert = DB_from.select_sql(sql)
        df_insert["side"] = df_insert["type"].map({"Buy": 0, "Sell": 1, "BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int)
        DB_to.execute_copy_from_df(df_insert, args.tbl, system_colname_list=[], filename="test.csv", n_jobs=4)
