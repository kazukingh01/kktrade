import argparse, datetime
import numpy as np
import pandas as pd
from tqdm import tqdm
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME

PKEY = {
    "bitflyer_executions": ["id"], #["symbol", "id"],
    "bitflyer_orderbook": ["unixtime"],
    "bitflyer_ticker": ["tick_id"] #["symbol", "tick_id"]
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
    parser.add_argument("--njob", type=int, default=1)
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
    assert len(PKEY[args.tbl]) == 1
    dfbase = dfbase.loc[~dfbase[PKEY[args.tbl][0]].isin(dfwk[PKEY[args.tbl][0]].unique())]
    dfbase = dfbase.groupby(PKEY[args.tbl][0]).first().reset_index(drop=False)
    assert dfbase.shape[0] > 0
    for index in tqdm(np.array_split(np.arange(dfbase.shape[0]), dfbase.shape[0] // args.num)):
        dfwk = dfbase.iloc[index].copy()
        sql  = (
            f"select main.*, " + 
            f"scale_pre->>'price' as scale_pre_price,scale_pre->>'size' as scale_pre_size,scale_pre->>'bid' as scale_pre_bid,scale_pre->>'ask' as scale_pre_ask,scale_pre->>'last_traded_price' as scale_pre_last_traded_price,scale_pre->>'bid_size' as scale_pre_bid_size,scale_pre->>'ask_size' as scale_pre_ask_size,scale_pre->>'total_bid_depth' as scale_pre_total_bid_depth,scale_pre->>'total_ask_depth' as scale_pre_total_ask_depth,scale_pre->>'market_bid_size' as scale_pre_market_bid_size,scale_pre->>'market_ask_size' as scale_pre_market_ask_size,scale_pre->>'volume' as scale_pre_volume,scale_pre->>'volume_by_product' as scale_pre_volume_by_product, " + 
            f"scale_aft->>'price' as scale_aft_price,scale_aft->>'size' as scale_aft_size,scale_aft->>'bid' as scale_aft_bid,scale_aft->>'ask' as scale_aft_ask,scale_aft->>'last_traded_price' as scale_aft_last_traded_price,scale_aft->>'bid_size' as scale_aft_bid_size,scale_aft->>'ask_size' as scale_aft_ask_size,scale_aft->>'total_bid_depth' as scale_aft_total_bid_depth,scale_aft->>'total_ask_depth' as scale_aft_total_ask_depth,scale_aft->>'market_bid_size' as scale_aft_market_bid_size,scale_aft->>'market_ask_size' as scale_aft_market_ask_size,scale_aft->>'volume' as scale_aft_volume,scale_aft->>'volume_by_product' as scale_aft_volume_by_product " + 
            f"from {args.tbl} as main " + 
            f"left join master_symbol as mst on main.symbol = mst.symbol_id " + 
            f"where main.{PKEY[args.tbl][0]} in (" + ",".join(dfwk[PKEY[args.tbl][0]].astype(str).tolist()) + ")"
        )
        df_insert = DB_from.select_sql(sql)
        for x in [
            'scale_aft_price', 'scale_aft_size','scale_aft_bid', 'scale_aft_ask', 'scale_aft_last_traded_price', 'scale_aft_bid_size', 'scale_aft_ask_size',
            'scale_aft_total_bid_depth', 'scale_aft_total_ask_depth', 'scale_aft_market_bid_size', 'scale_aft_market_ask_size', 'scale_aft_volume', 'scale_aft_volume_by_product'
        ]:
            df_insert[x] = df_insert[x].astype(np.float64)
            df_insert[x.replace("scale_aft", "scale_pre")] = df_insert[x.replace("scale_aft", "scale_pre")].astype(np.float64)
            df_insert.loc[df_insert[x].isna(), x] = (1.0 / df_insert.loc[df_insert[x].isna(), x.replace("scale_aft", "scale_pre")].copy())
            if x.replace("scale_aft_", "") in df_insert.columns: df_insert[x.replace("scale_aft_", "")] = (df_insert[x.replace("scale_aft_", "")] * df_insert[x]).astype(np.float64)
        if df_insert.columns.isin(["type"]).any():
            df_insert["side"] = df_insert["type"].map({"Buy": 0, "Sell": 1, "BUY": 0, "SELL": 1, "asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int) # nan = 板寄せ
        if args.update and df_insert.shape[0] > 0:
            DB_to.insert_from_df(df_insert, args.tbl, is_select=True, n_jobs=args.njob)
            DB_to.execute_sql()
