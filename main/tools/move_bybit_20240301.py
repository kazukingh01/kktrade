import argparse, datetime, copy
import numpy as np
import pandas as pd
from tqdm import tqdm
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME

PKEY = {
    "bybit_executions": ["unixtime"], #["id"],
    "bybit_kline": ["symbol", "unixtime", "kline_type", "interval"],
    "bybit_orderbook": ["symbol", "unixtime"],
    "bybit_ticker": ["symbol", "unixtime"],
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
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--useunittime", action='store_true', default=False)
    parser.add_argument("--isgroupby", action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    assert args.tbl is not None
    tbl2 = args.tbl
    if args.tbl.find("bybit_executions_") >= 0:
        tbl2     = args.tbl
        args.tbl = "bybit_executions"
    assert args.tbl in PKEY
    DB_from = Psgre(f"host={args.ipfr} port={args.portfr} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    DB_to   = Psgre(f"host={args.ipto} port={args.portto} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    if args.since is not None and args.until is not None:
        assert args.since < args.until
        dfbase = DB_from.select_sql(f"select {','.join(PKEY[args.tbl])} from {tbl2    } where unixtime >= {int(args.since.timestamp()) * 1000} and unixtime < {int(args.until.timestamp()) * 1000} group by {','.join(PKEY[args.tbl])};")
        DB_from.logger.info("select end: from.")
        dfwk   = DB_to  .select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl} where unixtime >= {int(args.since.timestamp())       } and unixtime < {int(args.until.timestamp())       } group by {','.join(PKEY[args.tbl])};")
        DB_from.logger.info("select end: to.")
        dfbase = dfbase.groupby(PKEY[args.tbl]).first().reset_index()
        dfwk   = dfwk  .groupby(PKEY[args.tbl]).first().reset_index()
        if "unixtime" in PKEY[args.tbl]:
            dfbase["_unixtime"] = dfbase["unixtime"].copy()
            dfbase["unixtime" ] = (dfbase["unixtime"] // 1000).astype(int)
    else:
        assert False
        dfbase = DB_from.select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl} group by {','.join(PKEY[args.tbl])};")
        dfwk   = DB_to  .select_sql(f"select {','.join(PKEY[args.tbl])} from {args.tbl} group by {','.join(PKEY[args.tbl])};")
    if len(PKEY[args.tbl]) == 1:
        dfbase = dfbase.loc[~dfbase[PKEY[args.tbl][0]].isin(dfwk[PKEY[args.tbl][0]].unique())]
        dfbase = dfbase.groupby(PKEY[args.tbl][0]).first().reset_index(drop=False)
    else:
        dfwk["__work"] = 0
        dfbase = pd.merge(dfbase, dfwk, how="left", on=PKEY[args.tbl])
        dfbase = dfbase.loc[dfbase["__work"].isna()]
        dfbase = dfbase.groupby(PKEY[args.tbl]).first().reset_index(drop=False).iloc[:, :-1]
    DB_from.logger.info("delete duplicated data end.")
    assert dfbase.shape[0] > 0
    if "unixtime" in PKEY[args.tbl]:
        dfbase["unixtime"] = dfbase["_unixtime"].copy()
        dfbase = dfbase.sort_values("unixtime").reset_index(drop=True)
    for index in tqdm(np.array_split(np.arange(dfbase.shape[0]), dfbase.shape[0] // args.num)):
        dfwk = dfbase.iloc[index].copy()
        list_pkey = PKEY[args.tbl].copy()
        if "unixtime" in list_pkey:
            sql_unixtime = f"unixtime >= {int(dfwk['unixtime'].min())} and unixtime <= {int(dfwk['unixtime'].max())}"
            if args.useunittime == False:
                list_pkey = np.array(list_pkey)[~np.isin(np.array(list_pkey), "unixtime")].tolist()
        else:
            sql_unixtime = f"unixtime >= {int(args.since.timestamp()) * 1000} and unixtime < {int(args.until.timestamp()) * 1000} "
        sql  = (
            f"select main.*, " + 
            f"scale_pre->>'price' as scale_pre_price,scale_pre->>'size' as scale_pre_size,scale_pre->>'bid' as scale_pre_bid,scale_pre->>'ask' as scale_pre_ask,scale_pre->>'last_traded_price' as scale_pre_last_traded_price,scale_pre->>'index_price' as scale_pre_index_price,scale_pre->>'mark_price' as scale_pre_mark_price,scale_pre->>'funding_rate' as scale_pre_funding_rate,scale_pre->>'bid_size' as scale_pre_bid_size,scale_pre->>'ask_size' as scale_pre_ask_size,scale_pre->>'volume' as scale_pre_volume,scale_pre->>'open_interest' as scale_pre_open_interest,scale_pre->>'open_interest_value' as scale_pre_open_interest_value,scale_pre->>'turnover' as scale_pre_turnover,scale_pre->>'price_open' as scale_pre_price_open,scale_pre->>'price_high' as scale_pre_price_high,scale_pre->>'price_low' as scale_pre_price_low,scale_pre->>'price_close' as scale_pre_price_close, " + 
            f"scale_aft->>'price' as scale_aft_price,scale_aft->>'size' as scale_aft_size,scale_aft->>'bid' as scale_aft_bid,scale_aft->>'ask' as scale_aft_ask,scale_aft->>'last_traded_price' as scale_aft_last_traded_price,scale_aft->>'index_price' as scale_aft_index_price,scale_aft->>'mark_price' as scale_aft_mark_price,scale_aft->>'funding_rate' as scale_aft_funding_rate,scale_aft->>'bid_size' as scale_aft_bid_size,scale_aft->>'ask_size' as scale_aft_ask_size,scale_aft->>'volume' as scale_aft_volume,scale_aft->>'open_interest' as scale_aft_open_interest,scale_aft->>'open_interest_value' as scale_aft_open_interest_value,scale_aft->>'turnover' as scale_aft_turnover,scale_aft->>'price_open' as scale_aft_price_open,scale_aft->>'price_high' as scale_aft_price_high,scale_aft->>'price_low' as scale_aft_price_low,scale_aft->>'price_close' as scale_aft_price_close  " + 
            f"from {tbl2} as main " + 
            f"left join master_symbol as mst on main.symbol = mst.symbol_id " +
            f"where {sql_unixtime} "
        )
        if len(list_pkey) == 1:
            val = dfwk[list_pkey[0]].iloc[0]
            if (str.isdigit(str(val)) == False) or ((args.tbl == "bybit_executions") and (list_pkey[0] == "id")):
                sql += f"and main.{list_pkey[0]} in ('" + "','".join(dfwk[list_pkey[0]].astype(str).tolist()) + "')"
            else:
                sql += f"and main.{list_pkey[0]} in (" + ",".join(dfwk[list_pkey[0]].astype(str).tolist()) + ")"
        elif len(list_pkey) > 1:
            sql += f"and ({','.join([f'main.{x}' for x in list_pkey])}) in (" + ",".join([f"({','.join(x.astype(str).tolist())})" for x in dfwk[list_pkey].values]) + ")"
        df_insert = DB_from.select_sql(sql)
        df_insert["unixtime"] = (df_insert["unixtime"] // 1000).astype(int)
        for x in df_insert.columns[df_insert.columns.str.contains("^scale_aft_", regex=True)].tolist():
            df_insert[x] = df_insert[x].astype(np.float64)
            df_insert[x.replace("scale_aft", "scale_pre")] = df_insert[x.replace("scale_aft", "scale_pre")].astype(np.float64)
            df_insert.loc[df_insert[x].isna(), x] = (1.0 / df_insert.loc[df_insert[x].isna(), x.replace("scale_aft", "scale_pre")].copy())
            if x.replace("scale_aft_", "") in df_insert.columns: df_insert[x.replace("scale_aft_", "")] = (df_insert[x.replace("scale_aft_", "")] * df_insert[x]).astype(np.float64)
        if df_insert.columns.isin(["type"]).any():
            df_insert["side"] = df_insert["type"].map({"Buy": 0, "Sell": 1, "BUY": 0, "SELL": 1, "asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int) # nan = 板寄せ
        if args.isgroupby:
            df_insert = df_insert.groupby(PKEY[args.tbl]).last().reset_index(drop=False)
        if args.update and df_insert.shape[0] > 0:
            DB_to.insert_from_df(df_insert, args.tbl, is_select=True, n_jobs=args.jobs)
            DB_to.execute_sql()
