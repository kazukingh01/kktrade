import datetime, requests, time, argparse
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME

EXCHANGE = "bitflyer"
URL_BASE = "https://api.bitflyer.com/v1/"
STATE = {
    "RUNNING": 0,
    "CLOSED": 1,
    "STARTING": 2,
    "PREOPEN": 3,
    "CIRCUIT BREAK": 4,
    "AWAITING SQ": 5,
    "MATURED": 6,
}


func_to_unixtime = np.vectorize(lambda x: x.timestamp())

def getmarkets():
    r  = requests.get(f"{URL_BASE}getmarkets")
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    return df

def getorderbook(symbol: str="BTC_JPY", count_max: int=100, mst_id: dict=None, scale_pre: dict=None):
    r    = requests.get(f"{URL_BASE}getboard", params={"product_code": symbol})
    assert r.status_code == 200
    time = int(datetime.datetime.now().timestamp())
    df1  = pd.DataFrame(r.json()["bids"])
    df1["side"] = "bids"
    df2 = pd.DataFrame(r.json()["asks"])
    df2["side"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
        pd.DataFrame([{"side": "mprc", "price": r.json()["mid_price"]}])
    ], axis=0, ignore_index=True)
    df["unixtime"] = time
    df["side"]     = df["side"].map({"asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    for x in ["price", "size"]:
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    return df

def getticker(symbol: str="BTC_JPY", mst_id: dict=None, scale_pre: dict=None):
    r  = requests.get(f"{URL_BASE}getticker", params={"product_code": symbol})
    assert r.status_code == 200
    df = pd.DataFrame([r.json()])
    conv_dict = {
        "best_bid": "bid",
        "best_ask": "ask",
        "best_bid_size": "bid_size",
        "best_ask_size": "ask_size",
        "ltp": "last_traded_price",
    }
    df.columns = df.columns.map(lambda x: conv_dict[x] if conv_dict.get(x) is not None else x)
    for x in [
        "bid", "ask", "bid_size", "ask_size", "total_bid_depth", "total_ask_depth",
        "market_bid_size", "market_ask_size", "volume", "volume_by_product", "last_traded_price",
    ]:
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    df["unixtime"] = func_to_unixtime(pd.to_datetime(df["timestamp"]).dt.to_pydatetime())
    df["unixtime"] = df["unixtime"].astype(int)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["state"]    = df["state"].map(STATE).fillna(-1).astype(int)
    return df

def getexecutions(symbol: str="BTC_JPY", before: int=None, after: int=None, mst_id: dict=None, scale_pre: dict=None):
    r  = requests.get(f"{URL_BASE}getexecutions", params={
        "product_code": symbol, "count": 10000, "before": before, "after": after,
    })
    if r.status_code in [400]: return pd.DataFrame()
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["symbol"] = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["side"]   = df["side"].map({"BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int) # nan = 板寄せ
    for x in ["price", "size"]:
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    df["unixtime"] = func_to_unixtime(pd.to_datetime(df["exec_date"]).dt.to_pydatetime())
    df["unixtime"] = df["unixtime"].astype(int)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=str, help="--fr 20200101")
    parser.add_argument("--to", type=str, help="--to 20200101")
    parser.add_argument("--sec", type=int, default=60)
    parser.add_argument("--cnt", type=int, default=20)
    parser.add_argument("--update", action='store_true', default=False)
    args      = parser.parse_args()
    DB        = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst    = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id    = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    scale_pre = {x:y for x, y in df_mst[["symbol_name", "scale_pre"]].values}
    if args.fn in ["getorderbook", "getticker", "getexecutions"]:
        while True:
            if "getorderbook" == args.fn:
                for symbol in mst_id.keys():
                    df = getorderbook(symbol=symbol, count_max=50, mst_id=mst_id, scale_pre=scale_pre[symbol])
                    if df.shape[0] > 0 and args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_orderbook", set_sql=True, str_null="")
                        DB.execute_sql()
                time.sleep(10)
            elif "getticker" == args.fn:
                for symbol in mst_id.keys():
                    df   = getticker(symbol=symbol, mst_id=mst_id, scale_pre=scale_pre[symbol])
                    dfwk = DB.select_sql(f"select tick_id from {EXCHANGE}_ticker where tick_id = {df['tick_id'].iloc[0]} and symbol = {mst_id[symbol]};")
                    if dfwk.shape[0] == 0 and args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_ticker", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(4)
            elif "getexecutions" == args.fn:
                for symbol in mst_id.keys():
                    dfwk = DB.select_sql(f"select max(id) as id from {EXCHANGE}_executions where symbol = {mst_id[symbol]};")
                    df   = getexecutions(symbol=symbol, after=dfwk["id"].iloc[0], mst_id=mst_id, scale_pre=scale_pre[symbol])
                    if df.shape[0] > 0 and args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(10)
    if args.fn in ["getall"]:
        time_since = int(datetime.datetime.fromisoformat(args.fr).timestamp()) if args.fr is not None else int(datetime.datetime.fromisoformat("20000101").timestamp())
        time_until = int(datetime.datetime.fromisoformat(args.to).timestamp()) if args.to is not None else int(datetime.datetime.now().timestamp())
        over_sec   = args.sec
        over_count = args.cnt
        dfwk = DB.select_sql(f"select symbol, id, unixtime from {EXCHANGE}_executions where unixtime <= {time_until} and unixtime >= {time_since};")
        if dfwk.shape[0] > 0:
            dfwk = dfwk.sort_values(["symbol", "unixtime", "id"]).reset_index(drop=True)
            dfwk["id_prev"]       = np.concatenate(dfwk.groupby("symbol").apply(lambda x: [-1] + x["id"      ].tolist()[:-1]).values).reshape(-1)
            dfwk["unixtime_prev"] = np.concatenate(dfwk.groupby("symbol").apply(lambda x: [-1] + x["unixtime"].tolist()[:-1]).values).reshape(-1)
            dfwk["diff"] = dfwk["unixtime"] - dfwk["unixtime_prev"]
            dfwk["bool"] = (dfwk["diff"] >= over_sec)
            dfwk = dfwk.sort_values("diff", ascending=False)
            DB.logger.info(f'Target num: {dfwk["bool"].sum()}')
            count = 0
            for index in dfwk.index[dfwk["bool"]]:
                idb    = dfwk.loc[index, "id"]
                ida    = dfwk.loc[index, "id_prev"] if dfwk.loc[index, "unixtime_prev"] > 0 else None
                symbol = {y:x for x, y in mst_id.items()}[dfwk.loc[index, "symbol"]]
                DB.logger.info(f'idb: {idb}, ida: {ida}, symbol: {symbol}, diff: {dfwk.loc[index, "diff"]}')
                df     = getexecutions(symbol=symbol, before=idb, after=ida, mst_id=mst_id, scale_pre=scale_pre[symbol])
                if df.shape[0] > 0:
                    df_exist = DB.select_sql(f"select symbol, id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and id in ({','.join(df['id'].astype(str).tolist())});")
                    df       = df.loc[~df["id"].isin(df_exist["id"])]
                if df.shape[0] > 0:
                    count = 0
                    if args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                else:
                    count += 1
                if count > over_count: break
                time.sleep(0.6)
