import datetime, requests, time, sys
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import CONNECTION_STRING

URL_BASE = "https://api.bitflyer.com/v1/"

SCALE_MST = {
    0: [0, 5],
    1: [2, 2],
    2: [3, 2],
}

SCALE = {
    "BTC_JPY": 0,
    "XRP_JPY": 1,
    "ETH_JPY": 0,
    # "XLM_JPY": 2,
    # "MONA_JPY": 2,
    "FX_BTC_JPY": 0,
}

STATE = {
    "RUNNING": 0,
    "CLOSED": 1,
    "STARTING": 2,
    "PREOPEN": 3,
    "CIRCUIT BREAK": 4,
    "AWAITING SQ": 5,
    "MATURED": 6,
}

DB = Psgre(CONNECTION_STRING, max_disp_len=200)


func_to_unixtime = np.vectorize(lambda x: x.timestamp())

def getmarkets():
    r  = requests.get(f"{URL_BASE}getmarkets")
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    return df

def getboard(symbol: str="BTC_JPY", count_max: int=200):
    r    = requests.get(f"{URL_BASE}getboard", params={"product_code": symbol})
    assert r.status_code == 200
    time = int(datetime.datetime.now().timestamp())
    df1  = pd.DataFrame(r.json()["bids"])
    df1["type"] = "bids"
    df2 = pd.DataFrame(r.json()["asks"])
    df2["type"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
        pd.DataFrame([{"type": "mprc", "price": r.json()["mid_price"]}])
    ], axis=0, ignore_index=True)
    df["unixtime"] = time
    df["symbol"]   = symbol
    df["scale"]    = SCALE[symbol]
    df["price"]    = (df["price"] * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ] * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    return df

def getticker(symbol: str="BTC_JPY"):
    r  = requests.get(f"{URL_BASE}getticker", params={"product_code": symbol})
    assert r.status_code == 200
    df = pd.DataFrame([r.json()])
    for x in ["best_bid", "best_ask", "ltp"]:
        df[x] = (df[x] * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    for x in [
        "best_bid_size", "best_ask_size", "total_bid_depth", "total_ask_depth",
        "market_bid_size", "market_ask_size", "volume", "volume_by_product"
    ]:
        df[x] = (df[x] * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    df["unixtime"] = func_to_unixtime(pd.to_datetime(df["timestamp"]).dt.to_pydatetime())
    df["unixtime"] = df["unixtime"].astype(int)
    df["symbol"]   = symbol
    df["state"]    = df["state"].map(STATE).fillna(-1).astype(int)
    df["scale"]    = SCALE[symbol]
    df["last_traded_price"] = df["ltp"]
    return df

def getexecutions(symbol: str="BTC_JPY", before: int=None, after: int=None):
    r  = requests.get(f"{URL_BASE}getexecutions", params={
        "product_code": symbol, "count": 10000, "before": before, "after": after,
    })
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["symbol"]   = symbol
    df["scale"]    = SCALE[symbol]
    df["type"]     = df["side"].copy()
    df["price"]    = (df["price"] * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ] * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    df["unixtime"] = func_to_unixtime(pd.to_datetime(df["exec_date"]).dt.to_pydatetime())
    df["unixtime"] = df["unixtime"].astype(int)
    return df


if __name__ == "__main__":
    args = sys.argv[1:]
    if "getboard" in args or "getticker" in args or "getexecutions" in args:
        while True:
            if "getboard" in args:
                for symbol in SCALE.keys():
                    df = getboard(symbol=symbol)
                    DB.insert_from_df(df, "board", set_sql=True, str_null="")
                    DB.execute_sql()
                time.sleep(10) # 4 * 6 = 24
            if "getticker" in args:
                for symbol in SCALE.keys():
                    df   = getticker(symbol=symbol)
                    dfwk = DB.select_sql(f"select tick_id from ticker where tick_id = {df['tick_id'].iloc[0]} and symbol = '{symbol}';")
                    if dfwk.shape[0] == 0:
                        DB.insert_from_df(df, "ticker", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(5) # 4 * 12 = 48
            if "getexecutions" in args:
                for symbol in SCALE.keys():
                    dfwk = DB.select_sql(f"select max(id) as id from executions where symbol = '{symbol}';")
                    df   = getexecutions(symbol=symbol, after=dfwk["id"].iloc[0])
                    if df.shape[0] > 0:
                        DB.insert_from_df(df, "executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(10) # 4 * 6 = 24
    if "getall" in args:
        dfwk = DB.select_sql(f"select symbol, id, unixtime from executions;")
        dfwk = dfwk.sort_values(["symbol", "unixtime", "id"]).reset_index(drop=True)
        dfwk["unixtime_prev"] = np.concatenate(dfwk.groupby("symbol").apply(lambda x: [-1] + x["unixtime"].tolist()[:-1]).values).reshape(-1)
        dfwk["diff"] = dfwk["unixtime"] - dfwk["unixtime_prev"]
        dfwk["bool"] = (dfwk["diff"] > 9)
        DB.logger.info(f'Target num: {dfwk["bool"].sum()}')
        for x in dfwk.index[dfwk["bool"]]:
            idb    = dfwk.loc[x  , "id"]
            ida    = dfwk.loc[x-1, "id"] if dfwk.loc[x  , "unixtime_prev"] > 0 else None
            symbol = dfwk.loc[x  , "symbol"]
            DB.logger.info(f'idb: {idb}, ida: {ida}, symbol: {symbol}')
            df     = getexecutions(symbol=symbol, before=idb, after=ida)
            if df.shape[0] > 0:
                DB.insert_from_df(df, "executions", set_sql=True, str_null="", is_select=True)
                DB.execute_sql()
            time.sleep(10)