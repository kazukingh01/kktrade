import datetime, requests, time, sys, re
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.com import SCALE_MST, NAME_MST

EXCHANGE = "bybit"
URL_BASE = "https://api.bybit.com/v5/"

SCALE = {
    "spot@BTCUSDT": 3,
    "spot@ETHUSDC": 4,
    "spot@BTCUSDC": 3,
    "spot@ETHUSDT": 4,
    "spot@XRPUSDT": 5,
    "linear@BTCUSDT": 6,
    "linear@ETHUSDT": 1,
    "linear@XRPUSDT": 7,
    "inverse@BTCUSD": 8,
    "inverse@ETHUSD": 9,
    "inverse@XRPUSD": 7,
}

func_to_unixtime = np.vectorize(lambda x: x.timestamp())
fnuc_parse = lambda x: {"category": x.split("@")[0], "symbol": x.split("@")[-1]}


def getinstruments():
    list_df = []
    for x in ["linear", "inverse", "option", "spot"]:
        r  = requests.get(f"{URL_BASE}market/instruments-info", params={"category": x})
        assert r.status_code == 200
        df = pd.DataFrame(r.json()["result"]["list"])
        df["category"] = x
        list_df.append(df.copy())
    df = pd.concat(list_df, axis=0, ignore_index=True)
    return df

def getorderbook(symbol: str=list(SCALE.keys())[0], count_max: int=100):
    r   = requests.get(f"{URL_BASE}market/orderbook", params=dict({"limit": count_max}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df1 = pd.DataFrame(r.json()["result"]["b"], columns=["price", "size"])
    df1["type"] = "bids"
    df2 = pd.DataFrame(r.json()["result"]["a"], columns=["price", "size"])
    df2["type"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
    ], axis=0, ignore_index=True)
    df["unixtime"] = r.json()["result"]["ts"]
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    df["price"]    = (df["price"].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    return df

def getticker(symbol: str=list(SCALE.keys())[0]):
    r  = requests.get(f"{URL_BASE}market/tickers", params=dict({}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()["result"]["list"])
    conv_dict = {
        "bid1Price": "best_bid",
        "ask1Price": "best_ask",
        "bid1Size": "best_bid_size",
        "ask1Size": "best_ask_size",
        "lastPrice": "last_traded_price",
        "indexPrice": "index_price",
        "markPrice": "mark_price",
        "openInterest": "open_interest",
        "openInterestValue": "open_interest_value",
        "turnover24h": "turnover",
        "volume24h": "volume",
        "fundingRate": "funding_rate",
    }
    df = df.loc[:, df.columns.isin(list(conv_dict.keys()))]
    df.columns = df.columns.map(conv_dict)
    for x in [
        "best_bid", "best_ask", "last_traded_price", "index_price",
        "mark_price", "funding_rate"
    ]:
        if x not in df.columns: continue
        df[x] = (df[x].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    for x in ["best_bid_size", "best_ask_size", "volume"]:
        df[x] = (df[x].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    for x in ["open_interest", "open_interest_value", "turnover"]:
        if x not in df.columns: continue
        df[x] = df[x].astype(float).fillna(-1).astype(int)
    df["unixtime"] = int(r.json()["time"])
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    return df

def getexecutions(symbol: str=list(SCALE.keys())[0]):
    r  = requests.get(f"{URL_BASE}market/recent-trade", params=dict({"limit": 1000}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()["result"]["list"])
    if df.shape[0] == 0: return df
    df["id"]       = df["execId"].astype(str)
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    df["type"]     = df["side"].map({"Sell": "SELL", "Buy": "BUY"}).fillna("OTHR")
    df["price"]    = (df["price"].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    df["unixtime"] = df["time"].astype(int)
    df["is_block_trade"] = df["isBlockTrade"].astype(bool)
    return df


if __name__ == "__main__":
    DB   = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    args = sys.argv[1:]
    if "getorderbook" in args or "getticker" in args or "getexecutions" in args:
        while True:
            if "getorderbook" in args:
                for symbol in SCALE.keys():
                    df = getorderbook(symbol=symbol, count_max=50)
                    DB.insert_from_df(df, f"{EXCHANGE}_orderbook", set_sql=True, str_null="")
                    DB.execute_sql()
                time.sleep(10) # 11 * 6 = 66
            if "getticker" in args:
                for symbol in SCALE.keys():
                    df   = getticker(symbol=symbol)
                    dfwk = DB.select_sql(f"select unixtime from {EXCHANGE}_ticker where unixtime = {df['unixtime'].iloc[0]} and symbol = {NAME_MST[symbol]};")
                    if dfwk.shape[0] == 0:
                        DB.insert_from_df(df, f"{EXCHANGE}_ticker", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(1) # 11 * 60 = 660
            if "getexecutions" in args:
                for symbol in SCALE.keys():
                    dfwk = DB.select_sql(f"select max(id) as id from {EXCHANGE}_executions where symbol = {NAME_MST[symbol]};")
                    df   = getexecutions(symbol=symbol)
                    if df.shape[0] > 0:
                        df_exist = DB.select_sql(
                            f"select symbol, id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and id in ('" + "','".join(df['id'].astype(str).tolist()) + "') and " + 
                            f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                        )
                        df       = df.loc[~df["id"].isin(df_exist["id"])]
                    if df.shape[0] > 0:
                        DB.insert_from_df(df, f"{EXCHANGE}_executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(5) # 11 * 12 = 132
