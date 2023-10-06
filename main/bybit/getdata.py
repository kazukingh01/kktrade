import datetime, requests, time, sys, re
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS
from kktrade.config.com import SCALE_MST, NAME_MST

DBNAME   = "bybit"
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


# DB = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)


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
    args = sys.argv[1:]
    if "getorderbook" in args or "getticker" in args or "getexecutions" in args:
        while True:
            if "getorderbook" in args:
                for symbol in SCALE.keys():
                    df = getorderbook(symbol=symbol)
                    DB.insert_from_df(df, "orderbook", set_sql=True, str_null="")
                    DB.execute_sql()
                time.sleep(10) # 4 * 6 = 24
            if "getticker" in args:
                for symbol in SCALE.keys():
                    df   = getticker(symbol=symbol)
                    dfwk = DB.select_sql(f"select tick_id from ticker where tick_id = {df['tick_id'].iloc[0]} and symbol = {NAME_MST[symbol]};")
                    if dfwk.shape[0] == 0:
                        DB.insert_from_df(df, "ticker", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(5) # 4 * 12 = 48
            if "getexecutions" in args:
                for symbol in SCALE.keys():
                    dfwk = DB.select_sql(f"select max(id) as id from executions where symbol = {NAME_MST[symbol]};")
                    df   = getexecutions(symbol=symbol, after=dfwk["id"].iloc[0])
                    if df.shape[0] > 0:
                        DB.insert_from_df(df, "executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(10) # 4 * 6 = 24
    if "getall" in args:
        if len(re.findall("^[0-9]+$", args[-3])) > 0 and len(re.findall("^[0-9]+$", args[-2])) > 0 and len(re.findall("^[0-9]+$", args[-1])) > 0:
            over_sec   = int(args[-2])
            over_count = int(args[-1])
            time_until = int(datetime.datetime.fromisoformat(args[-3]).timestamp())
        else:
            over_sec   = 60
            over_count = 20
            time_until = int(datetime.datetime.now().timestamp())
        dfwk = DB.select_sql(f"select symbol, id, unixtime from executions where unixtime <= {time_until};")
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
            symbol = {y:x for x, y in NAME_MST.items()}[dfwk.loc[index, "symbol"]]
            DB.logger.info(f'idb: {idb}, ida: {ida}, symbol: {symbol}')
            df     = getexecutions(symbol=symbol, before=idb, after=ida)
            if df.shape[0] > 0:
                count = 0
                DB.insert_from_df(df, "executions", set_sql=True, str_null="", is_select=True)
                DB.execute_sql()
            else:
                count += 1
            if count > over_count: break
            time.sleep(0.6)
