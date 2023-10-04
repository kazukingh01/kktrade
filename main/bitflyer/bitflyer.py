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
    "XLM_JPY": 2,
    "MONA_JPY": 2,
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

DB = Psgre(CONNECTION_STRING)


func_to_unixtime = np.vectorize(lambda x: x.timestamp())

def getmarkets():
    r  = requests.get(f"{URL_BASE}getmarkets")
    df = pd.DataFrame(r.json())
    print(df)

def getboard(symbol: str="BTC_JPY", count_max: int=200, is_update: bool=False):
    r    = requests.get(f"{URL_BASE}getboard", params={"product_code": symbol})
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
    if is_update:
        DB.insert_from_df(df, "board", set_sql=True, str_null="")
        DB.execute_sql()

def getticker(symbol: str="BTC_JPY", is_update: bool=False):
    r  = requests.get(f"{URL_BASE}getticker", params={"product_code": symbol})
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
    if is_update:
        DB.insert_from_df(df, "ticker", set_sql=True, str_null="", is_select=True)
        DB.execute_sql()

def getexecutions(symbol: str="BTC_JPY", before: int=None, after: int=None, is_update: bool=False):
    r  = requests.get(f"{URL_BASE}getexecutions", params={
        "product_code": symbol, "count": 10000, "before": before, "after": after,
    })
    df = pd.DataFrame(r.json())
    df["symbol"]   = symbol
    df["scale"]    = SCALE[symbol]
    df["type"]     = df["side"]
    df["price"]    = (df["price"] * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ] * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    df["unixtime"] = func_to_unixtime(pd.to_datetime(df["exec_date"]).dt.to_pydatetime())
    df["unixtime"] = df["unixtime"].astype(int)
    if is_update:
        DB.insert_from_df(df, "executions", set_sql=True, str_null="", is_select=True)
        DB.execute_sql()


if __name__ == "__main__":
    args = sys.argv[1:]
    if "getboard" in args or "getticker" in args or "getexecutions" in args:
        while True:
            if "getboard" in args:
                for symbol in SCALE.keys():
                    try: getboard(symbol=symbol, is_update=True)
                    except Exception as e: print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} getboard symbol: {symbol}. {e}")
                time.sleep(1)
            if "getticker" in args:
                for symbol in SCALE.keys():
                    try: getticker(symbol=symbol, is_update=True)
                    except Exception as e: print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} getticker symbol: {symbol}. {e}")
                time.sleep(1)
            if "getexecutions" in args:
                for symbol in SCALE.keys():
                    try:
                        dfwk = DB.select_sql(f"select max(id) as id from executions where symbol = '{symbol}';")
                        getexecutions(symbol=symbol, is_update=True, after=dfwk["id"].iloc[0])
                    except Exception as e:
                        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} getexecutions symbol: {symbol}. {e}")
                time.sleep(1)
