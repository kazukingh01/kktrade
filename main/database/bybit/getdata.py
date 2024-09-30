import datetime, requests, time, argparse, json
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from kkpsgre.util.com import strfind
from kkpsgre.comapi import select, insert
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


EXCHANGE = "bybit"
URL_BASE = "https://api.bybit.com/v5/"
KLINE_TYPE = {
    "normal": 0,
    "mark": 1,
    "index": 2,
    "premium": 3,
}
KILNE_URL = {
    "normal": "kline",
    "mark": "mark-price-kline",
    "index": "index-price-kline",
    "premium": "premium-index-price-kline",
}
LOGGER = set_logger(__name__)
FUNCTIONS = [
    "getorderbook",
    "getticker",
    "getexecutions",
    "getkline",
    "getallkline",
]


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

def getorderbook(symbol: str="inverse@BTCUSD", count_max: int=100, mst_id: dict=None):
    r   = requests.get(f"{URL_BASE}market/orderbook", params=dict({"limit": count_max}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df1 = pd.DataFrame(r.json()["result"]["b"], columns=["price", "size"])
    df1["side"] = "bids"
    df2 = pd.DataFrame(r.json()["result"]["a"], columns=["price", "size"])
    df2["side"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
    ], axis=0, ignore_index=True)
    df["unixtime"] = datetime.datetime.fromtimestamp(r.json()["result"]["ts"] / 1000, tz=datetime.UTC)
    df["side"]     = df["side"].map({"asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    return df

def getticker(symbol: str="inverse@BTCUSD", mst_id: dict=None):
    r  = requests.get(f"{URL_BASE}market/tickers", params=dict({}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()["result"]["list"])
    conv_dict = {
        "bid1Price": "bid",
        "ask1Price": "ask",
        "bid1Size": "bid_size",
        "ask1Size": "ask_size",
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
    df["unixtime"] = datetime.datetime.fromtimestamp(r.json()["time"] / 1000, tz=datetime.UTC)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    for x in [
        "bid", "ask", "last_traded_price", "index_price", "mark_price", "funding_rate",
        "bid_size", "ask_size", "volume", "open_interest", "open_interest_value", "turnover",
    ]:
        if x not in df.columns: continue
        df[x] = df[x].astype(float)
    return df

def getexecutions(symbol: str="inverse@BTCUSD", mst_id: dict=None):
    r  = requests.get(f"{URL_BASE}market/recent-trade", params=dict({"limit": 1000}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()["result"]["list"])
    if df.shape[0] == 0: return df
    df["id"]       = df["execId"].astype(str)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1}).astype(float).fillna(-1).astype(int)
    df["unixtime"] = pd.to_datetime(df['time'].astype(int), unit='ms', utc=True)
    df["is_block_trade"] = df["isBlockTrade"].astype(bool)
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    return df

def getkline(kline: str, symbol: str="inverse@BTCUSD", interval=1, start: int=None, end: int=None, limit: int=200, mst_id: dict=None):
    assert isinstance(kline, str) and kline in list(KLINE_TYPE.keys())
    assert interval in [1, 3, 5, 15, 30, 60, 120, 240, 360, 720, "D", "M", "W"]
    if fnuc_parse(symbol)["category"] == "spot" and kline != "normal": return pd.DataFrame()
    r = requests.get(f"{URL_BASE}market/{KILNE_URL[kline]}", params=dict({"interval": interval, "start": start, "end": end, "limit": limit}, **fnuc_parse(symbol)))
    assert r.status_code == 200
    assert r.json()["retCode"] == 0
    if kline == "normal":
        df = pd.DataFrame(r.json()["result"]["list"], columns=["unixtime", "price_open", "price_high", "price_low", "price_close", "volume", "turnover"])
    else:
        df = pd.DataFrame(r.json()["result"]["list"], columns=["unixtime", "price_open", "price_high", "price_low", "price_close"])
        for x in ["volume", "turnover"]: df[x] = float("nan")
    if df.shape[0] == 0: return df
    df["unixtime"]   = pd.to_datetime(df["unixtime"].astype(int), unit='ms', utc=True)  
    df["symbol"]     = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["kline_type"] = KLINE_TYPE[kline]
    df["interval"]   = {"D": 60*60*24, "W": 60*60*24*7, "M": -1}[interval] if isinstance(interval, str) else (interval * 60)
    # It might not be unique with symbol, unixtime, kline_type, interval. so gorupby.last() is better solution.
    df = df.sort_values(["symbol", "kline_type", "interval", "unixtime"]).reset_index(drop=True)
    df = df.groupby(["symbol", "kline_type", "interval", "unixtime"]).last().reset_index(drop=False)
    for x in ["price_open", "price_high", "price_low", "price_close", "volume", "turnover"]:
        df[x] = df[x].astype(float)
    return df
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=lambda x: FUNCTIONS[eval(x)] if strfind(r"^[0-9]+$", x) else x)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--ip",   type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    src    = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    df_mst = select(src, f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    if args.fn in [x for x in FUNCTIONS if x != "getallkline"]:
        while True:
            if "getorderbook" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    df = getorderbook(symbol=symbol, count_max=50, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_orderbook", False, add_sql=None)
                time.sleep(10) # 11 * 6 = 66
            if "getticker" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    df = getticker(symbol=symbol, mst_id=mst_id) # result value is only 1 record.
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_ticker", True, add_sql=None)
                time.sleep(1) # 11 * 60 = 660
            if "getexecutions" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    df = getexecutions(symbol=symbol, mst_id=mst_id)
                    if df.shape[0] > 0:
                        df_exist = select(src, (
                            f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
                        ))
                        df       = df.loc[~df["id"].isin(df_exist["id"])]
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_executions", True, add_sql=None)
                time.sleep(5) # 11 * 12 = 132
            if "getkline" == args.fn:
                for symbol in mst_id.keys():
                    for kline in list(KLINE_TYPE.keys()):
                        LOGGER.info(f"{args.fn}: {symbol}, {kline}")
                        df = getkline(kline, symbol=symbol, interval=1, limit=100, mst_id=mst_id)
                        if df.shape[0] > 0:
                            df_exist = select(src, (
                                f"select unixtime from {EXCHANGE}_kline where symbol = {df['symbol'].iloc[0]} and kline_type = {df['kline_type'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                                f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
                            ))
                            df = df.loc[~df["unixtime"].isin(df_exist["unixtime"])]
                        if df.shape[0] > 0 and args.update:
                            insert(src, df, f"{EXCHANGE}_kline", True, add_sql=None)
                time.sleep(60)
    if "getallkline" == args.fn:
        assert args.fr is not None and args.to is not None
        date_since, date_until = args.fr, args.to
        for date in [date_since + datetime.timedelta(days=x) for x in range((date_until - date_since).days + 1)]:
            for hour in [0, 12]:
                time_since = int((date + datetime.timedelta(hours=hour+ 0)).timestamp() * 1000)
                time_until = int((date + datetime.timedelta(hours=hour+12)).timestamp() * 1000)
                for symbol in mst_id.keys():
                    for kline in list(KLINE_TYPE.keys()):
                        LOGGER.info(f"{args.fn}: {date}, {hour}, {symbol}, {kline}")
                        df = getkline(kline, symbol=symbol, interval=1, start=time_since, end=time_until, limit=1000, mst_id=mst_id)
                        if df.shape[0] > 0:
                            df_exist = select(src, (
                                f"select unixtime from {EXCHANGE}_kline where symbol = {df['symbol'].iloc[0]} and kline_type = {df['kline_type'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                                f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
                            ))
                            df = df.loc[~df["unixtime"].isin(df_exist["unixtime"])]
                        if df.shape[0] > 0 and args.update:
                            insert(src, df, f"{EXCHANGE}_kline", True, add_sql=None)
