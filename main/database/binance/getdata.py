import datetime, requests, time, argparse
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME


# Spot Trading / Spot
# Spot Trading / Margin
# Derivatives Trading / USDS-M
# Derivatives Trading / COIN-M ( This is same as invers trading of Bybit )



EXCHANGE = "binance"
URL_BASE = {
    "SPOT": "https://api.binance.com/api/v3/",
    "USDS": "https://fapi.binance.com/fapi/v1/",
    "COIN": "https://dapi.binance.com/dapi/v1/",
}
KLINE_TYPE = {
    "normal": 0,
    "mark": 1,
    "index": 2,
    "premium": 3,
}
KILNE_URL = {
    "normal":  "klines",
    "mark":    "markPriceKlines",
    "index":   "indexPriceKlines",
    "premium": "premiumIndexKlines",
}
LS_RATIO_TYPE = {
    "normal": 0,
    "top_position": 1,
    "top_account": 2,
}
LS_RATIO_URL = {
    "normal": "/futures/data/globalLongShortAccountRatio",
    "top_position": "/futures/data/topLongShortPositionRatio",
    "top_account": "/futures/data/topLongShortAccountRatio",
}


fnuc_parse = lambda x: (x.split("@")[0], x.split("@")[-1])


def getexchangeInfo(url_type: str):
    assert url_type in list(URL_BASE.keys())
    r = requests.get(f"{URL_BASE[url_type]}exchangeInfo")
    assert r.status_code == 200
    df = pd.DataFrame(r.json()["symbols"])
    return df

def getorderbook(symbol: str="BTCUSDT", count_max: int=100, mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    r   = requests.get(f"{URL_BASE[url_type]}depth", params=dict({"limit": count_max, "symbol": _symbol}))
    assert r.status_code == 200
    df1 = pd.DataFrame(r.json()["bids"], columns=["price", "size"])
    df1["side"] = "bids"
    df2 = pd.DataFrame(r.json()["asks"], columns=["price", "size"])
    df2["side"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
    ], axis=0, ignore_index=True)
    if "E" in r.json():
        df["unixtime"] = r.json()["E"] // 1000
    else:
        df["unixtime"] = int(datetime.datetime.now().timestamp())
    df["side"]   = df["side"].map({"asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int)
    df["symbol"] = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    return df

def getexecutions(symbol: str="USDS@BTCUSDT", mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    r  = requests.get(f"{URL_BASE[url_type]}trades", params=dict({"limit": 1000, "symbol": _symbol}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["id"]       = df["id"].astype(np.int64)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["side"]     = (~df["isBuyerMaker"]).astype(int) # "Buy": 0, "Sell": 1
    df["unixtime"] = df["time"].astype(int) // 1000
    df["size"]     = df["qty"].astype(float)
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    return df

def getkline(kline: str, symbol: str="USDS@BTCUSDT", interval: str="1m", start: int=None, end: int=None, limit: int=99, mst_id: dict=None):
    """
    getkline("normal",  symbol="SPOT@BTCUSDT")
    getkline("normal",  symbol="USDS@BTCUSDT")
    getkline("mark",    symbol="USDS@BTCUSDT")
    getkline("index",   symbol="USDS@BTCUSDT")
    getkline("premium", symbol="USDS@BTCUSDT")
    getkline("normal",  symbol="COIN@BTCUSD_PERP")
    getkline("mark",    symbol="COIN@BTCUSD_PERP")
    getkline("index",   symbol="COIN@BTCUSD")
    getkline("premium", symbol="COIN@BTCUSD_PERP")
    """
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    assert isinstance(kline, str) and kline in list(KLINE_TYPE.keys())
    assert interval in ["1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"]
    if url_type == "SPOT" and kline != "normal": return pd.DataFrame()
    _symbol = _symbol.replace("_PERP", "") if url_type == "COIN" and kline == "index" else _symbol
    r = requests.get(f"{URL_BASE[url_type]}{KILNE_URL[kline]}", params=dict({"pair" if kline == "index" else "symbol": _symbol, "interval": interval, "startTime": start, "endTime": end, "limit": limit}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json(), columns=[
        "unixtime", "price_open", "price_high", "price_low", "price_close", "volume", "close_time",
        "volume_quote", "n_trades", "taker_buy_base_volume", "taker_buy_quote_volume", "tmp"
    ])
    if df.shape[0] == 0: return df
    if kline != "normal":
        for x in ["volume", "volume_quote", "n_trades", "taker_buy_base_volume", "taker_buy_quote_volume"]:
            df[x] = float("nan")
    df["unixtime"]   = df["unixtime"].astype(int)
    df["symbol"]     = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["kline_type"] = KLINE_TYPE[kline]
    df["interval"]   = {
        "1m":60,"3m":3*60,"5m":5*60,"15m":15*60,"30m":30*60,"1h":60*60,"2h":2*60*60,"4h":4*60*60,"6h":6*60*60,"8h":8*60*60,
        "12h":12*60*60,"1d":24*60*60,"3d":3*24*60*60,"1w":7*24*60*60,"1M":-1
    }[interval]
    # It might not be unique with symbol, unixtime, kline_type, interval. so gorupby.last() is better solution.
    df = df.sort_values(["symbol", "kline_type", "interval", "unixtime"]).reset_index(drop=True)
    df = df.groupby(["symbol", "kline_type", "interval", "unixtime"]).last().reset_index(drop=False)
    df["unixtime"]         = df["unixtime"] // 1000
    df["volume_taker_buy"] = df["taker_buy_base_volume"].astype(float).copy()
    for x in ["price_open", "price_high", "price_low", "price_close", "volume"]:
        df[x] = df[x].astype(float)
    return df

def getfundingrate(symbol: str="USDS@BTCUSDT", start: int=None, end: int=None, limit: int=100, mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    if url_type == "SPOT": return pd.DataFrame()
    r = requests.get(f"{URL_BASE[url_type]}fundingRate", params=dict({"symbol": _symbol, "startTime": start, "endTime": end, "limit": limit}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    df["unixtime"]     = df["fundingTime"] // 1000
    df["symbol"]       = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["funding_rate"] = df["fundingRate"].astype(float)
    df["mark_price"]   = df["markPrice"].astype(float)
    return df

def getopeninterest(symbol: str="USDS@BTCUSDT", interval: str="5m", start: int=None, end: int=None, limit: int=100, mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    assert interval in ["5m","15m","30m","1h","2h","4h","6h","12h","1d"]
    if url_type == "SPOT": return pd.DataFrame()
    url = "/".join(URL_BASE[url_type].split("/")[:-3]) + "/futures/data/openInterestHist"
    if url_type == "COIN": url += "?contractType=PERPETUAL"
    _symbol = _symbol.replace("_PERP", "") if url_type == "COIN" else _symbol
    r   = requests.get(url, params=dict({"pair" if url_type == "COIN" else "symbol": _symbol, "period": interval, "startTime": start, "endTime": end, "limit": limit}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    df["unixtime"] = df["timestamp"] // 1000
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["interval"] = {"5m":5*60,"15m":15*60,"30m":30*60,"1h":60*60,"2h":2*60*60,"4h":4*60*60,"6h":6*60*60,"12h":12*60*60,"1d":24*60*60}[interval]
    df["open_interest"]       = df["sumOpenInterest"].astype(float)
    df["open_interest_value"] = df["sumOpenInterestValue"].astype(float)
    return df

def getlongshortratio(ls_type: str, symbol: str="USDS@BTCUSDT", interval: str="5m", start: int=None, end: int=None, limit: int=100, mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    assert ls_type in list(LS_RATIO_TYPE.keys())
    assert interval in ["5m","15m","30m","1h","2h","4h","6h","12h","1d"]
    if url_type == "SPOT": return pd.DataFrame()
    url = "/".join(URL_BASE[url_type].split("/")[:-3]) + LS_RATIO_URL[ls_type]
    _symbol = _symbol.replace("_PERP", "") if url_type == "COIN" else _symbol
    r   = requests.get(url, params=dict({"pair" if url_type == "COIN" else "symbol": _symbol, "period": interval, "startTime": start, "endTime": end, "limit": limit}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    df["unixtime"] = df["timestamp"] // 1000
    df["ls_type"]  = LS_RATIO_TYPE[ls_type]
    df["interval"] = {"5m":5*60,"15m":15*60,"30m":30*60,"1h":60*60,"2h":2*60*60,"4h":4*60*60,"6h":6*60*60,"12h":12*60*60,"1d":24*60*60}[interval]
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["long"]     = df["longPosition"  ].astype(float) if url_type == "COIN" and ls_type == "top_position" else df["longAccount" ].astype(float)
    df["short"]    = df["shortPosition" ].astype(float) if url_type == "COIN" and ls_type == "top_position" else df["shortAccount"].astype(float)
    df["ls_ratio"] = df["longShortRatio"].astype(float)
    return df

def gettakervolume(symbol: str="USDS@BTCUSDT", interval: str="5m", start: int=None, end: int=None, limit: int=100, mst_id: dict=None):
    url_type, _symbol = fnuc_parse(symbol)
    assert url_type in list(URL_BASE.keys())
    assert interval in ["5m","15m","30m","1h","2h","4h","6h","12h","1d"]
    if url_type == "SPOT": return pd.DataFrame()
    url = "/".join(URL_BASE[url_type].split("/")[:-3]) + ("/futures/data/takerlongshortRatio" if url_type == "USDS" else "/futures/data/takerBuySellVol")
    _symbol = _symbol.replace("_PERP", "") if url_type == "COIN" else _symbol
    if url_type == "COIN": url += "?contractType=PERPETUAL"
    r   = requests.get(url, params=dict({"pair" if url_type == "COIN" else "symbol": _symbol, "period": interval, "startTime": start, "endTime": end, "limit": limit}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    df["unixtime"] = df["timestamp"] // 1000
    df["interval"] = {"5m":5*60,"15m":15*60,"30m":30*60,"1h":60*60,"2h":2*60*60,"4h":4*60*60,"6h":6*60*60,"12h":12*60*60,"1d":24*60*60}[interval]
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    if url_type == "USDS":
        df["sell_volume"] = df["sellVol"].astype(float)
        df["buy_volume"]  = df["buyVol"].astype(float)
    else:
        df["sell_volume"] = df["takerSellVolValue"].astype(float)
        df["buy_volume"]  = df["takerBuyVolValue"].astype(float)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    DB     = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    if args.fn in ["getorderbook", "getexecutions", "getkline", "getfundingrate", "getopeninterest", "getlongshortratio", "gettakervolume"]:
        while True:
            if "getorderbook" == args.fn:
                for symbol in mst_id.keys():
                    DB.logger.info(f"{args.fn}: {symbol}")
                    df = getorderbook(symbol=symbol, count_max=100, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_orderbook", set_sql=True, str_null="")
                        DB.execute_sql()
                time.sleep(10) # 11 * 6 = 66
            if "getexecutions" == args.fn:
                for symbol in mst_id.keys():
                    DB.logger.info(f"{args.fn}: {symbol}")
                    df = getexecutions(symbol=symbol, mst_id=mst_id)
                    if df.shape[0] > 0:
                        df_exist = DB.select_sql(
                            f"select symbol, id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and id in ('" + "','".join(df['id'].astype(str).tolist()) + "') and " + 
                            f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                        )
                        df = df.loc[~df["id"].isin(df_exist["id"])]
                    if df.shape[0] > 0 and args.update:
                        DB.insert_from_df(df, f"{EXCHANGE}_executions", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(5) # 11 * 12 = 132
            if "getkline" == args.fn:
                for symbol in list(mst_id.keys()):
                    for kline in list(KILNE_URL.keys()):
                        DB.logger.info(f"{args.fn}: {symbol}, {kline}")
                        df = getkline(kline, symbol=symbol, interval="1m", limit=99, mst_id=mst_id)
                        if df.shape[0] > 0 and args.update:
                            DB.set_sql(
                                f"delete from {EXCHANGE}_kline where symbol = {df['symbol'].iloc[0]} and kline_type = {df['kline_type'].iloc[0]} and " + 
                                f"interval = {df['interval'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                            ) # latest data is more accurete. The data within 60s wouldn't be completed.
                            DB.insert_from_df(df, f"{EXCHANGE}_kline", set_sql=True, str_null="", is_select=True)
                            DB.execute_sql()
                time.sleep(60)
            if "getfundingrate" == args.fn:
                for symbol in list(mst_id.keys()):
                    DB.logger.info(f"{args.fn}: {symbol}")
                    df = getfundingrate(symbol=symbol, limit=10, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        DB.set_sql(
                            f"delete from {EXCHANGE}_funding_rate where symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                        ) # latest data is more accurete. The data within 60s might not be completed.
                        DB.insert_from_df(df, f"{EXCHANGE}_funding_rate", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(60*60*4)
            if "getopeninterest" == args.fn:
                for symbol in list(mst_id.keys()):
                    DB.logger.info(f"{args.fn}: {symbol}")
                    df = getopeninterest(symbol=symbol, interval="5m", limit=10, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        DB.set_sql(
                            f"delete from {EXCHANGE}_open_interest where symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                        ) # latest data is more accurete. The data within 60s might not be completed.
                        DB.insert_from_df(df, f"{EXCHANGE}_open_interest", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(60*3)
            if "getlongshortratio" == args.fn:
                for symbol in list(mst_id.keys()):
                    for ls_type in list(LS_RATIO_TYPE.keys()):
                        DB.logger.info(f"{args.fn}: {ls_type}, {symbol}")
                        df = getlongshortratio(ls_type, symbol=symbol, interval="5m", limit=10, mst_id=mst_id)
                        if df.shape[0] > 0 and args.update:
                            DB.set_sql(
                                f"delete from {EXCHANGE}_long_short where symbol = {df['symbol'].iloc[0]} and ls_type = {df['ls_type'].iloc[0]} and " + 
                                f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                            ) # latest data is more accurete. The data within 60s might not be completed.
                            DB.insert_from_df(df, f"{EXCHANGE}_long_short", set_sql=True, str_null="", is_select=True)
                            DB.execute_sql()
                time.sleep(60*3)
            if "gettakervolume" == args.fn:
                for symbol in list(mst_id.keys()):
                    DB.logger.info(f"{args.fn}: {symbol}")
                    df = gettakervolume(symbol=symbol, interval="5m", limit=10, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        DB.set_sql(
                            f"delete from {EXCHANGE}_taker_volume where symbol = {df['symbol'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                            f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                        ) # latest data is more accurete. The data within 60s might not be completed.
                        DB.insert_from_df(df, f"{EXCHANGE}_taker_volume", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(60*3)
    if "getallkline" == args.fn:
        assert args.fr is not None and args.to is not None
        date_since, date_until = args.fr, args.to
        for date in [date_since + datetime.timedelta(days=x) for x in range((date_until - date_since).days + 1)]:
            for hour in [0, 12]:
                time_since = int((date + datetime.timedelta(hours=hour+ 0)).timestamp() * 1000)
                time_until = int((date + datetime.timedelta(hours=hour+12)).timestamp() * 1000)
                for symbol in mst_id.keys():
                    for kline in ["mark", "index"]:
                        DB.logger.info(f"{args.fn}: {date}, {hour}, {symbol}, {kline}")
                        df = getkline(kline, symbol=symbol, interval=1, start=time_since, end=time_until, limit=1000, mst_id=mst_id)
                        if df.shape[0] > 0:
                            df_exist = DB.select_sql(
                                f"select unixtime from {EXCHANGE}_kline where symbol = {df['symbol'].iloc[0]} and kline_type = {df['kline_type'].iloc[0]} and " + 
                                f"interval = {df['interval'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                            )
                            df = df.loc[~df["unixtime"].isin(df_exist["unixtime"])]
                        if df.shape[0] > 0 and args.update:
                            DB.insert_from_df(df, f"{EXCHANGE}_kline", set_sql=True, str_null="", is_select=True)
                            DB.execute_sql()
                