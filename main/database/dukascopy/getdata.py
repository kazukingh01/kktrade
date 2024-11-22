import requests, datetime, time, argparse
import pandas as pd
# local package
from kkpsgre.util.logger import set_logger
from kktrade.config.apikey import APIKEY_DUKASCOPY
from kkpsgre.util.com import strfind
from kkpsgre.comapi import select, insert
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


EXCHANGE = "dukascopy"
URL_BASE = "https://freeserv.dukascopy.com/2.0/"
LOGGER   = set_logger(__name__)
FUNCTIONS = [
    "getintraday",
    "getlastminutekline",
]


def getinstrumentlist():
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/instrumentList"}))
    return pd.DataFrame(r.json())

def getintraday(symbol: str="2032", interval: str="1min", date_from: datetime.datetime=None, date_to: datetime.datetime=None, max_try: int=5, sec_sleep: int=3):
    """
    !!! If requests.get is done twice very quickly, The response is diffenrent. !!!
    !!! Maybe previous data with different syboll is returned. !!!
    """
    df = {}
    dictwk = {"A": "ask", "B": "bid"}
    for side in ["A", "B"]:
        is_success = False
        for i_try in range(max_try):
            r = requests.get(f"{URL_BASE}", params=dict({
                "key": APIKEY_DUKASCOPY, "path": "api/historicalPrices", "instrument": symbol, "timeFrame": interval,
                "start": int(date_from.timestamp() * 1000) if date_from is not None else None,
                "end":   int(date_to.  timestamp() * 1000) if date_to   is not None else None,
                "dayStartTime": "UTC", "offerSide": side, "count": 5000,
            }))
            assert r.status_code == 200
            dict_json = r.json()
            if not ((dict_json["id"] == symbol) and (dict_json["timeFrame"] == interval) and (dict_json["offerSide"] == dictwk[side])):
                LOGGER.warning(f"Retry: {i_try}")
                LOGGER.warning(f'The input    params: {symbol}, {interval}, {dictwk[side]}')
                LOGGER.warning(f'The response params: {dict_json["id"]}, {dict_json["timeFrame"]}, {dict_json["offerSide"]}')
                time.sleep(sec_sleep)
                continue
            else:
                is_success = True
            df[side] = pd.DataFrame(dict_json["candles"])
            if df[side].shape[0] == 0:
                return pd.DataFrame()
            if is_success:
                break # go out of loop
        assert is_success
    df = pd.merge(df["A"], df["B"], how="left", on="timestamp")
    df = df.dropna(how="any", axis=0)
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x] + df["bid_" + x]) / 2.
    df["unixtime"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms", utc=True)
    df["interval"] = {"1min": 1, "10m": 10, "1hour": 60, "1day": 24*60}[interval]
    return df

def getlastminutekline():
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/lastOneMinuteCandles"}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()).T
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x].astype(float) + df["bid_" + x].astype(float)) / 2.
    df["unixtime"] = pd.to_datetime(df["candle_time"].astype(int), unit="ms", utc=True)
    df["interval"] = 1
    return df

def getcurrentprices(symbol: str="2032"):
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/currentPrices", "instruments": symbol}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    df["unixtime"] = df["time"].astype(int) // 1000
    return df

def correct_df(df: pd.DataFrame):
    df = df.copy()
    for x in ["price_open", "price_high", "price_low", "price_close", "ask_volume", "bid_volume"]:
        if not x in df.columns: continue
        df[x] = df[x].astype(float)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=lambda x: FUNCTIONS[eval(x)] if strfind(r"^[0-9]+$", x) else x)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--days",  type=int, help="--days 1", default=1)
    parser.add_argument("--ntry",  type=int, help="--ntry 5", default=5)
    parser.add_argument("--sleep", type=int, help="--sleep 3", default=3)
    parser.add_argument("--ip",    type=str, default="127.0.0.1")
    parser.add_argument("--port",  type=int, default=8000)
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    LOGGER.info(f"{args}")
    assert args.days <= 3 # The maximum records with 1 miniute interval is 5000. 3 days data is 60 * 24 * 3 = 4320
    assert args.fn in FUNCTIONS
    src    = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    df_mst = select(src, f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}api'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    if args.fn == "getintraday":
        assert args.fr is not None and args.to is not None
        assert args.fr < args.to
        dict_list = {y:x for x, y in getinstrumentlist()[["id", "name"]].values}
        list_dates = [args.fr + datetime.timedelta(days=x) for x in range(0, (args.to - args.fr).days + args.days, args.days)]
        assert list_dates[-1] >= args.to
        list_dates[-1] = args.to
        for i_date, date in enumerate(list_dates):
            if i_date == (len(list_dates) - 1): break
            for symbol_name, symbol_id in mst_id.items():
                LOGGER.info(f"{date}, {list_dates[i_date + 1]}, {symbol_name}")
                df = getintraday(dict_list[symbol_name], date_from=date, date_to=list_dates[i_date + 1], max_try=args.ntry, sec_sleep=args.sleep)
                if df.shape[0] > 0:
                    df["symbol"] = symbol_id
                    df = correct_df(df)
                else:
                    LOGGER.warning("data is nothing.")
                if df.shape[0] > 0 and args.update:
                    insert(
                        src, df, f"{EXCHANGE}_ohlcv", True,
                        add_sql=(
                            f"symbol = {df['symbol'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                            f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"
                        )
                    )
                time.sleep(args.sleep)
    elif args.fn == "getlastminutekline":
        while True:
            LOGGER.info("getlastminutekline start")
            df = getlastminutekline()
            df = df.loc[list(mst_id.keys())].reset_index()
            df.columns = ["symbol_name"] + df.columns[1:].tolist()
            df["symbol"] = df["symbol_name"].map(mst_id).astype(int)
            df = correct_df(df)
            LOGGER.info("getlastminutekline end")
            if df.shape[0] > 0 and args.update:
                df["_symbol"]   = df["symbol"].astype(str)
                df["_interval"] = df["interval"].astype(str)
                df["_unixtime"] = df["unixtime"].dt.strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
                insert(
                    src, df[['symbol','price_open','price_high','price_low','price_close','unixtime','interval']], f"{EXCHANGE}_ohlcv", False,
                    add_sql=(
                        "(symbol,interval,unixtime) in (" + ",".join([f"({x},{y},{z})" for x,y,z in df[["_symbol","_interval","_unixtime"]].values]) + ")"
                    )
                )
            time.sleep(30)