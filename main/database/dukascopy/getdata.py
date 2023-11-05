import requests, datetime, asyncio, time, argparse
import pandas as pd
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.apikey import APIKEY_DUKASCOPY


EXCHANGE = "dukascopy"
URL_BASE = "https://freeserv.dukascopy.com/2.0/"


def getinstrumentlist():
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/instrumentList"}))
    return pd.DataFrame(r.json())

def getintraday(symbol: str, interval: str="1min", date_from: datetime.datetime=None, date_to: datetime.datetime=None):
    df = {}
    for side in ["A", "B"]:
        r = requests.get(f"{URL_BASE}", params=dict({
            "key": APIKEY_DUKASCOPY, "path": "api/historicalPrices", "instrument": symbol, "timeFrame": interval,
            "start": int(date_from.timestamp() * 1000) if date_from is not None else None,
            "end":   int(date_to.  timestamp() * 1000) if date_to   is not None else None,
            "dayStartTime": "UTC", "offerSide": side, "count": 5000,
        }))
        assert r.status_code == 200
        df[side] = pd.DataFrame(r.json()["candles"])
    df = pd.merge(df["A"], df["B"], how="left", on="timestamp")
    df = df.dropna(how="any", axis=0)
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x] + df["bid_" + x]) / 2.
    df["unixtime"] = df["timestamp"].astype(int)
    df["interval"] = {"1min": 1, "10m": 10, "1hour": 60, "1day": 24*60}[interval]
    return df

def getlastminutekline():
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/lastOneMinuteCandles"}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()).T
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x].astype(float) + df["bid_" + x].astype(float)) / 2.
    df["unixtime"] = df["candle_time"].astype(int)
    df["interval"] = 1
    return df

def getcurrentprices(symbol: str):
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/currentPrices", "instruments": symbol}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    return df

def correct_df(df: pd.DataFrame, scale_pre: dict=None):
    df = df.copy()
    for x in ["price_open", "price_high", "price_low", "price_close", "ask_volume", "bid_volume"]:
        if df.columns.isin([x]).any() == False: continue
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=datetime.datetime.fromisoformat, help="--fr 20200101")
    parser.add_argument("--to", type=datetime.datetime.fromisoformat, help="--to 20200101")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    DB        = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst    = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}api';")
    mst_id    = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    scale_pre = {x:y for x, y in df_mst[["symbol_name", "scale_pre"]].values}
    if args.fn == "getintraday":
        assert args.fr is not None and args.to is not None
        dict_list = {y:x for x, y in getinstrumentlist()[["id", "name"]].values}
        for date in [args.fr + datetime.timedelta(days=x) for x in range((args.to - args.fr).days + 1)]:
            for symbol_name, symbol_id in mst_id.items():
                df = getintraday(dict_list[symbol_name], date_from=date, date_to=(date + datetime.timedelta(days=1)))
                df["symbol"] = symbol_id
                df = correct_df(df, scale_pre=scale_pre[symbol_name])
                if df.shape[0] > 0 and args.update:
                    DB.execute_sql(f"delete from {EXCHANGE}_ohlcv where symbol = {symbol_id} and interval = 1 and unixtime >= {df['unixtime'].min()} and unixtime <= {df['unixtime'].max()};")
                    DB.insert_from_df(df, f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=True)
                    DB.execute_sql()
    elif args.fn == "getlastminutekline":
        df = getlastminutekline()
        for symbol_name, symbol_id in mst_id.items():
            dfwk = correct_df(df.loc[[symbol_name]], scale_pre=scale_pre[symbol_name])
            dfwk["symbol"] = symbol_id
            if dfwk.shape[0] > 0 and args.update:
                DB.set_sql(f"DELETE FROM {EXCHANGE}_ohlcv WHERE symbol = {symbol_id} and interval = 1 and unixtime = {dfwk['unixtime'].iloc[0]};")
                DB.insert_from_df(dfwk[['symbol','price_open','price_high','price_low','price_close','unixtime','interval']], f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=False)
                DB.execute_sql()
