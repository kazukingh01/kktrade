import requests, datetime, time, argparse
import pandas as pd
# local package
from kkpsgre.psgre import Psgre
from kkpsgre.util.logger import set_logger
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.apikey import APIKEY_DUKASCOPY


EXCHANGE = "dukascopy"
URL_BASE = "https://freeserv.dukascopy.com/2.0/"
LOGGER   = set_logger(__name__) # only this program


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
        for i in range(max_try):
            r = requests.get(f"{URL_BASE}", params=dict({
                "key": APIKEY_DUKASCOPY, "path": "api/historicalPrices", "instrument": symbol, "timeFrame": interval,
                "start": int(date_from.timestamp() * 1000) if date_from is not None else None,
                "end":   int(date_to.  timestamp() * 1000) if date_to   is not None else None,
                "dayStartTime": "UTC", "offerSide": side, "count": 5000,
            }))
            assert r.status_code == 200
            dict_json = r.json()
            if not ((dict_json["id"] == symbol) and (dict_json["timeFrame"] == interval) and (dict_json["offerSide"] == dictwk[side])):
                LOGGER.warning(f"Retry: {i}")
                LOGGER.warning(f'The input    params: {symbol}, {interval}, {dictwk[side]}')
                LOGGER.warning(f'The response params: {dict_json["id"]}, {dict_json["timeFrame"]}, {dict_json["offerSide"]}')
                time.sleep(sec_sleep)
                continue
            else:
                is_success = True
            df[side] = pd.DataFrame(dict_json["candles"])
            if df[side].shape[0] == 0:
                return pd.DataFrame()
        assert is_success
    df = pd.merge(df["A"], df["B"], how="left", on="timestamp")
    df = df.dropna(how="any", axis=0)
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x] + df["bid_" + x]) / 2.
    df["unixtime"] = df["timestamp"].astype(int) // 1000
    df["interval"] = {"1min": 1, "10m": 10, "1hour": 60, "1day": 24*60}[interval]
    return df

def getlastminutekline():
    r = requests.get(f"{URL_BASE}", params=dict({"key": APIKEY_DUKASCOPY, "path": "api/lastOneMinuteCandles"}))
    assert r.status_code == 200
    df = pd.DataFrame(r.json()).T
    for x in ["open", "high", "low", "close"]:
        df["price_" + x] = (df["ask_" + x].astype(float) + df["bid_" + x].astype(float)) / 2.
    df["unixtime"] = df["candle_time"].astype(int) // 1000
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
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--days",  type=int, help="--days 1", default=1)
    parser.add_argument("--ntry",  type=int, help="--ntry 5", default=5)
    parser.add_argument("--sleep", type=int, help="--sleep 3", default=3)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    assert args.days <= 3 # The maximum records with 1 miniute interval is 5000. 3 days data is 60 * 24 * 3 = 4320
    DB     = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}api';")
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
                DB.logger.info(f"{date}, {list_dates[i_date + 1]}, {symbol_name}")
                df = getintraday(dict_list[symbol_name], date_from=date, date_to=list_dates[i_date + 1], max_try=args.ntry, sec_sleep=args.sleep)
                if df.shape[0] > 0:
                    df["symbol"] = symbol_id
                    df = correct_df(df)
                else:
                    DB.logger.warning("data is nothing.")
                if df.shape[0] > 0 and args.update:
                    DB.set_sql(f"delete from {EXCHANGE}_ohlcv where symbol = {symbol_id} and interval = 1 and unixtime >= {df['unixtime'].min()} and unixtime <= {df['unixtime'].max()};")
                    DB.insert_from_df(df, f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=True)
                    DB.execute_sql()
                time.sleep(args.sleep)
    elif args.fn == "getlastminutekline":
        while True:
            DB.logger.info("getlastminutekline start")
            df = getlastminutekline()
            df = df.loc[list(mst_id.keys())].reset_index()
            df.columns = ["symbol_name"] + df.columns[1:].tolist()
            df["symbol"] = df["symbol_name"].map(mst_id).astype(int)
            df = correct_df(df)
            DB.logger.info("getlastminutekline end")
            if df.shape[0] > 0 and args.update:
                DB.set_sql(f"DELETE FROM {EXCHANGE}_ohlcv WHERE (symbol,interval,unixtime) in (" + ",".join([f"({x},{y},{z})" for x,y,z in df[["symbol","interval","unixtime"]].values]) + ");")
                DB.insert_from_df(df[['symbol','price_open','price_high','price_low','price_close','unixtime','interval']], f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=False)
                DB.execute_sql()
            time.sleep(30)