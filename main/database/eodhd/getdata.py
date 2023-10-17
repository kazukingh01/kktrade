import requests, datetime, websockets, asyncio, json, time, argparse
import pandas as pd
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.com import SCALE_MST, NAME_MST
from kktrade.config.apikey import APIKEY_EODHD


# https://www.axiory.com/jp/trading-tools/historical-data
# https://www.dukascopy.com/plugins/fxMarketWatch/?historical_data # Bir, UTC, GMT
# https://finance.yahoo.com/currencies
# https://help.yahoo.com/kb/finance-for-web


# https://medium.com/codex/top-7-financial-apis-to-try-out-in-2022-4f7b38c1fb8

# これいいかも
# https://twelvedata.com/pricing

# 結局これにする。
# https://eodhd.com/pricing

EXCHANGE = "eodhd"
URL_BASE = "https://eodhd.com/api/"
SCALE = {
    "USDJPY.FOREX": 11,
    "EURUSD.FOREX": 13,
    "GBPUSD.FOREX": 13,
    "USDCHF.FOREX": 13,
    "AUDUSD.FOREX": 13,
    "USDCAD.FOREX": 13,
    "NZDUSD.FOREX": 13,
    "EURGBP.FOREX": 13,
    "EURJPY.FOREX": 11,
    "EURCHF.FOREX": 13,
    "XAUUSD.FOREX": 9,
    "GSPC.INDX":     7,
    "DJI.INDX":      11,
    "IXIC.INDX":     7,
    "BUK100P.INDX":  13,
    "VIX.INDX":      9,
    "GDAXI.INDX":    7,
    "FCHI.INDX":     7,
    "STOXX50E.INDX": 7,
    "N100.INDX":     13,
    "BFX.INDX":      7,
    # "IMOEX.INDX":    12, # no data
    "N225.INDX":     11,
    "HSI.INDX":      7,
    # "SSEC.INDX":     12, # no data
    "AORD.INDX":     7,
    "BSESN.INDX":    11,
    "JKSE.INDX":     7,
    "NZ50.INDX":     7,
    "KS11.INDX":     13, # 5
    "TWII.INDX":     7,  # 4
    "GSPTSE.INDX":   7,
    "BVSP.INDX":     11, # 3
    "MXX.INDX":      7,
    # "SPIPSA.INDX":   12, # no data
    # "MERV.INDX":     12, # no data
    # "TA125.INDX":    12, # no data
}



def getexchangelist():
    r = requests.get(f"{URL_BASE}exchanges-list/", params=dict({"api_token": APIKEY_EODHD, "fmt": "json"}))
    return pd.DataFrame(r.json())

def getintraday(symbol: str, interval: str="1m", date_from: str=None, date_to: str=None):
    if interval == "1m" and symbol in [
        "BUK100P.INDX", "GDAXI.INDX", "FCHI.INDX", "STOXX50E.INDX", "N100.INDX", "BFX.INDX", "N225.INDX",
        "HSI.INDX", "AORD.INDX", "BSESN.INDX", "JKSE.INDX", "NZ50.INDX", "TWII.INDX", "GSPTSE.INDX", "BVSP.INDX",
        "MXX.INDX", "KS11.INDX", 
    ]: interval = "5m"
    r = requests.get(f"{URL_BASE}intraday/{symbol}", params=dict({
        "api_token": APIKEY_EODHD, "interval": interval, "fmt": "json", "from": date_from, "to": date_to
    }))
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["unixtime"] = df["timestamp"].astype(int)
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    df["interval"] = {"1m": 1, "5m": 5, "1h": 60}[interval]
    for x in ["open", "high", "low", "close"]:
        df[f"price_{x}"] = (df[x].astype(float) * (10 ** SCALE_MST[df["scale"].iloc[0]][0])).fillna(-1).astype(int)
    df["volume"]   = (df["volume" ].astype(float) * (10 ** SCALE_MST[df["scale"].iloc[0]][1])).fillna(-1).astype(int)
    return df

def getliveprice(symbol: str):
    assert isinstance(symbol, str) and symbol.find(".FOREX") >= 0
    r = requests.get(f"{URL_BASE}real-time/{symbol}", params=dict({"api_token": APIKEY_EODHD, "fmt": "json"}))
    df = pd.DataFrame([r.json()])
    df["unixtime"] = df["timestamp"].astype(int)
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    df["interval"] = 1
    for x in ["open", "high", "low", "close"]:
        df[f"price_{x}"] = (df[x].astype(float) * (10 ** SCALE_MST[df["scale"].iloc[0]][0])).fillna(-1).astype(int)
    df["volume"]   = (df["volume" ].astype(float) * (10 ** SCALE_MST[df["scale"].iloc[0]][1])).fillna(-1).astype(int)
    return df

async def getfromws(symbol: str, type: str="forex", is_update: bool=False):
    assert type in ["forex", "crypto"]
    assert isinstance(symbol, str) and symbol.find(".FOREX") >= 0
    pkey = {"forex": "a", "crypto": "p"}[type]
    uri  = f"wss://ws.eodhistoricaldata.com/ws/{type}?api_token={APIKEY_EODHD}"
    db   = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200) if is_update else None
    async with websockets.connect(uri) as ws:
        await ws.send('{"action": "subscribe", "symbols": "'+symbol.replace(".FOREX", "")+'"}')
        message = await ws.recv()
        po, ph, pl, pc = None, None, None, None
        symbol_id = NAME_MST[symbol]
        scale     = SCALE[symbol]
        scale_mst = SCALE_MST[scale][0]
        time_base = datetime.datetime.now()
        time_base = (int(time_base.timestamp()) - time_base.second) * 1000
        while True:
            message = await ws.recv()
            message = json.loads(message)
            price   = int(float(message[pkey]) * (10 ** scale_mst))
            time    = int(message["t"])
            if is_update and (time - time_base) >= (60 * 1000) and po is not None:
                db.set_sql(
                    f"INSERT INTO eodhd_ohlcv (symbol, scale, unixtime, interval, price_open, price_high, price_low, price_close) VALUES " + 
                    f"({symbol_id}, {scale}, {time}, 1, {po}, {ph}, {pl}, {pc});"
                )
                db.execute_sql()
                time_base = time
                po, ph, pl, pc = None, None, None, None
            if po is None:
                po, ph, pl, pc = price, price, price, price
            else:
                pc = price
                if ph < price: ph = price
                if pl > price: pl = price
            print(f"Received: {symbol, time, po, ph, pl, pc}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--no", type=int, default=-1)
    parser.add_argument("--since", type=str, default="")
    parser.add_argument("--until", type=str, default="")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    DB   = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    if args.fn in ["getintraday", "getliveprice"]:
        while True:
            if args.fn == "getintraday":
                for symbol in SCALE.keys():
                    timestamp = int(datetime.datetime.now().timestamp())
                    df = getintraday(symbol, interval="1m", date_from=timestamp-(60*60*24*3), date_to=timestamp)
                    if df.shape[0] > 0 and args.update:
                        DB.set_sql(
                            f"delete from {EXCHANGE}_ohlcv where interval = {df['interval'].iloc[0]} and " + 
                            f"unixtime in ({','.join(df['unixtime'].astype(str).tolist())}) and symbol = {df['symbol'].iloc[0]};"
                        )
                        DB.execute_sql() # Because below function's is_select=True cause ANOTHER SELECT SQL, I have to delete step by step.
                        DB.insert_from_df(df, f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=True, n_jobs=8)
                        DB.execute_sql()
                time.sleep(60 * 60 * 1) # per hour
            if args.fn == "getliveprice":
                for symbol in SCALE.keys():
                    df = getliveprice(symbol)
                    if df.shape[0] == 1 and args.update:
                        DB.set_sql(f"delete from {EXCHANGE}_ohlcv where unixtime = {df['unixtime'].iloc[0]} and symbol = {df['symbol'].iloc[0]};")
                        DB.insert_from_df(df, f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=True)
                        DB.execute_sql()
                time.sleep(60) # per miniute
    if args.fn == "getall":
        assert args.since != "" and args.until != ""
        date_since = datetime.datetime.fromisoformat(args.since)
        date_until = datetime.datetime.fromisoformat(args.until)
        for date in [date_since + datetime.timedelta(days=x) for x in range(0, (date_until - date_since).days + 1, 50)]:
            date_from = int(date.timestamp())
            date_to   = int((date + datetime.timedelta(days=50)).timestamp())
            for symbol in SCALE.keys():
                print(date, date_from, date_to, symbol)
                df = getintraday(symbol, interval="1m", date_from=date_from, date_to=date_to)
                if df.shape[0] > 0 and args.update:
                    DB.set_sql(
                        f"delete from {EXCHANGE}_ohlcv where interval = {df['interval'].iloc[0]} and " + 
                        f"unixtime in ({','.join(df['unixtime'].astype(str).tolist())}) and symbol = {df['symbol'].iloc[0]};"
                    )
                    DB.execute_sql() # Because below function's is_select=True cause ANOTHER SELECT SQL, I have to delete step by step.
                    DB.insert_from_df(df, f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=True, n_jobs=8)
                    DB.execute_sql()
    if args.fn == "getfromws":
        assert args.no >= 0 and args.no < len(SCALE.keys())
        symbol = list(SCALE.keys())[args.no]
        assert symbol.find(".FOREX") >= 0
        asyncio.run(getfromws(symbol, type="forex", is_update=args.update))

