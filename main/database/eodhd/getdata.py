import requests, datetime, websockets, asyncio, json, time, argparse
import pandas as pd
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.apikey import APIKEY_EODHD


EXCHANGE = "eodhd"
URL_BASE = "https://eodhd.com/api/"
DICT_INTERVAL = {
    "BUK100P.INDX":  {"1m": "5m"},
    "GDAXI.INDX":    {"1m": "5m"},
    "FCHI.INDX":     {"1m": "5m"},
    "STOXX50E.INDX": {"1m": "5m"},
    "N100.INDX":     {"1m": "5m"},
    "BFX.INDX":      {"1m": "5m"},
    "N225.INDX":     {"1m": "5m"},
    "HSI.INDX":      {"1m": "5m"},
    "AORD.INDX":     {"1m": "5m"},
    "BSESN.INDX":    {"1m": "5m"},
    "JKSE.INDX":     {"1m": "5m"},
    "NZ50.INDX":     {"1m": "5m"},
    "KS11.INDX":     {"1m": "5m"},
    "TWII.INDX":     {"1m": "5m"},
    "GSPTSE.INDX":   {"1m": "5m"},
    "BVSP.INDX":     {"1m": "5m"},
    "MXX.INDX":      {"1m": "5m"},
    "AXJO.INDX":     {"1m": "5m"},
    "NSEI.INDX":     {"1m": "5m"},
    "IBEX.INDX":     {"1m": "5m"},
    "AEX.INDX":      {"1m": "5m"},
}


def getexchangelist():
    r = requests.get(f"{URL_BASE}exchanges-list/", params=dict({"api_token": APIKEY_EODHD, "fmt": "json"}))
    return pd.DataFrame(r.json())

def getsymbollist(exchange: str):
    r = requests.get(f"{URL_BASE}exchange-symbol-list/{exchange}", params=dict({"api_token": APIKEY_EODHD, "fmt": "json"}))
    return pd.DataFrame(r.json())

def getintraday(symbol: str, interval: str="1m", date_from: datetime.datetime=None, date_to: datetime.datetime=None):
    r = None
    for _ in range(3):
        r = requests.get(f"{URL_BASE}intraday/{symbol}", params=dict({
            "api_token": APIKEY_EODHD, "interval": interval, "fmt": "json",
            "from": int(date_from.timestamp()) if date_from is not None else None,
            "to":   int(date_to.  timestamp()) if date_to   is not None else None
        }))
        if r.status_code == 200: break
        continue
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["unixtime"] = df["timestamp"].astype(int)
    df["interval"] = {"1m": 1, "5m": 5, "1h": 60}[interval]
    columns = df.columns.copy()
    for x in ["open", "high", "low", "close"]: columns = columns.str.replace(f"^{x}$", f"price_{x}", regex=True)
    df.columns = columns
    return df

def getliveprice(symbol: str):
    assert isinstance(symbol, str) or isinstance(symbol, list)
    if isinstance(symbol, str): symbol = [symbol, ]
    r = requests.get(f"{URL_BASE}real-time/{symbol[0]}", params=dict({"api_token": APIKEY_EODHD, "fmt": "json", "s": ",".join(symbol[1:]) if len(symbol) > 1 else None}))
    assert r.status_code == 200
    dictwk = [r.json(), ] if isinstance(r.json(), dict) else r.json()
    df = pd.DataFrame(dictwk)
    df["unixtime"] = df["timestamp"].astype(int)
    df["interval"] = 1
    columns = df.columns.copy()
    for x in ["open", "high", "low", "close"]: columns = columns.str.replace(f"^{x}$", f"price_{x}", regex=True)
    df.columns = columns
    return df

def correct_df(df: pd.DataFrame, scale_pre: dict=None):
    df = df.copy()
    for x in ["price_open", "price_high", "price_low", "price_close", "volume"]:
        if df.columns.isin([x]).any() == False: continue
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    return df

def organize_values(list_values: list, scale_pre: dict, mst_id: dict, DB: Psgre=None):
    df = pd.DataFrame(list_values)
    df = df.sort_values(["t"]).reset_index(drop=True)
    df["tm"] = (df["t"] / 1000).astype(int)
    df["tm"] = (df["tm"] - (df["tm"] % 60) + 60) * 1000
    df["m"]  = (df["a"] + df["b"]) / 2.
    df = df.groupby(["s", "tm"])["m"].aggregate(["first", "max", "min", "last"]).reset_index()
    df.columns = ["symbol", "unixtime", "price_open", "price_high", "price_low", "price_close"]
    df = pd.concat([correct_df(dfwk, scale_pre=scale_pre[code]) for code, dfwk in df.groupby("symbol")], axis=0, ignore_index=True)
    df["interval"] = 1
    df["symbol"]   = df["symbol"].map(mst_id)
    if DB is not None and df.shape[0] > 0:
        DB.insert_from_df(
            df[["symbol", "unixtime", "interval", "price_open", "price_high", "price_low", "price_close"]],
            f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=False
        )
        DB.execute_sql()
    return df

async def getfromws(symbols: str, scale_pre: dict, mst_id: dict, type: str="forex", is_update: bool=False):
    # https://eodhd.com/financial-apis/new-real-time-data-api-websockets/
    assert type in ["forex"]
    assert isinstance(symbols, str) or isinstance(symbols, list)
    if isinstance(symbols, str): symbols = [symbols, ]
    uri = f"wss://ws.eodhistoricaldata.com/ws/{type}?api_token={APIKEY_EODHD}"
    DB  = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200) if is_update else None
    async with websockets.connect(uri) as ws:
        await ws.send('{"action": "subscribe", "symbols": "'+",".join(symbols)+'"}')
        list_values = []
        message     = await ws.recv()
        time_base   = None
        while True:
            message = await ws.recv()
            message = json.loads(message)
            t       = int(message["t"])
            if time_base is None:
                time_base = int(t / 1000)
                time_base = (time_base - (time_base % 60)) * 1000
            if (t - time_base) >= (60 * 1000) and len(list_values) > 0:
                df = organize_values(list_values.copy(), scale_pre, mst_id, DB=DB)
                print(df)
                list_values = []
                time_base   = int(t / 1000)
                time_base   = (time_base - (time_base % 60)) * 1000
            list_values.append(message)
            print(f"Received: {message}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--no", type=int, default=-1)
    parser.add_argument("--fr", type=datetime.datetime.fromisoformat, help="--fr 20200101")
    parser.add_argument("--to", type=datetime.datetime.fromisoformat, help="--to 20200101")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    DB        = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst    = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}';")
    mst_id    = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    scale_pre = {x:y for x, y in df_mst[["symbol_name", "scale_pre"]].values}
    if args.fn == "getintraday":
        assert args.fr is not None and args.to is not None
        for date in [args.fr + datetime.timedelta(days=x) for x in range((args.to - args.fr).days + 1)]:
            for symbol_name, symbol_id in mst_id.items():
                DB.logger.info(f"{date}, {symbol_name}")
                df = getintraday(
                    symbol_name, interval=DICT_INTERVAL[symbol_name]["1m"] if DICT_INTERVAL.get(symbol_name) is not None else "1m",
                    date_from=date, date_to=(date + datetime.timedelta(days=1))
                )
                df["symbol"] = symbol_id
                df = correct_df(df, scale_pre=scale_pre[symbol_name])
                DB.logger.info(f"{df}")
                if df.shape[0] > 0 and args.update:
                    DB.set_sql(f"delete from {EXCHANGE}_ohlcv where symbol = {symbol_id} and interval = {df['interval'].iloc[0]} and unixtime >= {df['unixtime'].min()} and unixtime <= {df['unixtime'].max()};")
                    DB.insert_from_df(
                        df[["symbol", "unixtime", "interval", "price_open", "price_high", "price_low", "price_close", "volume"]],
                        f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=False
                    )
                    DB.execute_sql()
    elif args.fn == "getliveprice":
        while True:
            df = getliveprice(list(mst_id.keys()))
            df = pd.concat([correct_df(dfwk, scale_pre=scale_pre[code]) for code, dfwk in df.groupby("code")], axis=0, ignore_index=True)
            df.loc[df["code"].isin(list(DICT_INTERVAL.keys())), "interval"] = 5
            df["symbol"] = df["code"].map(mst_id)
            if df.shape[0] > 0 and args.update:
                sql = f"delete from {EXCHANGE}_ohlcv where "
                for x, y, z in df[["symbol", "interval", "unixtime"]].values:
                    sql += f"(symbol = {x} and interval = {y} and unixtime = {z}) or "
                sql = sql[:-4] + ";"
                DB.set_sql(sql)
                DB.insert_from_df(
                    df[["symbol", "unixtime", "interval", "price_open", "price_high", "price_low", "price_close", "volume"]],
                    f"{EXCHANGE}_ohlcv", set_sql=True, str_null="", is_select=False
                )
                DB.execute_sql()
            time.sleep(30)
    elif args.fn == "getfromws":
        symbols = [x.split(".")[0] for x in list(mst_id.keys()) if x.find(".FOREX") >= 0]
        asyncio.run(getfromws(
            symbols, {x.split(".")[0]:y for x, y in scale_pre.items()}, 
            {x.split(".")[0]:y for x, y in mst_id.items()}, 
            type="forex", is_update=args.update
        ))

