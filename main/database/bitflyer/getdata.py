import datetime, requests, time, argparse, json
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from kktrade.util.database import select, insert
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


EXCHANGE = "bitflyer"
URL_BASE = "https://api.bitflyer.com/v1/"
STATE = {
    "RUNNING": 0,
    "CLOSED": 1,
    "STARTING": 2,
    "PREOPEN": 3,
    "CIRCUIT BREAK": 4,
    "AWAITING SQ": 5,
    "MATURED": 6,
}
LOGGER = set_logger(__name__)


func_to_unixtime = np.vectorize(lambda x: x.timestamp())


def getmarkets():
    r  = requests.get(f"{URL_BASE}getmarkets")
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    return df

def getorderbook(symbol: str="BTC_JPY", count_max: int=100, mst_id: dict=None):
    r    = requests.get(f"{URL_BASE}getboard", params={"product_code": symbol})
    assert r.status_code == 200
    time = datetime.datetime.now(tz=datetime.UTC)
    df1  = pd.DataFrame(r.json()["bids"])
    df1["side"] = "bids"
    df2 = pd.DataFrame(r.json()["asks"])
    df2["side"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-count_max:         ],
        df2.sort_values(by="price")[          :count_max],
        pd.DataFrame([{"side": "mprc", "price": r.json()["mid_price"]}])
    ], axis=0, ignore_index=True)
    df["unixtime"] = time
    df["side"]     = df["side"].map({"asks": 0, "bids": 1, "mprc": 2}).astype(float).fillna(-1).astype(int)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    return df

def getticker(symbol: str="BTC_JPY", mst_id: dict=None):
    r  = requests.get(f"{URL_BASE}getticker", params={"product_code": symbol})
    assert r.status_code == 200
    df = pd.DataFrame([r.json()])
    conv_dict = {
        "best_bid": "bid",
        "best_ask": "ask",
        "best_bid_size": "bid_size",
        "best_ask_size": "ask_size",
        "ltp": "last_traded_price",
    }
    df.columns = df.columns.map(lambda x: conv_dict[x] if conv_dict.get(x) is not None else x)
    for x in [
        "bid", "ask", "bid_size", "ask_size", "total_bid_depth", "total_ask_depth",
        "market_bid_size", "market_ask_size", "volume", "volume_by_product", "last_traded_price",
    ]:
        df[x] = df[x].astype(float)
    df["unixtime"] = pd.to_datetime(df["timestamp"], utc=True)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["state"]    = df["state"].map(STATE).fillna(-1).astype(int)
    return df

def getexecutions(symbol: str="BTC_JPY", before: int=None, after: int=None, mst_id: dict=None):
    r  = requests.get(f"{URL_BASE}getexecutions", params={
        "product_code": symbol, "count": 10000, "before": before, "after": after,
    })
    if r.status_code in [400]: return pd.DataFrame()
    assert r.status_code == 200
    df = pd.DataFrame(r.json())
    if df.shape[0] == 0: return df
    df["symbol"] = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["side"]   = df["side"].map({"BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int) # nan = 板寄せ
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df.loc[df["exec_date"].str.find(".") < 0, "exec_date"] += ".000" # Error. ValueError: time data "2024-07-02T12:18:50" doesn't match format "%Y-%m-%dT%H:%M:%S.%f", at position 1. You might want to try
    df["unixtime"] = pd.to_datetime(df["exec_date"], utc=True)
    return df

def getfundingrate(symbol: str="FX_BTC_JPY", mst_id: dict=None):
    r  = requests.get(f"{URL_BASE}getfundingrate", params={"product_code": symbol})
    if r.status_code in [400]: return pd.DataFrame()
    assert r.status_code == 200
    df = pd.DataFrame([r.json()])
    if df.shape[0] == 0: return df
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["unixtime"] = datetime.datetime.now(tz=datetime.UTC)
    df["next_funding_rate_settledate"] = pd.to_datetime(df["next_funding_rate_settledate"], utc=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--sec", type=int, default=60)
    parser.add_argument("--cnt", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=0.6)
    parser.add_argument("--nloop", type=int, default=5)
    parser.add_argument("--ip",    type=str, default="127.0.0.1")
    parser.add_argument("--port",  type=int, default=8000)
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    src    = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    df_mst = select(src, f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    if args.fn in ["getorderbook", "getticker", "getexecutions", "getfundingrate"]:
        while True:
            if "getorderbook" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    df = getorderbook(symbol=symbol, count_max=50, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_orderbook", False, add_sql=None)
                time.sleep(12)
            elif "getticker" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    df   = getticker(symbol=symbol, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_ticker", True, add_sql=None)
                time.sleep(5)
            elif "getexecutions" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    # select since 3 days before.
                    df_exist = select(src, (
                        f"select max(id) as id from {EXCHANGE}_executions where symbol = {mst_id[symbol]} and " + 
                        f"unixtime >= '{(datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')}';"
                    ))
                    id_after = None if df_exist.shape[0] == 0 else df_exist["id"].iloc[0]
                    df       = getexecutions(symbol=symbol, after=id_after, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_executions", True, add_sql=None)
                time.sleep(12)
            elif "getfundingrate" == args.fn:
                for symbol in mst_id.keys():
                    LOGGER.info(f"{args.fn}: {symbol}")
                    if symbol not in ["FX_BTC_JPY"]: continue
                    df = getfundingrate(symbol=symbol, mst_id=mst_id)
                    if df.shape[0] > 0 and args.update:
                        insert(src, df, f"{EXCHANGE}_fundingrate", True, add_sql=None)
                time.sleep(60 * 10)
    if args.fn in ["getall"]:
        # python getdata.py --fn getall --fr 20231001 --to 20231012 --sec 30 --cnt 200
        time_since = args.fr if args.fr is not None else datetime.datetime.fromisoformat("20000101T00:00:00Z")
        time_until = args.to if args.to is not None else datetime.datetime.now(tz=datetime.UTC)
        over_sec   = args.sec
        over_count = args.cnt
        for _ in range(args.nloop):
            # If the gap from idb to ida is huge, api cannot get full data between its period. so loop system must be used.
            dfwk = select(src, (
                f"select symbol, id, unixtime from {EXCHANGE}_executions where unixtime <= '{time_until.strftime('%Y-%m-%d %H:%M:%S')}' and unixtime >= '{time_since.strftime('%Y-%m-%d %H:%M:%S')}';"
            ))
            if dfwk.shape[0] > 0:
                dfwk = dfwk.sort_values(["symbol", "unixtime", "id"]).reset_index(drop=True)
                dfwk["id_prev"]       = np.concatenate(dfwk.groupby("symbol").apply(lambda x: [-1] + x["id"      ].tolist()[:-1]).values).reshape(-1)
                dfwk["unixtime_prev"] = np.concatenate(dfwk.groupby("symbol").apply(lambda x: [-1] + x["unixtime"].tolist()[:-1]).values).reshape(-1)
                dfwk["diff"] = (dfwk["unixtime"] - dfwk["unixtime_prev"]).dt.seconds
                dfwk["bool"] = (dfwk["diff"] >= over_sec)
                dfwk = dfwk.sort_values("diff", ascending=False)
                LOGGER.info(f'Target num: {dfwk["bool"].sum()}')
                count = 0
                for index in dfwk.index[dfwk["bool"]]:
                    idb    = dfwk.loc[index, "id"]
                    ida    = dfwk.loc[index, "id_prev"] if dfwk.loc[index, "unixtime_prev"].shape[0] > 0 else None
                    symbol = {y:x for x, y in mst_id.items()}[dfwk.loc[index, "symbol"]]
                    LOGGER.info(f'idb: {idb}, ida: {ida}, symbol: {symbol}, diff: {dfwk.loc[index, "diff"]}')
                    df     = getexecutions(symbol=symbol, before=idb, after=ida, mst_id=mst_id)
                    if df.shape[0] > 0:
                        df_exist = select(src, (
                            f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S')}' and unixtime < '{(df['unixtime'].max() + datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')}';"
                        ))
                        if df_exist.shape[0] > 0:
                            df = df.loc[~df["id"].isin(df_exist["id"])]
                    if df.shape[0] > 0:
                        count = 0
                        if args.update:
                             insert(src, df, f"{EXCHANGE}_executions", True, add_sql=None)
                    else:
                        count += 1
                    if count > over_count: break
                    time.sleep(args.sleep)
