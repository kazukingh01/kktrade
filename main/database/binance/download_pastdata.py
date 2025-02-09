import datetime, requests, argparse, os
from zipfile import ZipFile
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kklogger import set_logger
from kkpsgre.comapi import select, insert
from getdata import EXCHANGE, LS_RATIO_TYPE
from kkpsgre.connector import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


BASEURL  = "https://data.binance.vision/data/"
BASEURL2 = "https://quote-saver.bycsi.com/orderbook/"
MST_CONV = {
    "SPOT": "spot/",
    "USDS": "futures/um/",
    "COIN": "futures/cm/",
}
LOGGER = set_logger(__name__)


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./data1.zip", chunk_size: int=None):
    assert chunk_size is None or (isinstance(chunk_size, int) and chunk_size > 100)
    _type, _symbol = symbol.split("@")
    url = f"{BASEURL}{MST_CONV[_type]}daily/trades/{_symbol}/{_symbol}-trades-{date.strftime('%Y-%m-%d')}.zip"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"something is wrong. Data is missing. [{r.status_code}]")
        return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with ZipFile(tmp_file_path, mode="r") as zip_file:
        if chunk_size is None:
            with zip_file.open(zip_file.filelist[0].filename) as f:
                contents = f.read().decode()
                if contents[:2] == "id":
                    df = pd.read_csv(StringIO(contents))
                else:
                    df = pd.read_csv(StringIO(contents), header=None)
        else:
            fname = zip_file.filelist[0].filename
            zip_file.extract(fname, path=".")
            os.rename(fname, "tmp.csv")
            with open("tmp.csv", 'r') as file: contents = file.read(100) # A few letters reading from head.
            if contents[:2] == "id":
                df = pd.read_csv("tmp.csv", chunksize=chunk_size)
            else:
                df = pd.read_csv("tmp.csv", header=None, chunksize=chunk_size)
    return df

def organize_df(df: pd.DataFrame, mst_id: dict=None):
    df = df.copy()
    if df.shape[1] == 7:
        df = df.loc[df.iloc[:, -1] == True].iloc[:, :-1]
    df.columns = ["id", "price", "qty", "base_qty", "time", "is_buyer_maker"]
    assert df["is_buyer_maker"].dtype == np.bool_
    df["id"]       = df["id"].astype(np.int64)
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["side"]     = (~df["is_buyer_maker"]).astype(int) # "Buy": 0, "Sell": 1
    if len(str(df["time"].iloc[0])) == 16:
        df["unixtime"] = pd.to_datetime(df['time'], unit='us', utc=True)
    else:
        df["unixtime"] = pd.to_datetime(df['time'], unit='ms', utc=True) #df["time"].astype(int) // 1000
    df["size"]     = df["qty"].astype(float)
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df = df[["symbol", "id", "side", "unixtime", "price", "size"]]
    return df

def download_index(symbol: str, date: datetime.datetime, tmp_file_path: str="./data2.zip", mst_id: dict=None) -> list[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _type, _symbol = symbol.split("@")
    if _type == "SPOT": return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    url = f"{BASEURL}{MST_CONV[_type]}daily/metrics/{_symbol}/{_symbol}-metrics-{date.strftime('%Y-%m-%d')}.zip"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"something is wrong. Data is missing. [{r.status_code}]")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    open(tmp_file_path, 'wb').write(r.content)
    with ZipFile(tmp_file_path, mode="r") as zip_file:
        with zip_file.open(zip_file.filelist[0].filename) as f:
            contents = f.read()
            df = pd.read_csv(StringIO(contents.decode()))
    df["symbol"]   = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["unixtime"] = pd.to_datetime(df["create_time"], utc=True)
    df["interval"] = 5*60
    df["open_interest"]       = df["sum_open_interest"].astype(float)
    df["open_interest_value"] = df["sum_open_interest_value"].astype(float)
    df_ls_n, df_ls_ta, df_ls_tp = df.copy(), df.copy(), df.copy()
    df_ls_n[ "ls_type"]  = LS_RATIO_TYPE["normal"]
    df_ls_n[ "ls_ratio"] = df["count_long_short_ratio"].astype(float)
    df_ls_n[ "long"]     = df_ls_n["ls_ratio"] / (1 + df_ls_n["ls_ratio"])
    df_ls_n[ "short"]    = 1 - df_ls_n["long"]
    df_ls_tp["ls_type"]  = LS_RATIO_TYPE["top_position"]
    df_ls_tp["ls_ratio"] = df["sum_toptrader_long_short_ratio"].astype(float)
    df_ls_tp["long"]     = df_ls_tp["ls_ratio"] / (1 + df_ls_tp["ls_ratio"])
    df_ls_tp["short"]    = 1 - df_ls_tp["long"]
    df_ls_ta["ls_type"]  = LS_RATIO_TYPE["top_account"]
    df_ls_ta["ls_ratio"] = df["count_toptrader_long_short_ratio"].astype(float)
    df_ls_ta["long"]     = df_ls_ta["ls_ratio"] / (1 + df_ls_ta["ls_ratio"])
    df_ls_ta["short"]    = 1 - df_ls_ta["long"]
    return (df, df_ls_n, df_ls_ta, df_ls_tp)

def download_fundingrate(symbol: str, date: datetime.datetime, tmp_file_path: str="./data3.zip", mst_id: dict=None):
    _type, _symbol = symbol.split("@")
    if _type == "SPOT": return pd.DataFrame()
    url = f"{BASEURL}{MST_CONV[_type]}monthly/fundingRate/{_symbol}/{_symbol}-fundingRate-{date.strftime('%Y-%m')}.zip"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"something is wrong. Data is missing. [{r.status_code}]")
        return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with ZipFile(tmp_file_path, mode="r") as zip_file:
        with zip_file.open(zip_file.filelist[0].filename) as f:
            contents = f.read()
            df = pd.read_csv(StringIO(contents.decode()))
    df["unixtime"]     = pd.to_datetime(df["calc_time"], unit='ms', utc=True)
    df["symbol"]       = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
    df["funding_rate"] = df["last_funding_rate"].astype(float)
    df["mark_price"]   = float("nan")
    return df

 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101", required=True)
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101", required=True)
    parser.add_argument("--chunk", type=int, help="--chunk 1000000")
    parser.add_argument("--num",   type=int, default=1000)
    parser.add_argument("--sbls",  type=lambda x: [int(y) for y in x.split(",")], default=None)
    parser.add_argument("--fn",    type=lambda x: x.split(","), default="trade,index,fundingrate")
    parser.add_argument("--ip",   type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--fname",type=str, default="./data1.zip")
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    src    = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    df_mst = select(src, f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    date_st, date_ed = args.fr, args.to
    assert date_st <= date_ed
    assert isinstance(args.fn, list)
    for x in args.fn: assert x in ["trade", "index", "fundingrate"]
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            if args.sbls is not None and not mst_id[symbol] in args.sbls: continue
            LOGGER.info(f"date: {date}, symbol: {symbol}")
            # executions
            if "trade" in args.fn:
                dfs = download_trade(symbol, date, tmp_file_path=args.fname, chunk_size=args.chunk)
                if not isinstance(dfs, pd.io.parsers.readers.TextFileReader): dfs = [dfs, ]
                for df in dfs:
                    if df.shape[0] == 0: continue
                    df = organize_df(df, mst_id=mst_id)
                    if df.shape[0] > 0:
                        df_exist = select(src, f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
                        if df_exist.shape[0] > 0:
                            df = df.loc[~df["id"].isin(df_exist["id"])]
                    else:
                        LOGGER.warning("Nothing data.")
                    if df.shape[0] > 0 and args.update:
                        if args.db and src.dbinfo["dbtype"] == "mongo":
                            df["yearmonth"] = df["unixtime"].dt.strftime("%Y%m").astype(int)
                            df["metadata"]  = df[["yearmonth", "symbol"]].to_dict(orient="records")
                            df = df.loc[:, (df.columns != "yearmonth")]
                        for indexes in tqdm(np.array_split(np.arange(df.shape[0]), (df.shape[0] // args.num) if df.shape[0] >= args.num else 1)):
                            insert(src, df.iloc[indexes], f"{EXCHANGE}_executions", True, add_sql=None)
            # other index
            if "index" in args.fn:
                df_oi, df_ls_n, df_ls_ta, df_ls_tp = download_index(symbol, date, mst_id=mst_id)
                df = df_oi.copy()
                if df.shape[0] > 0:
                    df = df.loc[~df["open_interest"].isna()].copy()
                if df.shape[0] > 0 and args.update:
                    insert(
                        src, df, f"{EXCHANGE}_open_interest", True,
                        add_sql=(
                            f"symbol = {df['symbol'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                            f"unixtime IN ('" + "','".join([x.strftime('%Y-%m-%d %H:%M:%S.%f%z') for x in df['unixtime'].tolist()]) + "');"
                        )
                    )
                for df in [df_ls_n, df_ls_ta, df_ls_tp]:
                    if df.shape[0] > 0:
                        # Sometimes data is all-nan even getdata.getalllongshortratio() has a value.
                        df = df.loc[~df["long"].isna()].copy()
                    if df.shape[0] > 0 and args.update:
                        insert(
                            src, df, f"{EXCHANGE}_long_short", True,
                            add_sql=(
                                f"symbol = {df['symbol'].iloc[0]} and ls_type = {df['ls_type'].iloc[0]} and interval = {df['interval'].iloc[0]} and " + 
                                f"unixtime IN ('" + "','".join([x.strftime('%Y-%m-%d %H:%M:%S.%f%z') for x in df['unixtime'].tolist()]) + "');"
                            )
                        )
            # funding rate
            if "fundingrate" in args.fn:
                df = download_fundingrate(symbol, date, mst_id=mst_id)
                if df.shape[0] > 0 and args.update:
                    insert(
                        src, df, f"{EXCHANGE}_funding_rate", True,
                        add_sql=(
                            f"symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"
                        ) # latest data is more accurete. The data within 60s wouldn't be completed.
                    )
