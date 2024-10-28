import datetime, requests, gzip, argparse, json, subprocess, os
from zipfile import ZipFile
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from kkpsgre.comapi import select, insert, delete
from getdata import EXCHANGE
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


BASEURL  = "https://public.bybit.com/"
BASEURL2 = "https://quote-saver.bycsi.com/orderbook/"
LOGGER = set_logger(__name__)


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz", chunk_size: int=None):
    assert chunk_size is None or (isinstance(chunk_size, int) and chunk_size > 100)
    _type, _symbol = symbol.split("@")
    if _type == "spot":
        url = f"{BASEURL}spot/{_symbol}/{_symbol}_{date.strftime('%Y-%m-%d')}.csv.gz"
    else:
        url = f"{BASEURL}trading/{_symbol}/{_symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"Something is wrong. Data is missing. [{r.status_code}]")
        return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    if chunk_size is None:
        with gzip.open(tmp_file_path, mode="rt") as gzip_file:
            content = gzip_file.read()
            if content == "":
                # https://public.bybit.com/trading/SOLUSD/SOLUSD2022-12-12.csv.gz
                LOGGER.warning(f"Something is wrong. CSV has but no data. [{r.status_code}]")
                return pd.DataFrame()
            df = pd.read_csv(StringIO(content))
    else:
        with gzip.open(tmp_file_path, 'rb') as f_in:
            with open("tmp.csv", 'wb') as f_out:
                while True:
                    buffer = f_in.read(1024 ** 2 * 100) # 100M
                    if not buffer:
                        break
                    f_out.write(buffer)
        df = pd.read_csv("tmp.csv", chunksize=chunk_size)
    return df

def organize_df(df: pd.DataFrame, _type: str, mst_id: dict=None):
    df = df.copy()
    if _type == "spot":
        assert df['timestamp'].dtype == int # x isinstance(df['timestamp'].dtype, int) == False
        df["unixtime"] = pd.to_datetime(df['timestamp'], unit='ms', utc=True) #df["timestamp"].astype(int)
    else:
        if df['timestamp'].dtype == int:
            assert ((df["timestamp"] >= 1000000000) & (df["timestamp"] <= 4000000000)).sum() == df.shape[0]
            df['timestamp'] = df['timestamp'].astype(float)
        assert df['timestamp'].dtype == float # x isinstance(df['timestamp'].dtype, float) == False
        df["unixtime"] = pd.to_datetime(df['timestamp'], unit='s', utc=True) #df["timestamp"].astype(int)
    df["symbol"]   = mst_id[symbol]
    if _type != "spot":
        df["id"]   = df["trdMatchID"].astype(str)
    else:
        # Be careful !! Spot data ID is different from the one that is obtained via API.
        df["id"]   = df["unixtime"].dt.strftime("%Y%m%d%H") + "_" + df["symbol"].astype(str).str.zfill(3) + "_" + df["id"].astype(str).str.zfill(8)
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1, "buy": 0, "sell": 1, "BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int)
    df["is_block_trade"] = False
    if _type == "spot": df["size"] = df["volume"].copy()
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df = df.groupby(["symbol", "id"]).first().reset_index()
    df = df[["symbol", "id", "side", "unixtime", "price", "size"]]
    return df

def download_orderbook(symbol: str, date: datetime.datetime, count_max: int=100, tmp_file_path: str="./data.zip", mst_id: dict=None):
    assert count_max % 2 == 0
    count_max = count_max // 2
    _type, _symbol = symbol.split("@")
    url = f"{BASEURL2}{"/".join([_type, _symbol])}/{date.strftime('%Y-%m-%d')}_{_symbol}_ob500.data.zip"
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"something is wrong. Data is missing. [{r.status_code}]")
        return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    LOGGER.info(f"output a csv file via Rust script.")
    fname = None
    with ZipFile(tmp_file_path, mode="r") as zip_file:
        fname = zip_file.filelist[0].filename
        zip_file.extract(fname, "./")
    fname_out = fname.split(".")[0] + ".csv"
    subprocess.run(["./orderbook/target/debug/orderbook", fname, fname_out])
    df    = pd.read_csv(fname_out)
    dfwk1 = df.loc[df["side"] == 0].groupby("unixtime")[["side", "price", "size"]].apply(lambda x: x.sort_values("price", ascending=True).iloc[-count_max:         ]).reset_index() # ask
    dfwk2 = df.loc[df["side"] == 1].groupby("unixtime")[["side", "price", "size"]].apply(lambda x: x.sort_values("price", ascending=True).iloc[          :count_max]).reset_index() # bid
    df    = pd.concat([dfwk1[["unixtime", "side", "price", "size"]], dfwk2[["unixtime", "side", "price", "size"]]], axis=0, ignore_index=True, sort=False).reset_index(drop=True)
    df["symbol"]   = mst_id[symbol]
    df["unixtime"] = pd.to_datetime(df['unixtime'], unit='s', utc=True)
    os.remove(fname)
    os.remove(fname_out)
    df = df[["symbol", "unixtime", "side", "price", "size"]]
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101", required=True)
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101", required=True)
    parser.add_argument("--chunk", type=int, help="--chunk 1000000")
    parser.add_argument("--num",   type=int, default=1000)
    parser.add_argument("--fn",  type=lambda x: x.split(","), default="trade")
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
    for x in args.fn: assert x in ["trade", "orderbook"]
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            LOGGER.info(f"date: {date}, symbol: {symbol}")
            if "trade" in args.fn:
                dfs = download_trade(symbol, date, tmp_file_path=args.fname, chunk_size=args.chunk)
                if not isinstance(dfs, pd.io.parsers.readers.TextFileReader): dfs = [dfs, ]
                for df in dfs:
                    df = organize_df(df, symbol.split("@")[0], mst_id=mst_id)
                    df_exist = pd.DataFrame()
                    if df.shape[0] > 0:
                        df_exist = select(src, (
                            f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and " + 
                            f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
                        ))
                        if df_exist.shape[0] > 0:
                            if symbol.split("@")[0] == "spot":
                                # Be careful !! Spot data ID is different from the one that is obtained via API.
                                # So we dicide it by dependence of how much it is.
                                if df_exist.shape[0] == df.shape[0]:
                                    df = df.iloc[:0, :]
                            else:
                                df = df.loc[~df["id"].isin(df_exist["id"])]
                    else:
                        LOGGER.warning("Nothing data.")
                    if df.shape[0] > 0 and args.update:
                        if symbol.split("@")[0] == "spot" and df_exist.shape[0] > 0:
                            # Be careful !! Spot data ID is different from the one that is obtained via API. So All delete & All insert
                            delete(src, f"{EXCHANGE}_executions", str_where=(
                                f"symbol = {df['symbol'].iloc[0]} and " + 
                                f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"
                            ))
                        for indexes in tqdm(np.array_split(np.arange(df.shape[0]), (df.shape[0] // args.num) if df.shape[0] >= args.num else 1)):
                            insert(src, df.iloc[indexes], f"{EXCHANGE}_executions", True, add_sql=None)
            if "orderbook" in args.fn:
                df = download_orderbook(symbol, date, mst_id=mst_id, tmp_file_path=args.fname)
                if df.shape[0] == 0:
                    LOGGER.warning("Nothing data.")
                if df.shape[0] > 0 and args.update:
                    # All delete & All insert
                    delete(src, f"{EXCHANGE}_orderbook", str_where=(
                        f"symbol = {df['symbol'].iloc[0]} and " + 
                        f"unixtime >= '{df['unixtime'].min().strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime <= '{df['unixtime'].max().strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"
                    ))
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), (df.shape[0] // args.num) if df.shape[0] >= args.num else 1)):
                        insert(src, df.iloc[indexes], f"{EXCHANGE}_orderbook", True, add_sql=None)
