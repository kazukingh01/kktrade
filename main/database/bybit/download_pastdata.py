import datetime, requests, gzip, argparse, json, subprocess, os
from zipfile import ZipFile
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from getdata import EXCHANGE


BASEURL  = "https://public.bybit.com/"
BASEURL2 = "https://quote-saver.bycsi.com/orderbook/"
LOGGER = set_logger(__name__)


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz", mst_id: dict=None):
    _type, _symbol = symbol.split("@")
    if _type == "spot":
        url = f"{BASEURL}spot/{_symbol}/{_symbol}_{date.strftime('%Y-%m-%d')}.csv.gz"
    else:
        url = f"{BASEURL}trading/{_symbol}/{_symbol}{date.strftime('%Y-%m-%d')}.csv.gz"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        LOGGER.warning(f"something is wrong. Data is missing. [{r.status_code}]")
        return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with gzip.open(tmp_file_path, mode="rt") as gzip_file:
        content = gzip_file.read()
        df = pd.read_csv(StringIO(content))
    df["unixtime"] = df["timestamp"].astype(int)
    df["symbol"]   = mst_id[symbol]
    if _type != "spot":
        df["id"]   = df["trdMatchID"].astype(str)
    else:
        # Be careful !! Spot data ID is different from the one that is obtained via API.
        df["id"]   = df["unixtime"].astype(str) + df["symbol"].astype(str).str.zfill(3) + df["id"].astype(str).str.zfill(8)
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1, "buy": 0, "sell": 1, "BUY": 0, "SELL": 1}).astype(float).fillna(-1).astype(int)
    df["is_block_trade"] = False
    if _type == "spot": df["size"] = df["volume"].copy()
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df = df.groupby(["symbol", "id"]).first().reset_index()
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
    df["symbol"] = mst_id[symbol]
    os.remove(fname)
    os.remove(fname_out)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101", required=True)
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101", required=True)
    parser.add_argument("--num", type=int, default=100)
    parser.add_argument("--fn",  type=lambda x: x.split(","), default="trade")
    parser.add_argument("--ip",   type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    res    = requests.post(f"http://{args.ip}:{args.port}/select", json={"sql": f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'"}, headers={'Content-type': 'application/json'})
    df_mst = pd.DataFrame(json.loads(res.json()))
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    date_st, date_ed = args.fr, args.to
    assert date_st <= date_ed
    assert isinstance(args.fn, list)
    for x in args.fn: assert x in ["trade", "orderbook"]
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            LOGGER.info(f"date: {date}, symbol: {symbol}")
            if "trade" in args.fn:
                df = download_trade(symbol, date, mst_id=mst_id)
                if df.shape[0] > 0 and symbol.split("@")[0] in ["linear", "inverse"]:
                    # Be careful !! Spot data ID is different from the one that is obtained via API.
                    res      = requests.post(f"http://{args.ip}:{args.port}/select", json={"sql": f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"}, headers={'Content-type': 'application/json'})
                    df_exist = pd.DataFrame(json.loads(res.json()))
                    df       = df.loc[~df["id"].isin(df_exist["id"])]
                else:
                    LOGGER.warning("Nothing data.")
                    continue
                if df.shape[0] > 0 and args.update:
                    if symbol.split("@")[0] == "spot":
                        # Be careful !! Spot data ID is different from the one that is obtained via API. So All delete & All insert
                        res = requests.post(f"http://{args.ip}:{args.port}/exec", json={"sql": f"DELETE FROM {EXCHANGE}_executions WHERE symbol = {df['symbol'].iloc[0]} AND unixtime >= {int(df['unixtime'].min())} AND unixtime <= {int(df['unixtime'].max())};"}, headers={'Content-type': 'application/json'})
                    if df.shape[0] >= args.num:
                        for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // args.num)):
                            res = requests.post(f"http://{args.ip}:{args.port}/insert", json={"data": df.iloc[indexes].replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_executions", "is_select": True}, headers={'Content-type': 'application/json'})
                    else:
                        res = requests.post(f"http://{args.ip}:{args.port}/insert", json={"data": df.replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_executions", "is_select": True}, headers={'Content-type': 'application/json'})
            if "orderbook" in args.fn:
                df = download_orderbook(symbol, date, mst_id=mst_id)
                if df.shape[0] > 0:
                    res      = requests.post(f"http://{args.ip}:{args.port}/select", json={"sql": f"select unixtime from {EXCHANGE}_orderbook where symbol = {df['symbol'].iloc[0]} and unixtime in ({','.join(df['unixtime'].unique().astype(str).tolist())});"}, headers={'Content-type': 'application/json'})
                    df_exist = pd.DataFrame(json.loads(res.json()))
                    df       = df.loc[~df["unixtime"].isin(df_exist["unixtime"].unique())]
                else:
                    LOGGER.warning("Nothing data.")
                    continue
                if df.shape[0] >= args.num:
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // args.num)):
                        res = requests.post(f"http://{args.ip}:{args.port}/insert", json={"data": df.iloc[indexes].replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_orderbook", "is_select": True}, headers={'Content-type': 'application/json'})
                else:
                    res = requests.post(f"http://{args.ip}:{args.port}/insert", json={"data": df.replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_orderbook", "is_select": True}, headers={'Content-type': 'application/json'})
