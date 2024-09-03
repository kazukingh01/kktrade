import datetime, requests, gzip, argparse, json
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from getdata import EXCHANGE


BASEURL  = "https://public.bybit.com/trading/"
MST_CONV = {
    "linear@BTCUSDT": "BTCUSDT",
    "inverse@BTCUSD": "BTCUSD",
    "linear@ETHUSDT": "ETHUSDT",
    "inverse@ETHUSD": "ETHUSD",
    "linear@XRPUSDT": "XRPUSDT",
    "inverse@XRPUSD": "XRPUSD",
}
LOGGER = set_logger(__name__)


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz", mst_id: dict=None):
    r = requests.get(f"{BASEURL}{MST_CONV[symbol]}/{MST_CONV[symbol]}{date.strftime('%Y-%m-%d')}.csv.gz", allow_redirects=True)
    if r.status_code != 200: return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with gzip.open(tmp_file_path, mode="rt") as gzip_file:
        content = gzip_file.read()
        df = pd.read_csv(StringIO(content))
    df["id"]       = df["trdMatchID"].astype(str)
    df["symbol"]   = mst_id[symbol]
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1}).astype(float).fillna(-1).astype(int)
    df["unixtime"] = df["timestamp"].astype(int)
    df["is_block_trade"] = False
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df = df.groupby(["symbol", "id"]).first().reset_index()
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101", required=True)
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101", required=True)
    parser.add_argument("--num", type=int, default=100)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--update", action='store_true', default=False)
    kwargs_psgre = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 100,
    }
    args   = parser.parse_args()
    res    = requests.post("http://127.0.0.1:8000/select", json={"sql": f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'"}, headers={'Content-type': 'application/json'})
    df_mst = pd.DataFrame(json.loads(res.json()))
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    date_st, date_ed = args.fr, args.to
    assert date_st <= date_ed
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            LOGGER.info(f"date: {date}, symbol: {symbol}")
            if MST_CONV.get(symbol) is None: continue
            df = download_trade(symbol, date, mst_id=mst_id)
            if df.shape[0] > 0:
                res      = requests.post("http://127.0.0.1:8000/select", json={"sql": f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"}, headers={'Content-type': 'application/json'})
                df_exist = pd.DataFrame(json.loads(res.json()))
                df       = df.loc[~df["id"].isin(df_exist["id"])]
            else:
                LOGGER.warning("Nothing data.")
                continue
            if df.shape[0] > 0 and args.update:
                if df.shape[0] >= args.num:
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // args.num)):
                        res = requests.post("http://127.0.0.1:8000/insert", json={"data": df.iloc[indexes].replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_executions", "is_select": True}, headers={'Content-type': 'application/json'})
                else:
                    res = requests.post("http://127.0.0.1:8000/insert", json={"data": df.replace({float("nan"): None}).to_dict(), "tblname": f"{EXCHANGE}_executions", "is_select": True}, headers={'Content-type': 'application/json'})
