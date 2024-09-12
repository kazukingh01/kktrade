import datetime, requests, gzip, argparse, json
from zipfile import ZipFile
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.logger import set_logger
from getdata import EXCHANGE


BASEURL  = "https://public.bybit.com/trading/"
BASEURL2 = "https://quote-saver.bycsi.com/orderbook/"
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
    url = f"{BASEURL}{MST_CONV[symbol]}/{MST_CONV[symbol]}{date.strftime('%Y-%m-%d')}.csv.gz"
    LOGGER.info(f"download from: {url}")
    r   = requests.get(url, allow_redirects=True)
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


def download_orderbook(symbol: str, date: datetime.datetime, tmp_file_path: str="./data.zip", mst_id: dict=None):
    # This function is not completed. It might use huge memory.
    url = f"{BASEURL2}{"/".join(symbol.split("@"))}/{date.strftime('%Y-%m-%d')}_{MST_CONV[symbol]}_ob500.data.zip"
    LOGGER.warning(f"download from: {url}, this process use about 25GB RAM.")
    r   = requests.get(url, allow_redirects=True)
    if r.status_code != 200: return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with ZipFile(tmp_file_path, mode="r") as zip_file:
        with zip_file.open(zip_file.filelist[0].filename) as f:
            contents = f.read()
            contents = contents.decode()
            contents = "[" + contents.replace("\r\n", ","[:-1]) + "]"
            df       = pd.DataFrame(json.loads(contents))
    """
    >>> df.iloc[-3]["data"]
    {'s': 'BTCUSDT', 'b': [['44238.80', '2.448'], ['44238.30', '0.023'], ['44237.10', '1.063'], ['44237.00', '0'], ['44236.90', '0'], ['44235.20', '0.452'], ['44234.10', '0.837'], ['44233.70', '0.005'], ['44233.20', '1.300'], ['44232.80', '0.117'], ['44232.10', '0.011'], ['44231.60', '0.121'], ['44231.20', '0'], ['44231.10', '0'], ['44231.00', '2.218'], ['44230.90', '1.459'], ['44230.70', '0.113'], ['44230.50', '0'], ['44230.40', '0'], ['44229.30', '0'], ['44228.30', '0.046'], ['44227.50', '0.005'], ['44224.10', '0.229'], ['44223.00', '0'], ['44222.60', '0.045'], ['44221.20', '0.467'], ['44220.80', '5.153'], ['44219.30', '5.841'], ['44219.20', '1.311'], ['44217.90', '2.784'], ['44217.60', '0'], ['44217.40', '0.023'], ['44215.40', '0'], ['44215.10', '1.784'], ['44214.10', '0'], ['44211.50', '0.939'], ['44210.50', '1.544'], ['44209.90', '0.011'], ['44209.00', '0'], ['44208.90', '0.022'], ['44208.80', '0'], ['44208.40', '0'], ['44184.30', '7.287'], ['44170.40', '10.762'], ['44155.40', '15.853'], ['44150.00', '0.783'], ['44149.90', '2.275'], ['44149.70', '0.050'], ['44149.40', '0.689'], ['44149.30', '1.023'], ['44148.90', '0.001']], 'a': [['44238.90', '0.301'], ['44241.30', '0.009'], ['44244.90', '1.653'], ['44245.80', '1.301'], ['44247.10', '1.652'], ['44251.50', '1.376'], ['44252.40', '1.300'], ['44253.00', '1.723'], ['44253.10', '0.904'], ['44253.30', '0.723'], ['44253.60', '1.000'], ['44256.20', '0.046'], ['44256.60', '0.698'], ['44258.00', '0.046'], ['44258.30', '4.317'], ['44259.20', '0.114'], ['44260.40', '0.227'], ['44261.30', '1.559'], ['44261.70', '0.046'], ['44262.20', '0.300'], ['44262.50', '0'], ['44264.30', '2.263'], ['44265.00', '0.049'], ['44265.60', '0'], ['44266.90', '4.518'], ['44268.20', '7.685'], ['44276.50', '0.001'], ['44276.70', '7.227'], ['44277.90', '0.001'], ['44278.20', '0.068'], ['44280.10', '2.301'], ['44291.00', '10.542'], ['44305.90', '15.799'], ['44312.50', '0'], ['44312.60', '0'], ['44312.70', '0']], 'u': 4224151, 'seq': 114105158008}
    >>> df.iloc[-2]["data"]
    {'s': 'BTCUSDT', 'b': [['44238.80', '1.773'], ['44237.10', '0'], ['44236.50', '0'], ['44236.20', '0.638'], ['44233.00', '0.046'], ['44232.80', '0'], ['44232.20', '0'], ['44232.10', '0'], ['44231.00', '0'], ['44230.90', '1.573'], ['44230.80', '0'], ['44230.10', '9.817'], ['44229.90', '1.368'], ['44229.80', '0.350'], ['44229.40', '0'], ['44229.20', '0.120'], ['44229.10', '0.503'], ['44228.40', '0.114'], ['44228.30', '0'], ['44228.10', '0.118'], ['44228.00', '0.114'], ['44227.80', '0.234'], ['44227.70', '1.300'], ['44227.50', '0'], ['44227.20', '2.432'], ['44225.60', '0.499'], ['44225.30', '5.145'], ['44225.00', '0.092'], ['44224.10', '0'], ['44221.30', '0.610'], ['44219.80', '0.105'], ['44218.10', '0.114'], ['44217.80', '2.264'], ['44217.20', '3.157'], ['44216.60', '0.032'], ['44216.30', '0.377'], ['44215.20', '0'], ['44214.90', '1.300'], ['44214.50', '2.638'], ['44209.90', '0'], ['44208.80', '0.046'], ['44208.70', '0.092'], ['44197.30', '2.133'], ['44193.80', '4.526'], ['44189.50', '2.177'], ['44183.00', '0.046'], ['44168.30', '0'], ['44159.60', '0.154'], ['44155.40', '15.989'], ['44153.60', '0'], ['44148.70', '0.574'], ['44148.60', '0.007'], ['44148.50', '0.916'], ['44148.40', '0.046'], ['44148.30', '0.001'], ['44148.20', '0.033'], ['44148.10', '0.011']], 'a': [['44238.90', '0.759'], ['44240.20', '0'], ['44241.30', '0.003'], ['44242.80', '0.048'], ['44243.20', '0.058'], ['44243.30', '0.046'], ['44243.50', '1.300'], ['44243.60', '0.048'], ['44247.00', '0.128'], ['44250.00', '6.172'], ['44250.80', '0.005'], ['44251.50', '1.371'], ['44253.90', '0.016'], ['44254.90', '0.069'], ['44255.60', '1.161'], ['44256.60', '0.676'], ['44257.00', '1.558'], ['44257.30', '0.459'], ['44258.00', '0.007'], ['44258.20', '0.128'], ['44258.30', '4.430'], ['44259.80', '0.114'], ['44260.10', '2.613'], ['44260.40', '0.114'], ['44261.30', '0'], ['44261.40', '0'], ['44263.30', '0.011'], ['44264.30', '0.003'], ['44269.70', '0.061'], ['44277.90', '0.462'], ['44282.30', '0'], ['44308.20', '0.003'], ['44312.30', '0'], ['44312.40', '0']], 'u': 4224152, 'seq': 114105158356}
    >>> df.iloc[-1]["data"]
    {'s': 'BTCUSDT', 'b': [['44238.80', '1.773'], ['44238.40', '0.350'], ['44238.30', '0.023'], ['44238.00', '0.113'], ['44236.20', '0.638'], ['44236.10', '0.271'], ['44236.00', '0.113'], ['44235.60', '1.062'], ['44235.50', '0.010'], ['44235.20', '0.452'], ['44235.10', '0.048'], ['44235.00', '0.021'], ['44234.20', '0.452'], ['44234.10', '0.837'], ['44234.00', '0.137'], ['44233.70', '0.005'], ['44233.20', '1.300'], ['44233.00', '0.046'], ['44232.50', '0.113'], ['44232.00', '0.113'], ['44231.90', '1.814'], ['44231.80', '0.226'], ['44231.60', '0.121'], ['44231.30', '0.117'], ['44230.90', '1.573'], ['44230.70', '0.113'], ['44230.60', '0.030'], ['44230.10', '9.817'], ['44230.00', '36.186'], ['44229.90', '1.368'], ['44229.80', '0.350'], ['44229.60', '2.260'], ['44229.20', '0.120'], ['44229.10', '0.503'], ['44229.00', '0.068'], ['44228.80', '4.145'], ['44228.70', '0.034'], ['44228.60', '0.904'], ['44228.40', '0.114'], ['44228.20', '0
    # df.iloc[-2]: type = delta
    # df.iloc[-1]: type = snapshot
    # ['44237.10', '0'] means that someone has removed it or it has been dealed.
    """
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
