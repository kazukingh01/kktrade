import datetime, requests, gzip, argparse
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
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


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz", mst_id: dict=None, scale_pre: dict=None):
    r = requests.get(f"{BASEURL}{MST_CONV[symbol]}/{MST_CONV[symbol]}{date.strftime('%Y-%m-%d')}.csv.gz", allow_redirects=True)
    if r.status_code != 200: return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with gzip.open(tmp_file_path, mode="rt") as gzip_file:
        content = gzip_file.read()
        df = pd.read_csv(StringIO(content))
    df["id"]       = df["trdMatchID"].astype(str)
    df["symbol"]   = mst_id[symbol]
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1}).astype(float).fillna(-1).astype(int)
    df["unixtime"] = (df["timestamp"] * (10 ** 3)).astype(int)
    df["is_block_trade"] = False
    for x in ["price", "size"]:
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    df = df.groupby(["symbol", "id"]).first().reset_index()
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=datetime.datetime.fromisoformat, help="--fr 20200101", required=True)
    parser.add_argument("--to", type=datetime.datetime.fromisoformat, help="--to 20200101", required=True)
    parser.add_argument("--update", action='store_true', default=False)
    kwargs_psgre = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 100,
    }
    args      = parser.parse_args()
    DB        = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", kwargs_psgre=kwargs_psgre, max_disp_len=200)
    df_mst    = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id    = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    scale_pre = {x:y for x, y in df_mst[["symbol_name", "scale_pre"]].values}
    date_st, date_ed = args.fr, args.to
    assert date_st <= date_ed
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            print(date, symbol)
            if MST_CONV.get(symbol) is None: continue
            df = download_trade(symbol, date, mst_id=mst_id, scale_pre=scale_pre[symbol])
            if df.shape[0] > 0:
                df_exist = DB.select_sql(f"select id from {EXCHANGE}_executions_{date.year} where symbol = {df['symbol'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};")
                df = df.loc[~df["id"].isin(df_exist["id"])]
            else:
                print("Nothing data.")
                continue
            if df.shape[0] > 0 and args.update:
                if df.shape[0] > 10000:
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // 10000)):
                        DB.insert_from_df(df.iloc[indexes], f"{EXCHANGE}_executions_{date.year}", is_select=True, n_jobs=8)
                        DB.execute_sql()
                else:
                    DB.execute_copy_from_df(df, f"{EXCHANGE}_executions_{date.year}", filename=f"postgres.{symbol}.{date.strftime('%Y%m%d')}.csv", str_null="", n_jobs=8)
