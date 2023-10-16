import datetime, sys, re, requests, gzip, time
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from kktrade.config.com import SCALE_MST, NAME_MST
from getdata import SCALE, EXCHANGE


BASEURL  = "https://public.bybit.com/trading/"
MST_CONV = {
    "linear@BTCUSDT": "BTCUSDT",
    "inverse@BTCUSD": "BTCUSD",
    "linear@ETHUSDT": "ETHUSDT",
    "inverse@ETHUSD": "ETHUSD",
    "linear@XRPUSDT": "XRPUSDT",
    "inverse@XRPUSD": "XRPUSD",
}


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz"):
    r = requests.get(f"{BASEURL}{MST_CONV[symbol]}/{MST_CONV[symbol]}{date.strftime('%Y-%m-%d')}.csv.gz", allow_redirects=True)
    if r.status_code != 200: return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with gzip.open(tmp_file_path, mode="rt") as gzip_file:
        content = gzip_file.read()
        df = pd.read_csv(StringIO(content))
    df["id"]       = df["trdMatchID"].astype(str)
    df["symbol"]   = NAME_MST[symbol]
    df["scale"]    = SCALE[symbol]
    df["type"]     = df["side"].map({"Sell": "SELL", "Buy": "BUY"}).fillna("OTHR")
    df["price"]    = (df["price"].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][0])).fillna(-1).astype(int)
    df["size"]     = (df["size" ].astype(float) * (10 ** SCALE_MST[SCALE[symbol]][1])).fillna(-1).astype(int)
    df["unixtime"] = (df["timestamp"] * (10 ** 3)).astype(int)
    df["is_block_trade"] = False
    df = df.groupby(["symbol", "id"]).first().reset_index()
    return df


if __name__ == "__main__":
    DB   = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    args = sys.argv[1:]
    assert len(re.findall("^[0-9]+$", args[-2])) > 0
    assert len(re.findall("^[0-9]+$", args[-1])) > 0
    date_st, date_ed = datetime.datetime.fromisoformat(args[-2]), datetime.datetime.fromisoformat(args[-1])
    assert date_st <= date_ed
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in SCALE.keys():
            print(date, symbol)
            if MST_CONV.get(symbol) is None: continue
            df = download_trade(symbol, date)
            if df.shape[0] > 0:
                df_exist = DB.select_sql(
                    f"select symbol, id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and id in ('" + "','".join(df['id'].astype(str).tolist()) + "') and " + 
                    f"unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};"
                )
                df = df.loc[~df["id"].isin(df_exist["id"])]
            else:
                print("Nothing data.")
                continue
            if df.shape[0] > 0:
                if df.shape[0] > 10000:
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // 10000)):
                        DB.insert_from_df(df.iloc[indexes], f"{EXCHANGE}_executions", is_select=True, n_jobs=8)
                        DB.execute_sql()
                else:
                    DB.execute_copy_from_df(df, f"{EXCHANGE}_executions", filename=f"postgres.{symbol}.{date.strftime('%Y%m%d')}.csv", str_null="", n_jobs=8)
