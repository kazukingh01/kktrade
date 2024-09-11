import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kktrade.util.math import NonLinearXY
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE
from kktrade.util.logger import set_logger
from ohlc_base import convdatetime


LOGGER        = set_logger(__name__)
INTERVALS     = [120, 300, 600, 1200, ]
INTERVAL_BASE = 60
N_DIVIDE      = 20
COLUMNS       = [
    'size_sum',  'ntx_sum',  'amount_sum',
    'size_diff', 'ntx_diff', 'amount_diff',
    'amount_q000_ask', 'amount_q005_ask', 'amount_q010_ask', 'amount_q015_ask', 'amount_q020_ask', 'amount_q025_ask', 'amount_q030_ask', 'amount_q035_ask', 'amount_q040_ask', 'amount_q045_ask', 'amount_q050_ask', 'amount_q055_ask', 'amount_q060_ask', 'amount_q065_ask', 'amount_q070_ask', 'amount_q075_ask', 'amount_q080_ask', 'amount_q085_ask', 'amount_q090_ask', 'amount_q095_ask', 'amount_q100_ask',
    'amount_q000_bid', 'amount_q005_bid', 'amount_q010_bid', 'amount_q015_bid', 'amount_q020_bid', 'amount_q025_bid', 'amount_q030_bid', 'amount_q035_bid', 'amount_q040_bid', 'amount_q045_bid', 'amount_q050_bid', 'amount_q055_bid', 'amount_q060_bid', 'amount_q065_bid', 'amount_q070_bid', 'amount_q075_bid', 'amount_q080_bid', 'amount_q085_bid', 'amount_q090_bid', 'amount_q095_bid', 'amount_q100_bid',
    'amount_p01_ask',  'amount_p02_ask',  'amount_p03_ask',  'amount_p04_ask',  'amount_p05_ask',  'amount_p06_ask',  'amount_p07_ask',  'amount_p08_ask',  'amount_p09_ask',  'amount_p10_ask',
    'amount_p01_bid',  'amount_p02_bid',  'amount_p03_bid',  'amount_p04_bid',  'amount_p05_bid',  'amount_p06_bid',  'amount_p07_bid',  'amount_p08_bid',  'amount_p09_bid',  'amount_p10_bid',
    'cumsum_p01_ask',  'cumsum_p02_ask',  'cumsum_p03_ask',  'cumsum_p04_ask',  'cumsum_p05_ask',  'cumsum_p06_ask',  'cumsum_p07_ask',  'cumsum_p08_ask',  'cumsum_p09_ask',
    'cumsum_p01_bid',  'cumsum_p02_bid',  'cumsum_p03_bid',  'cumsum_p04_bid',  'cumsum_p05_bid',  'cumsum_p06_bid',  'cumsum_p07_bid',  'cumsum_p08_bid',  'cumsum_p09_bid',
]


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=convdatetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=convdatetime, help="--to 20200101", default= timenow)
    parser.add_argument("--update", action='store_true', default=False)
    args    = parser.parse_args()
    DB      = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200)
    ndfcol  = np.array(DB.db_layout["mart_ohlc"])
    ndfcol  = ndfcol[ndfcol != "attrs"]
    df      = DB.select_sql(
        f"SELECT " + ", ".join(ndfcol.tolist()) + ", " + ", ".join(['CAST(attrs->"$.' + x + '" AS FLOAT) as ' + x for x in COLUMNS]) + " FROM mart_ohlc " + 
        f"WHERE interval = {INTERVAL_BASE} and unixtime >= {int(args.fr.timestamp())} and unixtime < {int(args.to.timestamp())};"
    ).sort_values(["symbol", "unixtime"]).reset_index(drop=True).set_index(["symbol", "unixtime"])
    df_ohlc = pd.DataFrame(index=df.index.copy())
    for interval in INTERVALS:
        num   = interval // INTERVAL_BASE
        nlist = [x for x in range(num)]
        dfwk  = df.groupby("symbol").shift(nlist)
        df_ohlc["interval"] = interval
        df_ohlc["open"]     = dfwk[[f"open_{x}"  for x in nlist]].iloc[:, -1]
        df_ohlc["high"]     = dfwk[[f"high_{x}"  for x in nlist]].max(axis=1)
        df_ohlc["low"]      = dfwk[[f"low_{x}"   for x in nlist]].min(axis=1)
        df_ohlc["close"]    = dfwk[[f"close_{x}" for x in nlist]].iloc[:, 0]
        for name in ["ask", "bid"]:
            for col in ["amount", "size", "ntx"]:
                df_ohlc[f"{col}_{name}"] = dfwk[[f"{col}_{name}_{x}" for x in nlist]].fillna(0).sum(axis=1)
        for name in ["ask", "bid"]:
            df_ohlc[f"ave_{name}"] = (df_ohlc[f"amount_{name}"] / df_ohlc[f"size_{name}"]).replace(float("inf"), float("nan"))
        for col in ["amount", "size", "ntx"]:
            df_ohlc[f"{col}_sum" ] = df_ohlc[f"{col}_ask"] + df_ohlc[f"{col}_bid"]
            df_ohlc[f"{col}_diff"] = df_ohlc[f"{col}_ask"] - df_ohlc[f"{col}_bid"]
            df_ohlc[f"{col}_sum" ] = df_ohlc[f"{col}_sum" ].fillna(0)
            df_ohlc[f"{col}_diff"] = df_ohlc[f"{col}_diff"].fillna(0)
        df_ohlc["ave"] = (df_ohlc["amount_sum"] / df_ohlc["size_sum"]).replace(float("inf"), float("nan"))
        # amount_qXXX_ask/bid
        ndf_div = np.arange(0, 1.0000000001, 1/N_DIVIDE)
        for name in ["ask", "bid"]:
            dftmp = pd.DataFrame(index=dfwk.index.copy())
            for n in nlist:
                dfwkwk = pd.DataFrame(index=dfwk.index.copy())
                dfwkwk["func"] = dfwk[[f"amount_q{str(int(x*100)).zfill(3)}_{name}_{n}" for x in ndf_div]].apply(lambda x: NonLinearXY(ndf_div, x.values.copy()), axis=1)
                dfwkwk["x"]    = dfwk.loc[dfwk[f"ntx_{name}_{n}"] >= 2, f"ntx_{name}_{n}"].apply(lambda x: np.array(np.arange(0, 1, 1/(x-1)).tolist() + [1.0]))
                dfwkwk.loc[dfwk[f"ntx_{name}_{n}"] == 1, "x"] = dfwkwk.loc[dfwk[f"ntx_{name}_{n}"] == 1, "x"].apply(lambda x: np.array([0.0]))
                dfwkwk.loc[dfwk[f"ntx_{name}_{n}"] == 0, "x"] = dfwkwk.loc[dfwk[f"ntx_{name}_{n}"] == 0, "x"].apply(lambda x: np.array([]))
                dfwkwk.loc[dfwkwk["x"].isna(),           "x"] = dfwkwk.loc[dfwkwk["x"].isna(),           "x"].apply(lambda x: np.array([]))
                dftmp[n] = dfwkwk.apply(lambda x: x["func"].call_numpy(x["x"]), axis=1)
            setmp = dftmp.apply(lambda x: np.concatenate(x), axis=1)
            setmp = setmp.loc[~setmp.str[0].isna()].apply(lambda x: np.quantile(x, ndf_div))
            for q, idx in zip(ndf_div, np.arange(N_DIVIDE+1)):
                df_ohlc[f"amount_q{str(int(q*100)).zfill(3)}_{name}"] = setmp.str[idx]
                df_ohlc = df_ohlc
        # amount_pXX_ask/bid
        for name in ["ask", "bid"]:
            ndftmp = None
            for n in nlist:
                ndfwk = dfwk[[f"amount_p{str(y).zfill(2)}_{name}_{n}" for y in range(10, 0, -1)]].values.copy()
                if ndftmp is None: ndftmp = ndfwk.copy()
                else: ndftmp = np.concatenate([ndftmp, ndfwk], axis=-1)
            ndftmp[np.isnan(ndftmp)] = 0.0
            ndftmp = ndftmp.reshape(-1, 10, num).sum(axis=-1)
            for p, idx in zip(range(10, 0, -1), range(10)):
                df_ohlc[f"amount_p{str(p).zfill(2)}_{name}"] = ndftmp[:, idx]