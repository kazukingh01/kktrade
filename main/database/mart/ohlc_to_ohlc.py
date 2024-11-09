import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kktrade.core.mart import get_executions, EXCHANGES
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_price_over_time, ana_distribution_volume_over_price, ana_rank_corr_index, indexes_to_aggregate, check_common_input, check_interval
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import \
    HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kkpsgre.util.logger import set_logger


LOGGER  = set_logger(__name__)
COLUMNS = [
    'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid',
    'ave_p0050','ave_p0150','ave_p0250','ave_p0350','ave_p0450','ave_p0550','ave_p0650','ave_p0750','ave_p0850','ave_p0950',
    'var_p0050','var_p0150','var_p0250','var_p0350','var_p0450','var_p0550','var_p0650','var_p0750','var_p0850','var_p0950',
    'price_h0000','price_h0050','price_h0100','price_h0150','price_h0200','price_h0250','price_h0300','price_h0350','price_h0400','price_h0450','price_h0500','price_h0550','price_h0600','price_h0650','price_h0700','price_h0750','price_h0800','price_h0850','price_h0900','price_h0950','price_h1000',
    'size_p0050_ask','size_p0050_bid','size_p0150_ask','size_p0150_bid','size_p0250_ask','size_p0250_bid','size_p0350_ask','size_p0350_bid','size_p0450_ask','size_p0450_bid','size_p0550_ask','size_p0550_bid','size_p0650_ask','size_p0650_bid','size_p0750_ask','size_p0750_bid','size_p0850_ask','size_p0850_bid','size_p0950_ask','size_p0950_bid',
    'volume_p0050_ask','volume_p0050_bid','volume_p0150_ask','volume_p0150_bid','volume_p0250_ask','volume_p0250_bid','volume_p0350_ask','volume_p0350_bid','volume_p0450_ask','volume_p0450_bid','volume_p0550_ask','volume_p0550_bid','volume_p0650_ask','volume_p0650_bid','volume_p0750_ask','volume_p0750_bid','volume_p0850_ask','volume_p0850_bid','volume_p0950_ask','volume_p0950_bid',
    'volume_q0000_ask','volume_q0000_bid','volume_q0050_ask','volume_q0050_bid','volume_q0100_ask','volume_q0100_bid','volume_q0150_ask','volume_q0150_bid','volume_q0200_ask','volume_q0200_bid','volume_q0250_ask','volume_q0250_bid','volume_q0300_ask','volume_q0300_bid','volume_q0350_ask','volume_q0350_bid','volume_q0400_ask','volume_q0400_bid','volume_q0450_ask','volume_q0450_bid',
    'volume_q0500_ask','volume_q0500_bid','volume_q0550_ask','volume_q0550_bid','volume_q0600_ask','volume_q0600_bid','volume_q0650_ask','volume_q0650_bid','volume_q0700_ask','volume_q0700_bid','volume_q0750_ask','volume_q0750_bid','volume_q0800_ask','volume_q0800_bid','volume_q0850_ask','volume_q0850_bid','volume_q0900_ask','volume_q0900_bid','volume_q0950_ask','volume_q0950_bid','volume_q1000_ask','volume_q1000_bid',
    'volume_price_h0025_ask','volume_price_h0025_bid','volume_price_h0075_ask','volume_price_h0075_bid','volume_price_h0125_ask','volume_price_h0125_bid','volume_price_h0175_ask','volume_price_h0175_bid','volume_price_h0225_ask','volume_price_h0225_bid','volume_price_h0275_ask','volume_price_h0275_bid','volume_price_h0325_ask','volume_price_h0325_bid','volume_price_h0375_ask','volume_price_h0375_bid','volume_price_h0425_ask','volume_price_h0425_bid','volume_price_h0475_ask','volume_price_h0475_bid',
    'volume_price_h0525_ask','volume_price_h0525_bid','volume_price_h0575_ask','volume_price_h0575_bid','volume_price_h0625_ask','volume_price_h0625_bid','volume_price_h0675_ask','volume_price_h0675_bid','volume_price_h0725_ask','volume_price_h0725_bid','volume_price_h0775_ask','volume_price_h0775_bid','volume_price_h0825_ask','volume_price_h0825_bid','volume_price_h0875_ask','volume_price_h0875_bid','volume_price_h0925_ask','volume_price_h0925_bid','volume_price_h0975_ask','volume_price_h0975_bid',
]



if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--sr", type=int, help="sampling rate. --sr 120", default=120)
    parser.add_argument("--itvls",  type=lambda x: [int(y) for y in x.split(",")], default="120,480,2400,14400,86400")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    DB   = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df   = DB.select_sql(
        f"SELECT symbol, unixtime, type, interval, open, high, low, close, ave, " + ",".join([f"attrs->'{x}' as {x}" for x in COLUMNS]) + " " + 
        f"FROM mart_ohlc WHERE type = 0 AND interval = 60 AND unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
    )
    df["unixtime"] = (df["unixtime"].astype("int64") / 10e8).astype(int)
    for interval in args.itvls:
        sampling_rate = args.sr
        unixtime_name = "unixtime"
        from_tx = False
        df_ohlc    = create_ohlc(df[['symbol', 'unixtime', 'type', 'interval', 'open', 'high', 'low', 'close']], "unixtime", interval, args.sr, args.fr, args.to, index_names=["symbol"], from_tx=False)
        index_base = df_ohlc.index.copy()
        list_df    = []
        list_df.append(ana_size_price(                         df[['symbol', 'unixtime', 'interval', 'ave', 'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid']],                     "unixtime", interval, args.sr, index_base, from_tx=False))
        list_df.append(ana_quantile_tx_volume(                 df[["symbol", "unixtime", "interval", "ntx_ask", "ntx_bid"] + df.columns[df.columns.str.find("volume_q") == 0].tolist()],                     "unixtime", interval, args.sr, index_base, from_tx=False, n_div=20))
        list_df.append(ana_distribution_volume_price_over_time(df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(volume|size|ave|var)_p[0-9]")]].tolist()], "unixtime", interval, args.sr, index_base, from_tx=False, n_div=10))
        list_df.append(ana_distribution_volume_over_price(     df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(price|volume_price)_h[0-9]" )]].tolist()], "unixtime", interval, args.sr, index_base, from_tx=False, n_div=20))
        # ana_rank_corr_index(dfwk, "unixtime", interval, args.sr, index_base)
    raise
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