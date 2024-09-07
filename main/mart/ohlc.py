import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE
from kkpsgre.util.logger import set_logger


EXCHANGES = ["bybit", "binance", "bitflyer"]
LOGGER    = set_logger(__name__)
INTERVALS = [60, ]


def convdatetime(x: str):
    if   len(x) == 8:
        return datetime.datetime.strptime(x + "000000 +0000", "%Y%m%d%H%M%S %z")
    elif len(x) == 12:
        return datetime.datetime.strptime(x + " +0000", "%Y%m%d%H%M%S %z")
    else:
        raise Exception("FORMAT ex) 20240101090000")


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=convdatetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=convdatetime, help="--to 20200101", default= timenow)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    DB     = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200)
    df_mst = DB.select_sql(f"select * from master_symbol where is_active = true;")
    for exchange in EXCHANGES:
        LOGGER.info(f"exchange: {exchange}, from: {args.fr}, to: {args.to}")
        df = DB.select_sql(f"select * from {exchange}_executions where side in (0,1) and unixtime >= {int(args.fr.timestamp())} and unixtime < {int(args.to.timestamp())};")
        df = df.sort_values(["symbol", "unixtime", "price"]).reset_index(drop=True)
        df["amount"] = (df["price"] * df["size"])
        for interval in INTERVALS:
            df["timegrp"] = (df["unixtime"] // interval * interval)
            df["cumsum"]  = df.groupby(["symbol", "timegrp", "side"])["amount"].cumsum()
            ndf_tg        = np.arange(int(args.fr.timestamp()) // interval * interval, int(args.to.timestamp()) // interval * interval, interval, dtype=int)
            ndf_idx       = df["symbol"].unique()
            ndf_idx       = np.concatenate([np.repeat(ndf_idx, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idx.shape[0]).reshape(-1, 1)], axis=-1)
            df_ohlc       = pd.DataFrame(ndf_idx, columns=["symbol", "timegrp"]).set_index(["symbol", "timegrp"])
            df_ohlc[["open", "high", "low", "close"]] = df.groupby(["symbol", "timegrp"])["price"].aggregate(["first", "max", "min", "last"])
            dfwk          = df.groupby(["symbol", "timegrp", "side"])[["size", "amount"]].sum()
            dfwk["ntx"]   = df.groupby(["symbol", "timegrp", "side"]).size()
            dfwk["ave"]   = dfwk["amount"] / dfwk["size"]
            for side, name in zip([0, 1], ["ask", "bid"]):
                df_ohlc[[f"size_{name}", f"ntx_{name}", f"amount_{name}"]] = dfwk.loc[(slice(None), slice(None), side)][["size", "ntx", "amount"]]
                df_ohlc[[f"size_{name}", f"ntx_{name}", f"amount_{name}"]] = df_ohlc[[f"size_{name}", f"ntx_{name}", f"amount_{name}"]].fillna(0)
            df_ohlc[[f"size_sum",  f"ntx_sum",  f"amount_sum" ]] = dfwk.loc[(slice(None), slice(None), 0)][["size", "ntx", "amount"]] + dfwk.loc[(slice(None), slice(None), 1)][["size", "ntx", "amount"]]
            df_ohlc[[f"size_diff", f"ntx_diff", f"amount_diff"]] = dfwk.loc[(slice(None), slice(None), 0)][["size", "ntx", "amount"]] - dfwk.loc[(slice(None), slice(None), 1)][["size", "ntx", "amount"]]
            df_ohlc[[f"size_sum",  f"ntx_sum",  f"amount_sum" ]] = df_ohlc[[f"size_sum",  f"ntx_sum",  f"amount_sum" ]].fillna(0)
            df_ohlc[[f"size_diff", f"ntx_diff", f"amount_diff"]] = df_ohlc[[f"size_diff", f"ntx_diff", f"amount_diff"]].fillna(0)
            df_ohlc["ave"] = (df_ohlc["amount_sum"] / df_ohlc["size_sum"]).replace(float("inf"), float("nan"))
            ndfwk          = np.arange(0.1, 1.0, 0.2)
            dfwk           = df.groupby(["symbol", "timegrp", "side"])[["amount", "cumsum"]].quantile(ndfwk)
            for x in ndfwk:
                for side, name in zip([0, 1], ["ask", "bid"]):
                    df_ohlc[f"amount_q{str(int(x * 100)).zfill(2)}_{name}"] = dfwk.loc[(slice(None), slice(None), side, x)]["amount"] / df_ohlc[f"amount_{name}"]
                    df_ohlc[f"cumsum_q{str(int(x * 100)).zfill(2)}_{name}"] = dfwk.loc[(slice(None), slice(None), side, x)]["cumsum"] / df_ohlc[f"amount_{name}"]
