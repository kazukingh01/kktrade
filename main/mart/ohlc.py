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
            df["timegroup"] = (df["unixtime"] // interval * interval)
            df["cumsum"]    = df.groupby(["symbol", "timegroup", "side"])["amount"].cumsum()
            ndf_tg          = np.arange(int(args.fr.timestamp()) // interval * interval, int(args.to.timestamp()) // interval * interval, interval, dtype=int)
            dfwk            = df.groupby(["symbol", "timegroup", "side"])["price"].aggregate(["max", "min", "first", "last"])
            dfwkwk          = df.groupby(["symbol", "timegroup", "side"])[["size", "amount"]].sum()
            dfwk["size"]    = dfwkwk.loc[:, "size"]
            dfwk["amount"]  = dfwkwk.loc[:, "amount"]
            dfwk["ntx"]     = df.groupby(["symbol", "timegroup", "side"]).size()
            dfwk["ave"]     = dfwk["amount"] / dfwk["size"]
            ndfwk           = np.arange(0.1, 1.0, 0.2)
            dfwkwk          = df.groupby(["symbol", "timegroup", "side"])[["amount", "cumsum"]].quantile(ndfwk)
            for x in ndfwk:
                dfwk[f"amount_q{str(int(x * 100)).zfill(2)}"] = dfwkwk.loc[(slice(None), slice(None), slice(None), x)]["amount"] / dfwk["amount"]
                dfwk[f"cumsum_q{str(int(x * 100)).zfill(2)}"] = dfwkwk.loc[(slice(None), slice(None), slice(None), x)]["cumsum"] / dfwk["amount"]
            raise
            dfwk    = dfwk.reset_index()
            ndf_sbl = df["symbol"].unique()
            df_ohlc = pd.DataFrame(np.concatenate([np.repeat(ndf_sbl, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_sbl.shape[0]).reshape(-1, 1)], axis=-1), columns=["symbol", "timegroup"])
            df_ohlc = pd.merge(df_ohlc, dfwk, how="left", on=["symbol", "timegroup"])
            raise