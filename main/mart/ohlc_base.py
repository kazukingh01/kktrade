import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE
from kkpsgre.util.logger import set_logger


EXCHANGES = ["bitflyer", "bybit", "binance"]
LOGGER    = set_logger(__name__)
INTERVALS = [60, ]
DIVIDES   = 10


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
        sql = f"select * from {exchange}_executions where side in (0,1) and unixtime >= {int(args.fr.timestamp())} and unixtime < {int(args.to.timestamp())};"
        LOGGER.info(f"exchange: {exchange}, from: {args.fr}, to: {args.to}, \nsql: {sql}")
        df  = DB.select_sql(sql)
        df  = df.sort_values(["symbol", "unixtime", "price"]).reset_index(drop=True)
        df["amount"] = (df["price"] * df["size"])
        for interval in INTERVALS:
            df["timegrp" ] = (df["unixtime"] // interval * interval)
            df["timegrp2"] = (df["unixtime"] // (interval // DIVIDES) * (interval // DIVIDES))
            ndf_tg         = np.arange(int(args.fr.timestamp()) // interval * interval, int(args.to.timestamp()) // interval * interval, interval, dtype=int)
            ndf_sbl        = df["symbol"].unique()
            ndf_idx        = np.concatenate([np.repeat(ndf_sbl, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_sbl.shape[0]).reshape(-1, 1)], axis=-1)
            df_ohlc        = pd.DataFrame(ndf_idx, columns=["symbol", "timegrp"]).set_index(["symbol", "timegrp"])
            df_ohlc[["open", "high", "low", "close"]] = df.groupby(["symbol", "timegrp"])["price"].aggregate(["first", "max", "min", "last"])
            df_ohlc["interval"] = interval
            dfwk           = df.groupby(["symbol", "timegrp", "side"])[["size", "amount"]].sum()
            dfwk["ntx"]    = df.groupby(["symbol", "timegrp", "side"]).size()
            dfwk["ave"]    = dfwk["amount"] / dfwk["size"]
            for side, name in zip([0, 1], ["ask", "bid"]):
                df_ohlc[[f"ave_{name}", f"size_{name}", f"ntx_{name}", f"amount_{name}"]] = dfwk.loc[(slice(None), slice(None), side)][["ave", "size", "ntx", "amount"]]
                df_ohlc[[               f"size_{name}", f"ntx_{name}", f"amount_{name}"]] = df_ohlc[[f"size_{name}", f"ntx_{name}", f"amount_{name}"]].fillna(0)
            for col in ["amount", "size", "ntx"]:
                df_ohlc[f"{col}_sum" ] = df_ohlc[f"{col}_ask"] + df_ohlc[f"{col}_bid"]
                df_ohlc[f"{col}_diff"] = df_ohlc[f"{col}_ask"] - df_ohlc[f"{col}_bid"]
                df_ohlc[f"{col}_sum" ] = df_ohlc[f"{col}_sum" ].fillna(0)
                df_ohlc[f"{col}_diff"] = df_ohlc[f"{col}_diff"].fillna(0)
            df_ohlc["ave"] = (df_ohlc["amount_sum"] / df_ohlc["size_sum"]).replace(float("inf"), float("nan"))
            ndfwk          = list(np.arange(0., 1.0, 0.05)) + [1.0, ]
            dfwk           = df.groupby(["symbol", "timegrp", "side"])["amount"].quantile(ndfwk)
            for x in ndfwk:
                for side, name in zip([0, 1], ["ask", "bid"]):
                    df_ohlc[f"amount_q{str(int(x * 100)).zfill(3)}_{name}"] = dfwk.loc[(slice(None), slice(None), side, x)]
            ndf_tg  = np.arange(int(args.fr.timestamp()) // interval * interval, int(args.to.timestamp()) // (interval // DIVIDES) * (interval // DIVIDES), (interval // DIVIDES), dtype=int)
            ndf_idx = np.concatenate([np.repeat(ndf_sbl, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_sbl.shape[0]).reshape(-1, 1)], axis=-1)
            dfwk    = pd.DataFrame(ndf_idx, columns=["symbol", "timegrp2"]).set_index(["symbol", "timegrp2"])
            dfwkwk  = df.groupby(["symbol", "timegrp2", "side"])["amount"].sum()
            for side, name in zip([0, 1], ["ask", "bid"]): dfwk[f"amount_{name}"] = dfwkwk.loc[(slice(None), slice(None), side)]
            dfwk    = dfwk.reset_index()
            dfwk["timegrp"] = dfwk["timegrp2"] // interval * interval
            dfwk[["amount_ask", "amount_bid"]] = dfwk[["amount_ask", "amount_bid"]].fillna(0)
            dfwk[["cumsum_ask", "cumsum_bid"]] = dfwk.groupby(["symbol", "timegrp"])[["amount_ask", "amount_bid"]].cumsum()
            dfwk["timegrp2"] = (dfwk["timegrp2"] - dfwk["timegrp"]) // (interval // DIVIDES) + 1
            dfwk = dfwk.set_index(["symbol", "timegrp", "timegrp2"])
            for x in range(1, DIVIDES + 1):
                for side, name in zip([0, 1], ["ask", "bid"]):
                    df_ohlc[    f"amount_p{str(x).zfill(2)}_{name}"] = dfwk.loc[(slice(None), slice(None), x)][f"amount_{name}"]
                    if x < 10:
                        df_ohlc[f"cumsum_p{str(x).zfill(2)}_{name}"] = dfwk.loc[(slice(None), slice(None), x)][f"cumsum_{name}"]
            df_ohlc = df_ohlc.reset_index()
            df_ohlc.columns     = df_ohlc.columns.str.replace("timegrp", "unixtime")
            df_ohlc["unixtime"] = (df_ohlc["unixtime"] + interval)
            df_ohlc["attrs"]    = df_ohlc.loc[:, df_ohlc.columns[~df_ohlc.columns.isin(DB.db_layout["mart_ohlc"])]].apply(lambda x: str({y:z for y, z in x.to_dict().items() if not (z is None or np.isnan(z))}).replace("'", '"'), axis=1)
            if args.update and df_ohlc.shape[0] > 0:
                DB.set_sql(
                    f"DELETE FROM mart_ohlc WHERE interval = {interval} and symbol in (" + ",".join(df_ohlc["symbol"].unique().astype(str).tolist()) + ") and " + 
                    f"unixtime in (" + ",".join(df_ohlc["unixtime"].unique().astype(str).tolist()) + ");"
                )
                DB.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
                DB.execute_sql()
