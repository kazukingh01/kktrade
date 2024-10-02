import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import \
    HOST_BS, PORT_BS, USER_BS, PASS_BS, DBNAME_BS, DBTYPE_BS, \
    HOST_BK, PORT_BK, USER_BK, PASS_BK, DBNAME_BK, DBTYPE_BK, \
    HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kkpsgre.util.logger import set_logger


EXCHANGES = ["bitflyer", "bybit", "binance"]
LOGGER    = set_logger(__name__)
INTERVALS = [60, ]
DIVIDES   = 10


def get_executions(db_bs: DBConnector, db_bk: DBConnector, exchange: str, date_fr: datetime.datetime, date_to: datetime.datetime, date_sw: datetime.datetime):
    assert isinstance(db_bs, DBConnector)
    assert isinstance(db_bk, DBConnector)
    assert isinstance(exchange, str) and exchange in EXCHANGES
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert isinstance(date_sw, datetime.datetime)
    assert date_fr < date_to
    if   date_fr >= date_sw:
        df = db_bs.select_sql( f"SELECT * FROM {exchange}_executions WHERE side in (0,1) and unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    elif date_to <= date_sw:
        df = db_bk.select_sql( f"SELECT * FROM {exchange}_executions WHERE side in (0,1) and unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    else:
        df1 = db_bk.select_sql(f"SELECT * FROM {exchange}_executions WHERE side in (0,1) and unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        df2 = db_bs.select_sql(f"SELECT * FROM {exchange}_executions WHERE side in (0,1) and unixtime >= '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        df  = pd.concat([df2, df1], axis=0, sort=False, ignore_index=True)
    return df


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--switch", type=str_to_datetime, help="--switch 20200101", default=(timenow - datetime.timedelta(days=7)))
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    DB_BS  = DBConnector(HOST_BS, PORT_BS, DBNAME_BS, USER_BS, PASS_BS, dbtype=DBTYPE_BS, max_disp_len=200)
    DB_BK  = DBConnector(HOST_BK, PORT_BK, DBNAME_BK, USER_BK, PASS_BK, dbtype=DBTYPE_BK, max_disp_len=200)
    DB_TO  = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df_mst = DB_BS.select_sql(f"select * from master_symbol where is_active = true;")
    for exchange in EXCHANGES:
        LOGGER.info(f"exchange: {exchange}, from: {args.fr}, to: {args.to}")
        df = get_executions(DB_BS, DB_BK, exchange, args.fr, args.to, args.switch)
        assert isinstance(df["unixtime"].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        df = df.sort_values(["symbol", "unixtime", "price"]).reset_index(drop=True)
        df = pd.merge(df, df_mst, how="left", left_on="symbol", right_on="symbol_id")
        df["dateitme"] = df["unixtime"].copy()
        df["unixtime"] = df["unixtime"].astype("int64") / 10e8
        boolwk = ((df["symbol_name"].str.find("inverse@") == 0) | (df["symbol_name"].str.find("COIN@") == 0))
        df["amount"] = (df["price"] * df["size"])
        df.loc[boolwk, "amount"] = df.loc[boolwk, "size"]
        for interval in INTERVALS:
            assert interval % DIVIDES == 0
            df["timegrp" ] = (df["unixtime"] // interval * interval).astype(int)
            df["timegrp2"] = (df["unixtime"] // (interval // DIVIDES) * (interval // DIVIDES)).astype(int)
            ndf_tg         = np.arange(int(args.fr.timestamp()) // interval * interval, int(args.to.timestamp()) // interval * interval, interval, dtype=int)
            ndf_sbl        = df["symbol"].unique()
            ndf_idx        = np.concatenate([np.repeat(ndf_sbl, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_sbl.shape[0]).reshape(-1, 1)], axis=-1)
            df_ohlc        = pd.DataFrame(ndf_idx, columns=["symbol", "timegrp"]).set_index(["symbol", "timegrp"])
            # basics. open, high, low, close
            df_ohlc[["open", "high", "low", "close"]] = df.groupby(["symbol", "timegrp"])["price"].aggregate(["first", "max", "min", "last"])
            df_ohlc["interval"] = interval
            # average price, number of transactions, size, amount
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
            # quantile amount of each transactions in each time group
            ndfwk          = list(np.arange(0., 1.0, 0.05)) + [1.0, ]
            dfwk           = df.groupby(["symbol", "timegrp", "side"])["amount"].quantile(ndfwk)
            for x in ndfwk:
                for side, name in zip([0, 1], ["ask", "bid"]):
                    df_ohlc[f"amount_q{str(int(x * 100)).zfill(3)}_{name}"] = dfwk.loc[(slice(None), slice(None), side, x)]
            # divide each time group more. calcurate amount and cumsum in divided time frame 
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
            # analyze amount in each price in each time group
            df["amount_ask"] = df["amount"] * (df["side"] == 0).astype(float)
            df["amount_bid"] = df["amount"] * (df["side"] == 1).astype(float)
            dfwk = df.groupby(["symbol", "timegrp"])[["price", "amount_ask", "amount_bid"]].apply(
                lambda x: np.concatenate(np.histogram(x["price"], DIVIDES, weights=x["amount_ask"])[::-1]).tolist() + np.histogram(x["price"], DIVIDES, weights=x["amount_bid"])[0].tolist()
            ).reset_index().set_index(["symbol", "timegrp"])
            df_ohlc = pd.concat([df_ohlc, pd.DataFrame({f"price_h{str(int(i)).zfill(2)}"     : dfwk.iloc[:, 0].str[i              ].astype(float) for i in range(DIVIDES + 1)})], axis=1)
            df_ohlc = pd.concat([df_ohlc, pd.DataFrame({f"price_amount_h{int(i_hist)}_{name}": dfwk.iloc[:, 0].str[DIVIDES + 1 + i].astype(float) for i, (name, i_hist) in enumerate(zip((["ask"] * DIVIDES) + (["bid"] * DIVIDES), np.concatenate([np.arange(DIVIDES), np.arange(DIVIDES)])))})], axis=1)
            df_ohlc = df_ohlc.reset_index()
            df_ohlc.columns     = df_ohlc.columns.str.replace("timegrp", "unixtime")
            df_ohlc["unixtime"] = (df_ohlc["unixtime"] + interval)
            df_ohlc["unixtime"] = pd.to_datetime(df_ohlc["unixtime"], unit="s", utc=True)
            df_ohlc["attrs"]    = df_ohlc.loc[:, df_ohlc.columns[~df_ohlc.columns.isin(DB_TO.db_layout["mart_ohlc"])]].apply(lambda x: str({y:z for y, z in x.to_dict().items() if not (z is None or np.isnan(z))}).replace("'", '"'), axis=1)
            if args.update and df_ohlc.shape[0] > 0:
                DB_TO.delete_sql("mart_ohlc", str_where=(
                    f"interval = {interval} and symbol in (" + ",".join(df_ohlc["symbol"].unique().astype(str).tolist()) + ") and " + 
                    f"unixtime in (" + ",".join(df_ohlc["unixtime"].unique().astype(str).tolist())
                ))
                DB_TO.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
                DB_TO.execute_sql()
            raise