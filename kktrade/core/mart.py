import datetime
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import check_type
from kkpsgre.util.logger import set_logger


__all__ = {
    "get_executions"
}


EXCHANGES = ["bitflyer", "bybit", "binance"]
LOGGER    = set_logger(__name__)


def get_executions(db_bs: DBConnector, db_bk: DBConnector, exchange: str, date_fr: datetime.datetime, date_to: datetime.datetime, date_sw: datetime.datetime):
    LOGGER.info("START")
    assert isinstance(db_bs, DBConnector)
    assert isinstance(db_bk, DBConnector)
    assert isinstance(exchange, str) and exchange in EXCHANGES
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert isinstance(date_sw, datetime.datetime)
    assert date_fr < date_to
    df_mst = db_bs.select_sql(f"select * from master_symbol where is_active = true and exchange = '{exchange}';")
    if   date_fr >= date_sw:
        df = db_bs.select_sql( f"SELECT * FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    elif date_to <= date_sw:
        df = db_bk.select_sql( f"SELECT * FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    else:
        df1 = db_bk.select_sql(f"SELECT * FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        df2 = db_bs.select_sql(f"SELECT * FROM {exchange}_executions WHERE unixtime >= '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        df  = pd.concat([df2, df1], axis=0, sort=False, ignore_index=True)
    if df.shape[0] == 0:
        LOGGER.info("END")
        return df
    assert check_type(df["unixtime"].dtype, [np.dtypes.DateTime64DType, pd.core.dtypes.dtypes.DatetimeTZDtype])
    df = df.loc[df["side"].isin([0, 1])].reset_index(drop=True)
    df = df.sort_values(["symbol", "unixtime", "price"]).reset_index(drop=True)
    df = pd.merge(df, df_mst, how="left", left_on="symbol", right_on="symbol_id")
    df["datetime"] = df["unixtime"].copy()
    df["unixtime"] = df["unixtime"].astype("int64") / 10e8
    # Inverse type's size shows USD.
    boolwk = ((df["symbol_name"].str.find("inverse@") == 0) | (df["symbol_name"].str.find("COIN@") == 0))
    df["volume"] = (df["price"] * df["size"])
    df.loc[boolwk, "volume"] = df.loc[boolwk, "size"]
    df.loc[boolwk, "size"  ] = (df.loc[boolwk, "volume"] / df.loc[boolwk, "price"]) # Define all size as volume / price
    LOGGER.info("END")
    return df