import datetime
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import check_type, check_type_list
from kklogger import set_logger


__all__ = {
    "get_executions",
    "get_mart_ohlc",
}


EXCHANGES = ["bitflyer", "bybit", "binance"]
LOGGER    = set_logger(__name__)
COLUMNS_MART = [
    'williams_r', 'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid',
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
    if exchange in ["bitflyer"]:
        df = db_bs.select_sql( f"SELECT * FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    else:
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

def get_mart_ohlc(db: DBConnector, date_fr: datetime.datetime, date_to: datetime.datetime, type: int, interval: int, sampling_rate: int, exchanges: str=["bybit", "binance"]):
    LOGGER.info("START")
    assert isinstance(db, DBConnector)
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert date_fr < date_to and date_fr >= datetime.datetime(2018,12,31,0,0,0, tzinfo=datetime.UTC)
    assert isinstance(type, int)     and type in [0,1,2]
    assert isinstance(interval,      int) and (interval % 60) == 0
    assert isinstance(sampling_rate, int) and (interval % sampling_rate) == 0
    assert exchanges is None or (isinstance(exchanges, list) and check_type_list(exchanges, str))
    symbols = None
    if exchanges is not None:
        df_mst  = db.select_sql(f"select * from master_symbol where exchange in ('" + "','".join(exchanges) +"');")
        symbols = df_mst["symbol_id"].tolist()
    """
    The data is like below.
    unixtime  sr  itvl
    0         60   60  ... -60 ~ -0.0000001
    60        60   60  ...   0 ~ 59.9999999
    ...
    If you want 0s ~ 60s data, you must constrain 0 < unixtime <= 60. "<" and "<=" is important.
    """
    sql = (
        f"SELECT symbol, unixtime, type, interval, sampling_rate, open, high, low, close, ave, attrs " + #+ ",".join([f"attrs->'{x}' as {x}" for x in COLUMNS]) + " " + 
        f"FROM mart_ohlc WHERE type = {type} AND interval = {interval} AND sampling_rate = {sampling_rate} AND " + 
        f"unixtime >  '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' AND " + 
        f"unixtime <= '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' "
    )
    if symbols is not None:
        sql += " AND symbol in (" + ",".join([str(x) for x in symbols]) + ") "
    sql += ";"
    df = db.select_sql(sql)
    df = pd.concat([df.iloc[:, :-1], pd.DataFrame(df["attrs"].tolist(), index=df.index.copy())[COLUMNS_MART]], axis=1, ignore_index=False, sort=False)
    df["unixtime"] = (df["unixtime"].astype("int64") / 10e8).astype(int)
    # The datetime shoule be interpolated because new symbol's data is missing before it starts
    ndf_tg = np.arange(
        int(date_fr.timestamp()) // sampling_rate * sampling_rate + sampling_rate, # unixtime >  date_fr
        int(date_to.timestamp()) // sampling_rate * sampling_rate + sampling_rate, # unixtime <= date_to
        sampling_rate, dtype=int
    )
    ndf_idxs = df["symbol"].unique()
    ndf_idxs = np.concatenate([np.repeat(ndf_idxs, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idxs.shape[0]).reshape(-1, 1)], axis=-1)
    df = pd.merge(pd.DataFrame(ndf_idxs, columns=["symbol", "unixtime"]), df, how="left", on=["symbol", "unixtime"])
    df["type"]          = df["type"         ].fillna(type         ).astype(int)
    df["interval"]      = df["interval"     ].fillna(interval     ).astype(int)
    df["sampling_rate"] = df["sampling_rate"].fillna(sampling_rate).astype(int)
    # Ensure the conditions are the same for TX. In TX to OHLC, with sampling_rate=60, the data at 00:00:30 will be rounded to 00:00:00
    df["unixtime"] = df["unixtime"] - df["sampling_rate"]
    LOGGER.info("END")
    return df
