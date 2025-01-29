import datetime
import pandas as pd
import polars as pl
import numpy as np
# local package
from kkpsgre.connector import DBConnector
from kkpsgre.util.com import check_type_list
from kklogger import set_logger


__all__ = {
    "get_executions",
    "get_mart_ohlc",
}


EXCHANGES = ["bitflyer", "bybit", "binance"]
LOGGER    = set_logger(__name__)
COLUMNS_BASE = ["symbol", "unixtime", "type", "interval", "sampling_rate", "open", "high", "low", "close", "ave", "volume", "size"]


def get_executions(db_bs: DBConnector, db_bk: DBConnector, exchange: str, date_fr: datetime.datetime, date_to: datetime.datetime, date_sw: datetime.datetime):
    LOGGER.info("START")
    assert isinstance(db_bs, DBConnector)
    assert isinstance(db_bk, DBConnector) or db_bk is None
    if db_bk is not None: assert db_bs.use_polars == db_bk.use_polars
    assert isinstance(exchange, str) and exchange in EXCHANGES
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert isinstance(date_sw, datetime.datetime)
    assert date_fr < date_to
    LOGGER.info(f"exchange: {exchange}, from: {date_fr}, to: {date_to}", color=["BOLD", "GREEN"])
    df_mst  = db_bs.select_sql(f"select * from master_symbol where is_active = true and exchange = '{exchange}';")
    columns = ",".join(["unixtime", "symbol", "side", "price", "size"])
    if exchange in ["bitflyer"]:
        df = db_bs.select_sql( f"SELECT {columns} FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    else:
        if   date_fr >= date_sw:
            df = db_bs.select_sql( f"SELECT {columns} FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        elif date_to <= date_sw:
            df = db_bk.select_sql( f"SELECT {columns} FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
        else:
            df1 = db_bk.select_sql(f"SELECT {columns} FROM {exchange}_executions WHERE unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
            df2 = db_bs.select_sql(f"SELECT {columns} FROM {exchange}_executions WHERE unixtime >= '{date_sw.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
            if db_bs.use_polars:
                df = pl.concat([df2, df1], how="vertical")
            else:
                df = pd.concat([df2, df1], axis=0, sort=False, ignore_index=True)
    if df.shape[0] == 0:
        LOGGER.info("END")
        return df
    if db_bs.use_polars:
        assert df.schema["unixtime"] == pl.Datetime
        df = df.filter(pl.col("side").is_in([0, 1]))
        df = df.sort(["symbol", "unixtime", "price"])
        df = df.join(df_mst, how="left", left_on="symbol", right_on="symbol_id")
        df = df.with_columns([
            pl.col("unixtime").alias("datetime"),
            (pl.col("unixtime").dt.timestamp() / 10e5).alias("unixtime"),
        ])
        boolwk = (
            pl.col("symbol_name").str.starts_with("inverse@") | 
            pl.col("symbol_name").str.starts_with("COIN@")
        )
        df = df.with_columns(pl.when(boolwk).then(pl.col("size")).otherwise(pl.col("price") * pl.col("size")).alias("volume"))
        df = df.with_columns(pl.when(boolwk).then(pl.col("volume") / pl.col("price")).otherwise("size").alias("size"))
    else:
        assert pd.api.types.is_datetime64_any_dtype(df["unixtime"])
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
    assert isinstance(type, int) and type in [0,1,2]
    assert isinstance(interval,      int)
    assert isinstance(sampling_rate, int) and (interval % sampling_rate) == 0
    assert exchanges is None or (isinstance(exchanges, list) and check_type_list(exchanges, str))
    LOGGER.info(f"from: {date_fr}, to: {date_to}", color=["BOLD", "GREEN"])
    symbols = None
    if exchanges is not None:
        df_mst  = db.select_sql(f"select * from master_symbol where exchange in ('" + "','".join(exchanges) +"');")
        if db.use_polars:
            symbols = df_mst["symbol_id"].to_list()
        else:
            symbols = df_mst["symbol_id"].tolist()
    """
    The data is like below.
    unixtime  sr  itvl
    0         60   60  ... -60 ~ -0.0000001
    60        60   60  ...   0 ~ 59.9999999
    ...
    If you want 0s ~ 60s data, you must constrain 0 < unixtime <= 60. "<" and "<=" is important.
    """
    if type == 0:
        sql = (
            f"SELECT symbol, unixtime, type, interval, sampling_rate, open, high, low, close, ave, volume, size, " + 
            ",".join([f"attrs->'{x}'     as {x}"     for x in ['size_ask', 'volume_ask', 'ntx_ask', 'size_bid', 'volume_bid', 'ntx_bid', 'var']]) + ", " + 
            ",".join([f"attrs->'{x}_{y}' as {x}_{y}" for x in ['volume_q0000', 'volume_q0200', 'volume_q0400', 'volume_q0600', 'volume_q0800', 'volume_q1000'] for y in ["ask", "bid"]]) + " " + 
            f"FROM mart_ohlc WHERE "
        )
    else:
        sql = (
            f"SELECT symbol, unixtime, type, interval, sampling_rate, open, high, low, close, ave, volume, size, attrs " + #+ ",".join([f"attrs->'{x}' as {x}" for x in COLUMNS]) + " " + 
            f"FROM mart_ohlc WHERE "
        )
    if symbols is not None:
        sql += "symbol in (" + ",".join([str(x) for x in symbols]) + ") AND "
    sql += (
        f"unixtime >  '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' AND " + 
        f"unixtime <= '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' AND " + 
        f"type = {type} AND interval = {interval} AND sampling_rate = {sampling_rate};"
    )
    df   = db.select_sql(sql)
    if db.use_polars:
        if type != 0:
            df = df.with_columns(pl.col("attrs").struct.unnest())
        df = df.with_columns([
            pl.col("unixtime").alias("datetime"),
            (pl.col("unixtime").dt.timestamp() / 10e5).cast(pl.Int64).alias("unixtime"),
        ])
    else:
        if type != 0:
            dfwk = pd.DataFrame(df["attrs"].tolist(), index=df.index.copy())
            df   = pd.concat([df.iloc[:, :-1], dfwk], axis=1, ignore_index=False, sort=False)
        df["unixtime"] = (df["unixtime"].astype("int64") / 10e8).astype(int)
    # The datetime shoule be interpolated because new symbol's data is missing before it starts
    ndf_tg = np.arange(
        int(date_fr.timestamp()) // sampling_rate * sampling_rate + sampling_rate, # unixtime >  date_fr
        int(date_to.timestamp()) // sampling_rate * sampling_rate + sampling_rate, # unixtime <= date_to
        sampling_rate, dtype=int
    )
    ndf_idxs = df["symbol"].unique().to_numpy() if db.use_polars else df["symbol"].unique()
    ndf_idxs = np.concatenate([np.repeat(ndf_idxs, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idxs.shape[0]).reshape(-1, 1)], axis=-1)
    if db.use_polars:
        df_base = pl.DataFrame(ndf_idxs, schema=["symbol", "unixtime"])
        df      = df_base.join(df, how="left", on=["symbol", "unixtime"]).sort(["symbol", "unixtime"])
        df      = df.with_columns([
            pl.col("type"         ).fill_nan(type         ).fill_null(type         ).cast(pl.Int64),
            pl.col("interval"     ).fill_nan(interval     ).fill_null(interval     ).cast(pl.Int64),
            pl.col("sampling_rate").fill_nan(sampling_rate).fill_null(sampling_rate).cast(pl.Int64),
            (pl.col("unixtime") - sampling_rate).alias("unixtime"),
        ])
    else:
        df_base = pd.DataFrame(ndf_idxs, columns=["symbol", "unixtime"])
        df      = pd.merge(df_base, df, how="left", on=["symbol", "unixtime"])
        df["type"]          = df["type"         ].fillna(type         ).astype(int)
        df["interval"]      = df["interval"     ].fillna(interval     ).astype(int)
        df["sampling_rate"] = df["sampling_rate"].fillna(sampling_rate).astype(int)
        # Ensure the conditions are the same for TX. In TX to OHLC, with sampling_rate=60, the data at 00:00:30 will be rounded to 00:00:00
        df["unixtime"] = df["unixtime"] - df["sampling_rate"]
    LOGGER.info("END")
    return df
