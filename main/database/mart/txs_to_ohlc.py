import datetime, argparse
import pandas as pd
import polars as pl
import numpy as np
# local package
from kktrade.core.mart import get_executions, EXCHANGES
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_price_over_time, ana_distribution_volume_over_price, \
    check_common_input, check_interval, indexes_to_aggregate
from kkpsgre.connector import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import \
    HOST_BS, PORT_BS, USER_BS, PASS_BS, DBNAME_BS, DBTYPE_BS, \
    HOST_BK, PORT_BK, USER_BK, PASS_BK, DBNAME_BK, DBTYPE_BK, \
    HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kklogger import set_logger


LOGGER    = set_logger(__name__)


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--sr",     type=int, help="sampling rate. --sr 60", default=3)
    parser.add_argument("--itvl",   type=int, help="interval.", default=3)
    parser.add_argument("--exmin",  type=int, help="extra minute to read DB. --exmin 30", default=30)
    parser.add_argument("--nq",     type=int, help="n divided quantile. --nq 10", default=10)
    parser.add_argument("--hours",  type=lambda x: [int(y) for y in x.split(",")], default="0")
    parser.add_argument("--switch", type=str_to_datetime, help="--switch 20200101", default=(timenow - datetime.timedelta(days=9)))
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    LOGGER.info(f"args: {args}")
    DB_BS  = DBConnector(HOST_BS, PORT_BS, DBNAME_BS, USER_BS, PASS_BS, dbtype=DBTYPE_BS, max_disp_len=200, is_read_layout=False, use_polars=True)
    DB_TO  = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200, is_read_layout=True,  use_polars=True)
    if (args.switch + datetime.timedelta(days=7)) < args.fr:
        DB_BK = None # No need to connect.
    else:
        DB_BK = DBConnector(HOST_BK, PORT_BK, DBNAME_BK, USER_BK, PASS_BK, dbtype=DBTYPE_BK, max_disp_len=200, is_read_layout=False, use_polars=True)
    list_dates = [args.fr + datetime.timedelta(days=x) + datetime.timedelta(hours=hour) for x in range(0, (args.to - args.fr).days + 1, 1) for hour in args.hours]
    if len(list_dates) == 1: list_dates = list_dates + [args.to, ]
    for i_date in range(1, len(list_dates)):
        date_fr = list_dates[i_date-1]
        date_to = list_dates[i_date  ]
        for exchange in EXCHANGES:
            df = get_executions(DB_BS, DB_BK, exchange, date_fr - datetime.timedelta(minutes=args.exmin), date_to, args.switch)
            if df.shape[0] == 0: continue
            sampling_rate, interval = args.sr, args.itvl
            df_ohlc, df_base = create_ohlc(df.select("symbol", "unixtime", "price"), interval, sampling_rate, date_fr - datetime.timedelta(minutes=30), date_to, index_names=["symbol"], from_tx=True)
            list_df    = []
            list_df.append(ana_size_price(        df[["symbol", "unixtime", "price", "size", "volume", "side"]], interval, sampling_rate, df_base, from_tx=True))
            list_df.append(ana_quantile_tx_volume(df[["symbol", "unixtime", "price", "size", "volume", "side"]], interval, sampling_rate, df_base, from_tx=True, n_div=args.nq))
            # list_df.append(ana_distribution_volume_price_over_time(df[["symbol", "unixtime", "price", "size", "volume"]],         interval, sampling_rate, df_base, from_tx=False, n_div=10))
            # list_df.append(ana_distribution_volume_over_price(     df[["symbol", "unixtime", "price", "side", "volume"        ]], interval, sampling_rate, df_base, from_tx=True, n_div=20))
            for dfwk in list_df:
                df_ohlc = df_ohlc.join(dfwk, how="left", on=df_base.columns)
            df_ohlc = df_ohlc.rename({"timegrp": "unixtime"})
            df_ohlc = df_ohlc.filter(
                (pl.col("unixtime") >= int(date_fr.timestamp() // sampling_rate * sampling_rate)) &
                (pl.col("unixtime") <  int(date_to.timestamp() // sampling_rate * sampling_rate))
            )
            df_ohlc = df_ohlc.with_columns([
                pl.lit(interval).alias("interval"),
                pl.lit(sampling_rate).alias("sampling_rate"),
                pl.lit(0).alias("type"),
                ((pl.col("unixtime") + sampling_rate) * 1000).cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("unixtime"),
            ])
            columns_base = [x for x in df_ohlc.columns if x     in DB_TO.db_layout["mart_ohlc"]]
            columns_oth  = [x for x in df_ohlc.columns if x not in DB_TO.db_layout["mart_ohlc"]]
            df_ohlc = df_ohlc.with_columns(
                pl.struct(columns_oth).map_elements(lambda x: str({k: v for k, v in x.items() if not (v is None or np.isnan(v))}).replace("'", '"'), return_dtype=str).alias("attrs")
            ).select(columns_base + ["attrs"])
            if args.update and df_ohlc.shape[0] > 0:
                DB_TO.delete_sql("mart_ohlc", str_where=(
                    f"interval = {interval} and sampling_rate = {sampling_rate} and type = {df_ohlc['type'][0]} and " + 
                    f"symbol in (" + ",".join(df_ohlc["symbol"].unique().cast(str).to_list()) + ") and " + 
                    f"unixtime >= " + df_ohlc["unixtime"].min().strftime("'%Y-%m-%d %H:%M:%S.%f%z'") + " and " + 
                    f"unixtime <= " + df_ohlc["unixtime"].max().strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
                ))
                DB_TO.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
                DB_TO.execute_sql()
