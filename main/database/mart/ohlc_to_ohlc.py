import datetime, argparse, re
import polars as pl
import numpy as np
# local package
from kktrade.core.mart import get_mart_ohlc
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_price_over_time, ana_distribution_volume_over_price, ana_other_factor, \
    check_common_input, check_interval, indexes_to_aggregate, calc_ave_var
from kkpsgre.connector import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kklogger import set_logger


LOGGER  = set_logger(__name__)


if __name__ == "__main__":
    """
    tyep:
        0: from tx. 60s
        1: from 60s ( means from 60s )
        2: from other
    
    tx -> 60s
        buffer         2 minute ago                                                          now
        --------------------|                                                                 |  
        00:09:40   00:10:00   00:10:20   00:10:40   00:11:00   00:11:20   00:11:40   00:12:00   00:12:20   00:12:40
            v          v          v          v          v          v          v          v          v
        00:09:00   00:10:00   00:10:00   00:10:00   00:11:00   00:11:00   00:11:00   00:12:00   00:12:00
                                                                                         x          x     No need.
                                        ( Add 60 s )
        00:10:00   00:11:00   00:11:00   00:11:00   00:12:00   00:12:00   00:12:00

    60s -> 120s
                   00:11:00   00:11:00   00:11:00   00:12:00   00:12:00   00:12:00
                                        ( Sub 60 s )
                   00:10:00   00:10:00   00:10:00   00:11:00   00:11:00   00:11:00
                       v          v          v          v          v          v
                   00:10:00   00:10:00   00:10:00   00:10:00   00:10:00   00:10:00
                                        ( Add 120 s )
                   00:12:00   00:12:00   00:12:00   00:12:00   00:12:00   00:12:00

    """
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--frsr",   type=int, help="from sampling rate. --frsr 60",  default=60)
    parser.add_argument("--type",   type=int)
    parser.add_argument("--tosr",   type=lambda x: [int(y) for y in x.split(",")], help="to sampling rate. --tosr 120,240", default="120,240")
    parser.add_argument("--itvls",  type=lambda x: [int(y) for y in x.split(",")], help="--itvls 120,480,2400,14400,86400", default="120,480,2400,14400")
    parser.add_argument("--ndiv",   type=int, help="n divide. --ndiv 10", default=10)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.type in [1, 2]
    assert len(args.tosr) == len(args.itvls)
    for x in args.tosr:  assert x % args.frsr == 0
    for x, y in zip(args.tosr, args.itvls): assert y % x == 0
    LOGGER.info(f"args: {args}")
    DB = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200, use_polars=True)
    df = get_mart_ohlc(
        DB, args.fr - datetime.timedelta(seconds=(max(args.itvls) + max(args.tosr) + args.frsr)), args.to, 
        type=(0 if args.type == 1 else 1), interval=args.frsr, sampling_rate=args.frsr
    )
    for sampling_rate, interval in zip(args.tosr, args.itvls):
        LOGGER.info(f"processing sampling rate: {sampling_rate}, interval: {interval}", color=["BOLD", "GREEN"])
        df_ohlc, df_base = create_ohlc(
            df.select(['symbol', 'unixtime', 'sampling_rate', 'interval', 'open', 'high', 'low', 'close']),
            interval, sampling_rate, args.fr, args.to, index_names=["symbol"], from_tx=False
        )
        list_df    = []
        list_df.append(ana_size_price(
            df[[x for x in df.columns if x in [
                'symbol', 'unixtime', 'sampling_rate', 'interval', 
                'size_ask', 'volume_ask', 'ntx_ask', 'size_bid', 'volume_bid', 'ntx_bid',
                'size', 'volume', 'ave', 'var', "skewness", "kurtosis",
            ]]], interval, sampling_rate, df_base, from_tx=False
        ))
        list_df.append(ana_quantile_tx_volume(
            df[['symbol', 'unixtime', 'sampling_rate', 'interval', 'ntx_ask', 'ntx_bid'] + [x for x in df.columns if x.startswith("volume_q")]],
            interval, sampling_rate, df_base, from_tx=False, n_div=args.ndiv
        ))
        list_df.append(ana_distribution_volume_price_over_time(
            df[
                ['symbol', 'unixtime', 'sampling_rate', 'interval', 'ave', 'var', 'size_ask', 'volume_ask', 'size_bid', 'volume_bid', 'ntx_ask', 'ntx_bid'] + 
                [x for x in df.columns if len(re.findall(r"volume_p[0-9]+", x)) > 0] + 
                [x for x in df.columns if len(re.findall(  r"size_p[0-9]+", x)) > 0] + 
                [x for x in df.columns if len(re.findall(   r"ave_p[0-9]+", x)) > 0] + 
                [x for x in df.columns if len(re.findall(   r"var_p[0-9]+", x)) > 0] + 
                [x for x in df.columns if len(re.findall( r"bband_p[0-9]+", x)) > 0]
            ], interval, sampling_rate, df_base, n_div=args.ndiv
        ))
        list_df.append(ana_distribution_volume_over_price(
            df[
                ["symbol", "unixtime", 'sampling_rate', 'interval', "ave", 'volume_ask', 'volume_bid'] + 
                [x for x in df.columns if len(re.findall(r"volume_price_h[0-9]+", x)) > 0] + 
                [x for x in df.columns if x in ["price_h0000", "price_h1000"]]
            ],
            interval, sampling_rate, df_base, from_small_sr=(True if args.type == 1 else False), n_div=args.ndiv
        ))
        list_df.append(ana_other_factor(
            df[["symbol", "unixtime", 'sampling_rate', 'interval', "open", "high", "low", "close", "ave", 'size_ask', 'volume_ask', 'size_bid', 'volume_bid']],
            interval, sampling_rate, df_base
        ))
        for dfwk in list_df:
            df_ohlc = df_ohlc.join(dfwk, how="left", on=df_base.columns)
        df_ohlc = df_ohlc.rename({"timegrp": "unixtime"})
        df_ohlc = df_ohlc.filter(
            (pl.col("unixtime") >= int(args.fr.timestamp() // sampling_rate * sampling_rate)) &
            (pl.col("unixtime") <  int(args.to.timestamp() // sampling_rate * sampling_rate))
        )
        df_ohlc = df_ohlc.with_columns([
            pl.lit(interval).alias("interval"),
            pl.lit(sampling_rate).alias("sampling_rate"),
            pl.lit(args.type).alias("type"),
            ((pl.col("unixtime") + sampling_rate) * 1000).cast(pl.Datetime("ms")).dt.replace_time_zone("UTC").alias("unixtime"),
        ])
        df_ohlc = df_ohlc.with_columns([
            pl.col(x).replace(float("inf"), None).replace(float("-inf"), None).alias(x) for x, y in df_ohlc.schema.items() if y in [pl.Float32, pl.Float64]
        ])
        columns_base = [x for x in df_ohlc.columns if x     in DB.db_layout["mart_ohlc"]]
        columns_oth  = [x for x in df_ohlc.columns if x not in DB.db_layout["mart_ohlc"]]
        df_sql = df_ohlc.with_columns(
            pl.struct(columns_oth).map_elements(lambda x: str({k: v for k, v in x.items() if not (v is None or np.isnan(v))}).replace("'", '"'), return_dtype=str).alias("attrs")
        ).select(columns_base + ["attrs"])
        if args.update and df_sql.shape[0] > 0:
            DB.delete_sql("mart_ohlc", str_where=(
                f"interval = {interval} and sampling_rate = {sampling_rate} and type = {df_sql['type'][0]} and " + 
                f"symbol in (" + ",".join(df_sql["symbol"].unique().cast(str).to_list()) + ") and " + 
                f"unixtime >= " + df_sql["unixtime"].min().strftime("'%Y-%m-%d %H:%M:%S.%f%z'") + " and " + 
                f"unixtime <= " + df_sql["unixtime"].max().strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
            ))
            DB.insert_from_df(df_sql, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
            DB.execute_sql()
