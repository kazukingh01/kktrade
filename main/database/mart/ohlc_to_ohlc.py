import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kktrade.core.mart import get_mart_ohlc
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_price_over_time, ana_distribution_volume_over_price, ana_rank_corr_index
from kkpsgre.psgre import DBConnector
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
    """
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--frsr", type=int, help="sampling rate. --frsr 60",  default=60)
    parser.add_argument("--tosr", type=int, help="sampling rate. --tosr 120", default=120)
    parser.add_argument("--type",   type=int)
    parser.add_argument("--itvls",  type=lambda x: [int(y) for y in x.split(",")], help="--itvls 120,480,2400,14400,86400", default="120,480,2400,14400")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.type in [1, 2]
    assert args.frsr % 60 == 0
    assert args.tosr % args.frsr == 0
    for x in args.itvls: assert x % args.tosr == 0
    LOGGER.info(f"args: {args}")
    DB = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df = get_mart_ohlc(
        DB, args.fr - datetime.timedelta(seconds=(max(args.itvls) + args.tosr + args.frsr)), args.to, 
        type=(0 if args.type == 1 else 1), interval=args.frsr, sampling_rate=args.frsr
    )
    for interval in args.itvls:
        LOGGER.info(f"processing sampling rate: {args.tosr}, interval: {interval}", color=["BOLD", "GREEN"])
        sampling_rate = args.tosr
        unixtime_name = "unixtime"
        from_tx = False
        df_ohlc    = create_ohlc(df[['symbol', 'unixtime', 'type', 'interval', 'open', 'high', 'low', 'close']], "unixtime", interval, args.tosr, args.fr, args.to, index_names=["symbol"], from_tx=False)
        index_base = df_ohlc.index.copy()
        list_df    = []
        list_df.append(ana_size_price(                         df[['symbol', 'unixtime', 'interval', 'ave', 'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid']],                     "unixtime", interval, args.tosr, index_base, from_tx=False))
        list_df.append(ana_quantile_tx_volume(                 df[["symbol", "unixtime", "interval", "ntx_ask", "ntx_bid"] + df.columns[df.columns.str.find("volume_q") == 0].tolist()],                     "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=20))
        list_df.append(ana_distribution_volume_price_over_time(df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(volume|size|ave|var)_p[0-9]")]].tolist()], "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=10))
        list_df.append(ana_distribution_volume_over_price(     df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(price|volume_price)_h[0-9]" )]].tolist()], "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=20))
        if args.type == 2:
            list_df.append(ana_rank_corr_index(                df[['symbol', 'unixtime', 'interval', 'ave']],                                                                                                "unixtime", interval, args.tosr, index_base))
        df_ohlc = pd.concat([df_ohlc, ] + list_df, axis=1, ignore_index=False, sort=False)
        df_ohlc = df_ohlc.loc[:, ~df_ohlc.columns.duplicated()]
        df_ohlc = df_ohlc.reset_index()
        df_ohlc.columns     = df_ohlc.columns.str.replace("timegrp", "unixtime")
        df_ohlc = df_ohlc.loc[(df_ohlc["unixtime"] >= int(args.fr.timestamp())) & (df_ohlc["unixtime"] < int(args.to.timestamp()))]
        df_ohlc["type"]     = args.type
        df_ohlc["unixtime"] = (df_ohlc["unixtime"] + args.tosr)
        df_ohlc["unixtime"] = pd.to_datetime(df_ohlc["unixtime"], unit="s", utc=True)
        df_ohlc["attrs"]    = df_ohlc.loc[:, df_ohlc.columns[~df_ohlc.columns.isin(DB.db_layout["mart_ohlc"])]].apply(lambda x: str({y:z for y, z in x.to_dict().items() if not (z is None or np.isnan(z))}).replace("'", '"'), axis=1)
        if args.update and df_ohlc.shape[0] > 0:
            DB.delete_sql("mart_ohlc", str_where=(
                f"interval = {interval} and sampling_rate = {args.tosr} and type = {df_ohlc['type'].iloc[0]} and symbol in (" + ",".join(df_ohlc["symbol"].unique().astype(str).tolist()) + ") and " + 
                f"unixtime >= " + df_ohlc["unixtime"].min().strftime("'%Y-%m-%d %H:%M:%S.%f%z'") + " and " + 
                f"unixtime <= " + df_ohlc["unixtime"].max().strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
            ))
            DB.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
            DB.execute_sql()
