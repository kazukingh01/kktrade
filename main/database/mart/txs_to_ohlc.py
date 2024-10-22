import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kktrade.core.mart import get_executions, EXCHANGES
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_over_time, ana_distribution_volume_over_price
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import \
    HOST_BS, PORT_BS, USER_BS, PASS_BS, DBNAME_BS, DBTYPE_BS, \
    HOST_BK, PORT_BK, USER_BK, PASS_BK, DBNAME_BK, DBTYPE_BK, \
    HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kkpsgre.util.logger import set_logger


LOGGER    = set_logger(__name__)
INTERVALS = [60, ]
DIVIDES   = 10


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--switch", type=str_to_datetime, help="--switch 20200101", default=(timenow - datetime.timedelta(days=7)))
    parser.add_argument("--hours",  type=lambda x: [int(y) for y in x.split(",")], default="0")
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    DB_BS  = DBConnector(HOST_BS, PORT_BS, DBNAME_BS, USER_BS, PASS_BS, dbtype=DBTYPE_BS, max_disp_len=200)
    DB_BK  = DBConnector(HOST_BK, PORT_BK, DBNAME_BK, USER_BK, PASS_BK, dbtype=DBTYPE_BK, max_disp_len=200)
    DB_TO  = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    list_dates = [args.fr + datetime.timedelta(days=x) + datetime.timedelta(hours=hour) for x in range(0, (args.to - args.fr).days + 1, 1) for hour in args.hours]
    if len(list_dates) == 1: list_dates = list_dates + [args.to, ]
    for i_date in range(1, len(list_dates)):
        date_fr = list_dates[i_date-1]
        date_to = list_dates[i_date  ]
        for exchange in EXCHANGES:
            LOGGER.info(f"exchange: {exchange}, from: {date_fr}, to: {date_to}")
            df = get_executions(DB_BS, DB_BK, exchange, date_fr - datetime.timedelta(minutes=30), date_to, args.switch)
            if df.shape[0] == 0: continue
            for interval in INTERVALS:
                assert interval % DIVIDES == 0
                df_ohlc    = create_ohlc(df, "unixtime", interval, interval, date_fr - datetime.timedelta(minutes=30), date_to, index_names=["symbol"], from_tx=True)
                index_base = df_ohlc.index.copy()
                list_df    = []
                list_df.append(ana_size_price(                    df, "unixtime", interval, interval, index_base, from_tx=True))
                list_df.append(ana_quantile_tx_volume(            df, "unixtime", interval, interval, index_base, from_tx=True, n_div=20))
                list_df.append(ana_distribution_volume_over_time( df, "unixtime", interval, interval, index_base, from_tx=True, n_div=10))
                list_df.append(ana_distribution_volume_over_price(df, "unixtime", interval, interval, index_base, from_tx=True, n_div=20))
                df_ohlc = pd.concat([df_ohlc, ] + list_df, axis=1, ignore_index=False, sort=False)
                df_ohlc = df_ohlc.loc[:, ~df_ohlc.columns.duplicated()]
                df_ohlc = df_ohlc.reset_index()
                df_ohlc.columns     = df_ohlc.columns.str.replace("timegrp", "unixtime")
                df_ohlc = df_ohlc.loc[(df_ohlc["unixtime"] >= int(date_fr.timestamp())) & (df_ohlc["unixtime"] < int(date_to.timestamp()))]
                df_ohlc["type"]     = 0
                df_ohlc["unixtime"] = (df_ohlc["unixtime"] + interval)
                df_ohlc["unixtime"] = pd.to_datetime(df_ohlc["unixtime"], unit="s", utc=True)
                df_ohlc["attrs"]    = df_ohlc.loc[:, df_ohlc.columns[~df_ohlc.columns.isin(DB_TO.db_layout["mart_ohlc"])]].apply(lambda x: str({y:z for y, z in x.to_dict().items() if not (z is None or np.isnan(z))}).replace("'", '"'), axis=1)
                if args.update and df_ohlc.shape[0] > 0:
                    DB_TO.delete_sql("mart_ohlc", str_where=(
                        f"interval = {interval} and type = {df_ohlc['type'].iloc[0]} and symbol in (" + ",".join(df_ohlc["symbol"].unique().astype(str).tolist()) + ") and " + 
                        f"unixtime >= " + df_ohlc["unixtime"].min().strftime("'%Y-%m-%d %H:%M:%S.%f%z'") + " and " + 
                        f"unixtime <= " + df_ohlc["unixtime"].max().strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
                    ))
                    DB_TO.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
                    DB_TO.execute_sql()
