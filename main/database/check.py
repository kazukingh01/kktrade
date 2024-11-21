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


LOGGER = set_logger(__name__)
TABLES = {
    'binance_executions'   : ["symbol", "id"],
    'binance_funding_rate' : None,
    'binance_kline'        : None,
    'binance_long_short'   : None,
    'binance_open_interest': None,
    'binance_orderbook'    : ["symbol", "unixtime"], 
    'binance_taker_volume' : None,
    'bitflyer_executions'  : ["symbol", "id"],
    'bitflyer_fundingrate' : None, 
    'bitflyer_orderbook'   : ["symbol", "unixtime"], 
    'bitflyer_ticker'      : ["symbol", "unixtime"], 
    'bybit_executions'     : ["symbol", "id"], 
    'bybit_kline'          : None, 
    'bybit_orderbook'      : ["symbol", "unixtime"],
    'bybit_ticker'         : ["symbol", "unixtime"],
    'dukascopy_ohlcv'      : None, 
    'dukascopy_ticks'      : ["symbol", "unixtime"], 
    'economic_calendar'    : None,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", required=True)
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", required=True)
    parser.add_argument("--sr", type=str_to_datetime, help="--sr 600", default=600)
    args   = parser.parse_args()
    LOGGER.info(f"args: {args}")
    DB_BS  = DBConnector(HOST_BS, PORT_BS, DBNAME_BS, USER_BS, PASS_BS, dbtype=DBTYPE_BS, max_disp_len=200)
    DB_BK  = DBConnector(HOST_BK, PORT_BK, DBNAME_BK, USER_BK, PASS_BK, dbtype=DBTYPE_BK, max_disp_len=200)
    DB_TO  = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    # Base
    for tblname in TABLES.keys():
        LOGGER.info(f"table name: {tblname}", color=["BOLD", "GREEN"])
        df = DB_BS.select_sql(
            f"SELECT TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM unixtime) / {args.sr}) * {args.sr}) AS time_slot, COUNT(*) AS record_count FROM {tblname} " + 
            f"WHERE unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' GROUP BY time_slot;"
        )
        print(df)
