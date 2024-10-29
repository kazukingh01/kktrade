import argparse, datetime
import pandas as pd
# local package
from kktrade.config.migration import HOST_FR, PORT_FR, DBNAME_FR, USER_FR, PASS_FR, DBTYPE_FR, HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, DBTYPE_TO
from kkpsgre.psgre import DBConnector
from kkpsgre.migration import migrate
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


def convdatetime(x: str):
    if   len(x) == 8:
        return datetime.datetime.strptime(x + "000000 +0000", "%Y%m%d%H%M%S %z")
    elif len(x) == 12:
        return datetime.datetime.strptime(x + " +0000", "%Y%m%d%H%M%S %z")
    else:
        raise Exception("FORMAT ex) 20240101090000")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=convdatetime, help="--fr 20200101", required=True)
    parser.add_argument("--to", type=convdatetime, help="--to 20200101", required=True)
    parser.add_argument("--hs", type=lambda x: [int(y) for y in x.split(",")], default="0")
    parser.add_argument("--tbl", type=str, help="table name", required=True)
    parser.add_argument("--num", type=int, default=10000)
    parser.add_argument("--isnoerr", action='store_true', default=False)
    parser.add_argument("--update",  action='store_true', default=False)
    args = parser.parse_args()
    print(args)
    DB_from   = DBConnector(HOST_FR, PORT_FR, DBNAME_FR, USER_FR, PASS_FR, dbtype=DBTYPE_FR, max_disp_len=200)
    DB_to     = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    list_date = [args.fr + datetime.timedelta(days=x) + datetime.timedelta(hours=hours) for x in range((args.to - args.fr).days) for hours in args.hs]
    for i_date, date in enumerate(list_date[:-1]):
        LOGGER.info(f"date: {date}", color=["BOLD", "GREEN"])
        date_fr = date
        date_to = list_date[i_date + 1]
        df_from, df_exist, df_insert = migrate(
            DB_from, DB_to, args.tbl, f"unixtime >= '{date_fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{date_to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'",
            pkeys=TABLES[args.tbl], n_split=args.num, is_no_error_when_different=args.isnoerr, is_delete=False, is_update=args.update
        )
