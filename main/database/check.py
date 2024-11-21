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
    'binance_executions'   : ["symbol", "unixtime"],
    'binance_funding_rate' : None,
    'binance_kline'        : None,
    'binance_long_short'   : None,
    'binance_open_interest': None,
    'binance_orderbook'    : ["symbol", "unixtime"], 
    'binance_taker_volume' : None,
    'bitflyer_executions'  : ["symbol", "unixtime"],
    'bitflyer_fundingrate' : None, 
    'bitflyer_orderbook'   : ["symbol", "unixtime"], 
    'bitflyer_ticker'      : ["symbol", "unixtime"], 
    'bybit_executions'     : ["symbol", "unixtime"], 
    'bybit_kline'          : None, 
    'bybit_orderbook'      : ["symbol", "unixtime"],
    'bybit_ticker'         : ["symbol", "unixtime"],
    # 'dukascopy_ohlcv'      : None, 
    # 'economic_calendar'    : None,
}
SYMBOL_ACTIVE = {# symbol_id       symbol_name  exchange    base currency
      0: "20230905", #     0           BTC_JPY  bitflyer     BTC      JPY
      1: "20230905", #     1           XRP_JPY  bitflyer     XRP      JPY
      2: "20230905", #     2           ETH_JPY  bitflyer     ETH      JPY
      5: "20230905", #     5        FX_BTC_JPY  bitflyer  BTC_FX      JPY
      6: "20221110", #     6      spot@BTCUSDT     bybit     BTC     USDT
      7: "20221110", #     7      spot@ETHUSDC     bybit     ETH     USDC
      8: "20221110", #     8      spot@BTCUSDC     bybit     BTC     USDC
      9: "20221110", #     9      spot@ETHUSDT     bybit     ETH     USDT
     10: "20221110", #    10      spot@XRPUSDT     bybit     XRP     USDT
     11: "20200325", #    11    linear@BTCUSDT     bybit     BTC     USDT
     12: "20201021", #    12    linear@ETHUSDT     bybit     ETH     USDT
     13: "20210513", #    13    linear@XRPUSDT     bybit     XRP     USDT
     14: "20191001", #    14    inverse@BTCUSD     bybit     BTC      USD
     15: "20191001", #    15    inverse@ETHUSD     bybit     ETH      USD
     16: "20191001", #    16    inverse@XRPUSD     bybit     XRP      USD
    119: "20190101", #   119      SPOT@BTCUSDT   binance     BTC     USDT
    120: "20190101", #   120      SPOT@ETHUSDC   binance     ETH     USDC
    121: "20190101", #   121      SPOT@BTCUSDC   binance     BTC     USDC
    122: "20190101", #   122      SPOT@ETHUSDT   binance     ETH     USDT
    123: "20190101", #   123      SPOT@XRPUSDT   binance     XRP     USDT
    124: "20190910", #   124      USDS@BTCUSDT   binance     BTC     USDT
    125: "20191130", #   125      USDS@ETHUSDT   binance     ETH     USDT
    126: "20200107", #   126      USDS@XRPUSDT   binance     XRP     USDT
    127: "20200801", #   127  COIN@BTCUSD_PERP   binance     BTC      USD
    128: "20200801", #   128  COIN@ETHUSD_PERP   binance     ETH      USD
    129: "20200901", #   129  COIN@XRPUSD_PERP   binance     XRP      USD
    130: "20200801", #   130      SPOT@SOLUSDT   binance     SOL     USDT
    131: "20190101", #   131      SPOT@BNBUSDT   binance     BNB     USDT
    132: "20200901", #   132      USDS@SOLUSDT   binance     SOL     USDT
    133: "20200201", #   133      USDS@BNBUSDT   binance     BNB     USDT
    134: "20210901", #   134  COIN@SOLUSD_PERP   binance     SOL      USD
    135: "20200801", #   135  COIN@BNBUSD_PERP   binance     BNB      USD
    136: "20221110", #   136      spot@SOLUSDT     bybit     SOL     USDT
    137: "20210629", #   137    linear@SOLUSDT     bybit     SOL     USDT
    138: "20220325", #   138    inverse@SOLUSD     bybit     SOL      USD
    139: "20221110", #   139      spot@BNBUSDT     bybit     BNB     USDT
    140: "20210629", #   140    linear@BNBUSDT     bybit     BNB     USDT
}


def check_first_date(db_bk: DBConnector, df_mst: pd.DataFrame):
    ndf = df_mst.loc[df_mst["exchange"] == "binance"]["symbol_id"].unique()
    dictwk = {}
    for days in range(365 * 4):
        date = datetime.datetime(2019,1,1, tzinfo=datetime.UTC) + datetime.timedelta(days=days)
        df = db_bk.select_sql(f"SELECT symbol, _id FROM binance_executions WHERE unixtime >= '{date.strftime('%Y-%m-%d 00:00:00.000000+0000')}' and unixtime < '{date.strftime('%Y-%m-%d 00:05:00.000000+0000')}';")
        dictwk[date] = np.isin(ndf, df["symbol"].unique())
    dfwk = pd.DataFrame(dictwk).T
    dfwk.columns = ndf
    for x in dfwk.columns:
        print(x, dfwk.index[dfwk[x]][0])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", required=True)
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", required=True)
    parser.add_argument("--sr", type=int, help="--sr 600", default=600)
    args    = parser.parse_args()
    LOGGER.info(f"args: {args}")
    DB_BS    = DBConnector(HOST_BS, PORT_BS, DBNAME_BS, USER_BS, PASS_BS, dbtype=DBTYPE_BS, max_disp_len=200)
    DB_BK    = DBConnector(HOST_BK, PORT_BK, DBNAME_BK, USER_BK, PASS_BK, dbtype=DBTYPE_BK, max_disp_len=200, is_read_layout=False)
    DB_TO    = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df_mst   = DB_BS.select_sql("SELECT * FROM master_symbol WHERE is_active = true;")
    ndf_mst  = df_mst["symbol_id"].values.copy()
    ndf_time = np.arange(int(args.fr.timestamp()) // args.sr * args.sr, int(args.to.timestamp()) // args.sr * args.sr, args.sr, dtype=int)
    df_base  = pd.DataFrame(np.stack([ndf_mst.repeat(ndf_time.shape[0]), np.tile(ndf_time, ndf_mst.shape[0])]).T, columns=["symbol", "timestamp"])
    df_base  = pd.merge(df_base, df_mst, how="left", left_on="symbol", right_on="symbol_id")
    df_base  = pd.merge(df_base, pd.DataFrame([[x, str_to_datetime(y)] for x, y in SYMBOL_ACTIVE.items()], columns=["symbol", "active_date"]), how="left", on="symbol")
    # Base
    for tblname, pkeys in TABLES.items():
        exchange, table_type = tblname.split("_")[0], "_".join(tblname.split("_")[1:])
        if pkeys is None: pkeys = DB_BS.db_constraint[tblname]
        LOGGER.info(f"exchange: {exchange}, table_type: {table_type}, table name: {tblname}, pkeys: {pkeys}", color=["BOLD", "GREEN"])
        assert "unixtime" in pkeys
        df = DB_BS.select_sql(
            f"SELECT {','.join([x for x in pkeys if x != 'unixtime'])}, " + 
            f"FLOOR(EXTRACT(EPOCH FROM unixtime) / {args.sr}) * {args.sr} AS timestamp, COUNT(*) AS n_records FROM {tblname} " + 
            f"WHERE unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' GROUP BY {','.join([x for x in pkeys if x != 'unixtime'])}, timestamp;"
        )
        df_check = pd.merge(df_base, df, how="left", on=["symbol", "timestamp"])
        df_check = df_check.loc[df_check["exchange"] == exchange]
        df_check["unixtime"] = pd.to_datetime(df_check["timestamp"], unit="s", utc=True)
        if tblname in ["binance_funding_rate"]:
            df_check = df_check.loc[df_check["unixtime"].dt.hour.isin([0, 8, 16])]
            df_check = df_check.loc[df_check["symbol_name"].str.split("@").str[0] != "SPOT"]
        elif table_type in ["binance_long_short", "binance_open_interest", "binance_taker_volume"]:
            df_check = df_check.loc[df_check["symbol_name"].str.split("@").str[0] != "SPOT"]
        elif tblname in ["bitflyer_fundingrate"]:
            df_check = df_check.loc[df_check["symbol_name"] == "FX_BTC_JPY"]
        if df_check.shape[0] == 0:
            LOGGER.warning("Nothing data.")
        else:
            df_check = df_check.loc[df_check["unixtime"] >= df_check["active_date"]]
            for ndfwk in df_check.loc[df_check["n_records"].isna(), pkeys].values:
                LOGGER.warning(", ".join([f"{x}: {y}" for x, y in zip(pkeys, ndfwk)]))

    # df = DB_BK.select_sql(f"SELECT symbol, unixtime FROM {tblname} WHERE unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';")
    # df["timestamp"] = (df["unixtime"].astype(int) // 10e8).astype(int)
    # df["timestamp"] = df["timestamp"] // args.sr * args.sr
    # df_check = pd.merge(df_base.loc[df_base["exchange"] == ""], df.groupby(["symbol", "timestamp"]).size().reset_index(), how="left", on="")
