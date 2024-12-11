import datetime
import pandas as pd
import numpy as np
# local package
from kktrade.core.mart import DICT_MART, COLUMNS_MART
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import check_type_list
from kklogger import set_logger


__all__ = {
    "get_data_for_trainign",
}


LOGGER  = set_logger(__name__)
SYMBOLS = [
    # 6,   # 'spot@BTCUSDT',     'bybit', 'BTC', 'USDT', 't', NULL),
    # 7,   # 'spot@ETHUSDC',     'bybit', 'ETH', 'USDC', 't', NULL),
    # 8,   # 'spot@BTCUSDC',     'bybit', 'BTC', 'USDC', 't', NULL),
    # 9,   # 'spot@ETHUSDT',     'bybit', 'ETH', 'USDT', 't', NULL),
    # 10,  # 'spot@XRPUSDT',     'bybit', 'XRP', 'USDT', 't', NULL),
    11,  # 'linear@BTCUSDT',   'bybit', 'BTC', 'USDT', 't', NULL),
    12,  # 'linear@ETHUSDT',   'bybit', 'ETH', 'USDT', 't', NULL),
    13,  # 'linear@XRPUSDT',   'bybit', 'XRP', 'USDT', 't', NULL),
    14,  # 'inverse@BTCUSD',   'bybit', 'BTC', 'USD', 't', NULL),
    15,  # 'inverse@ETHUSD',   'bybit', 'ETH', 'USD', 't', NULL),
    16,  # 'inverse@XRPUSD',   'bybit', 'XRP', 'USD', 't', NULL),
    # 136, # 'spot@SOLUSDT',     'bybit', 'SOL', 'USDT', 't', NULL),
    # 137, # 'linear@SOLUSDT',   'bybit', 'SOL', 'USDT', 't', NULL),
    # 138, # 'inverse@SOLUSD',   'bybit', 'SOL', 'USD',  't', NULL),
    # 139, # 'spot@BNBUSDT',     'bybit', 'BNB', 'USDT', 't', NULL),
    # 140, # 'linear@BNBUSDT',   'bybit', 'BNB', 'USDT', 't', NULL);
    119, # 'SPOT@BTCUSDT',     'binance', 'BTC', 'USDT', 't', NULL),
    # 120, # 'SPOT@ETHUSDC',     'binance', 'ETH', 'USDC', 't', NULL),
    # 121, # 'SPOT@BTCUSDC',     'binance', 'BTC', 'USDC', 't', NULL),
    122, # 'SPOT@ETHUSDT',     'binance', 'ETH', 'USDT', 't', NULL),
    123, # 'SPOT@XRPUSDT',     'binance', 'XRP', 'USDT', 't', NULL),
    124, # 'USDS@BTCUSDT',     'binance', 'BTC', 'USDT', 't', NULL),
    125, # 'USDS@ETHUSDT',     'binance', 'ETH', 'USDT', 't', NULL),
    126, # 'USDS@XRPUSDT',     'binance', 'XRP', 'USDT', 't', NULL),
    127, # 'COIN@BTCUSD_PERP', 'binance', 'BTC', 'USD', 't', NULL),
    128, # 'COIN@ETHUSD_PERP', 'binance', 'ETH', 'USD', 't', NULL),
    129, # 'COIN@XRPUSD_PERP', 'binance', 'XRP', 'USD', 't', NULL);
    # 130, # 'SPOT@SOLUSDT',     'binance', 'SOL', 'USDT', 't', NULL),
    # 131, # 'SPOT@BNBUSDT',     'binance', 'BNB', 'USDT', 't', NULL),
    # 132, # 'USDS@SOLUSDT',     'binance', 'SOL', 'USDT', 't', NULL),
    # 133, # 'USDS@BNBUSDT',     'binance', 'BNB', 'USDT', 't', NULL),
    # 134, # 'COIN@SOLUSD_PERP', 'binance', 'SOL', 'USD', 't', NULL),
    # 135, # 'COIN@BNBUSD_PERP', 'binance', 'BNB', 'USD', 't', NULL),
]
BASE_INTERVAL   = 2400
PRICE_BASE      = f"ave_{BASE_INTERVAL}"
VOLUME_ASK_BASE = f"volume_ask_{BASE_INTERVAL}"
VOLUME_BID_BASE = f"volume_bid_{BASE_INTERVAL}"


def get_data_for_trainign(
    db: DBConnector, date_fr: datetime.datetime, date_to: datetime.datetime, sbls: list[int], sets_sr_itvl: list[list[int, int]],
) -> pd.DataFrame:
    LOGGER.info("START")
    assert isinstance(db, DBConnector)
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert date_fr < date_to
    assert (isinstance(sbls, tuple) or isinstance(sbls, list)) and check_type_list(sbls, int)
    assert isinstance(sets_sr_itvl, list) and check_type_list(sets_sr_itvl, [list, tuple], int)
    for x in sets_sr_itvl: assert len(x) == 2
    ndf_sets = np.array(sets_sr_itvl)
    assert np.unique(ndf_sets[:, 1]).shape[0] == ndf_sets.shape[0]
    # Create base dataframe
    ndf_sr   = np.sort(np.unique(ndf_sets[:, 0])).astype(int)
    min_sr   = ndf_sr[0]
    ndf_tg   = np.arange(
        int(date_fr.timestamp()) // min_sr * min_sr + min_sr, # unixtime >  date_fr
        int(date_to.timestamp()) // min_sr * min_sr + min_sr, # unixtime <= date_to
        min_sr, dtype=int
    )
    ndf_idxs = np.array(sbls)
    ndf_idxs = np.concatenate([np.repeat(ndf_idxs, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idxs.shape[0]).reshape(-1, 1)], axis=-1)
    df_base  = pd.DataFrame(ndf_idxs, columns=["symbol", "unixtime"])
    for x in ndf_sr: df_base[f"unixtime_sr_{x}"] = (df_base["unixtime"]) // x * x
    # query
    sql = (
        f"SELECT symbol, unixtime, interval, sampling_rate, open, high, low, close, ave, attrs " + #+ ",".join([f"attrs->'{x}' as {x}" for x in COLUMNS]) + " " + 
        f"FROM mart_ohlc WHERE " + 
        f"symbol IN (" + ",".join([str(x) for x in SYMBOLS]) + ") AND type IN (1,2) AND " +
        f"(sampling_rate, interval) IN (" + ",".join([f"({x}, {y})" for x, y in sets_sr_itvl]) + ") AND "
        f"unixtime >  '{(date_fr - datetime.timedelta(seconds=int(ndf_sr.max()))).strftime('%Y-%m-%d %H:%M:%S.%f%z')}' AND " + 
        f"unixtime <= '{ date_to                                                 .strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
    )
    df   = db.select_sql(sql)
    dfwk = pd.DataFrame(df["attrs"].tolist(), index=df.index.copy())
    dfwk = dfwk.loc[:, dfwk.columns.isin(COLUMNS_MART)]
    df   = pd.concat([df.iloc[:, :-1], dfwk], axis=1, ignore_index=False, sort=False)
    df["unixtime"] = (df["unixtime"].astype("int64") / 10e8).astype(int)
    # being relative by each interval
    df_mst = pd.DataFrame(DICT_MART).T
    for name_vs in df_mst.loc[~df_mst["vs"].isna(), "vs"].unique():
        dfwk = df[df_mst.index[df_mst["vs"] == name_vs]].copy() / df[name_vs].values.reshape(-1, 1)
        dfwk.columns = [f"{x}_@rel@" for x in dfwk.columns]
        df   = pd.concat([df, dfwk], axis=1, ignore_index=False, sort=False)
    # join to base dataframe
    for sampling_rate, interval in sets_sr_itvl:
        dfwk    = df.loc[(df["sampling_rate"] == sampling_rate) & (df["interval"] == interval)].copy()
        dfwk.columns = [f"{x}_{interval}" for x in dfwk.columns]
        df_base = pd.merge(df_base, dfwk, how="left", left_on=["symbol", f"unixtime_sr_{sampling_rate}"], right_on=[f"symbol_{interval}", f"unixtime_{interval}"])
    # being relative by basic interval
    df_main = df_base[["symbol", "unixtime"] + df_base.columns[df_base.columns.str.contains("@rel@")].tolist()].copy()
    dictwk = {
        "price"     : PRICE_BASE,
        "volume_ask": VOLUME_ASK_BASE,
        "volume_bid": VOLUME_BID_BASE,
    }
    for _type in ["price", "volume_ask", "volume_bid"]:
        columns = df_mst.index[df_mst["type"] == _type].copy().tolist()
        columns = [f"{x}_{y}" for x in columns for y in ndf_sets[:, 1]]
        df_main = pd.concat([df_main, df_base[columns] / df_base[dictwk[_type]].values.reshape(-1, 1)], axis=1, ignore_index=False, sort=False)
    for itvl in ndf_sets[:, 1]:
        columns = [f"{x}_{itvl}" for x in df_mst.index[df_mst["type"].isna()].tolist()]
        df_main = pd.concat([df_main, df_base[columns]], axis=1, ignore_index=False, sort=False)
    df_main = df_main.set_index(["symbol", "unixtime"])
    df_base = df_base.set_index(["symbol", "unixtime"])
    df_ret  = pd.DataFrame(index=ndf_tg)
    for sbl in sbls:
        dfwk = df_main.loc[sbl].copy()
        dfwk.columns = [f"{x}_s{sbl}" for x in dfwk.columns]
        df_ret = pd.concat([df_ret, dfwk], axis=1, ignore_index=False, sort=False)
    # GT
    df_ret["==="] = False
    for sbl in sbls:
        for _, interval in sets_sr_itvl:
            dfwk = df_base.loc[sbl, [f"ave_{interval}", f"close_{interval}"]].copy()
            dfwk.columns = [f"gt@ave_{interval}_s{sbl}", f"gt@close_{interval}_s{sbl}"]
            df_ret = pd.concat([df_ret, dfwk], axis=1, ignore_index=False, sort=False)
    LOGGER.info("EBD")
    return df_ret

