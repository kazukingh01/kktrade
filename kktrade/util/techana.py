import datetime
import pandas as pd
import numpy as np
# local package
from kklogger import set_logger
from kkpsgre.util.com import check_type_list
from kktrade.util.math import NonLinearXY


__all__ = [
    "create_ohlc",
    "ana_size_price",
    "ana_quantile_tx_volume",
    "ana_distribution_volume_over_time",
    "ana_distribution_volume_over_price",
]


LOGGER = set_logger(__name__)


def check_common_input(
    df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, 
    date_fr: datetime.datetime=None, date_to: datetime.datetime=None, from_tx: bool=None,
    index_names: str | list[str]=None, 
):
    LOGGER.info("STRAT")
    assert isinstance(df, pd.DataFrame)
    assert isinstance(unixtime_name, str) and unixtime_name != "timegrp"
    assert not ("timegrp" in df.columns)
    assert unixtime_name in df.columns and df[unixtime_name].dtype in [int, float] # !! NOT check_type(df[unixtime_name].dtype, [int, float]) !!
    assert (df[unixtime_name] >= datetime.datetime(2000,1,1).timestamp()).sum() == (df[unixtime_name] <= datetime.datetime(2099,1,1).timestamp()).sum() == df.shape[0]
    assert isinstance(interval,      int) and interval      >= 1
    assert isinstance(sampling_rate, int) and sampling_rate >= 1
    assert interval % sampling_rate == 0
    if date_fr is not None:
        assert isinstance(date_fr, datetime.datetime)
        assert isinstance(date_to, datetime.datetime)
        assert date_fr <= date_to
    if from_tx is not None:
        assert isinstance(from_tx, bool)
    if index_names is not None:
        if isinstance(index_names, str): index_names = [index_names, ]
        assert isinstance(index_names, list) and check_type_list(index_names, str)
    df            = df.copy()
    df["timegrp"] = (df[unixtime_name] // sampling_rate * sampling_rate).astype(int)
    LOGGER.info("END")
    return df

def check_interval(df: pd.DataFrame, unixtime_name: str, sampling_rate: int, index_names: str | list[str]=[]):
    LOGGER.info("STRAT")
    assert isinstance(unixtime_name, str)
    assert unixtime_name in df.columns and df[unixtime_name].dtype == int
    assert "interval" in df.columns and df["interval"].dtype == int
    if index_names is not None:
        if isinstance(index_names, str): index_names = [index_names, ]
        assert isinstance(index_names, list) and check_type_list(index_names, str)
        if "timegrp" in index_names:
            index_names = [x for x in index_names if x != "timegrp"]
    else:
        index_names = []
    assert isinstance(sampling_rate, int)
    assert (df["interval"] <= sampling_rate).sum() == df.shape[0] and df["interval"].unique().shape[0] == 1
    for x in index_names: assert x in df.columns
    df = df[index_names + [unixtime_name, "interval"]].copy().sort_values(index_names + [unixtime_name]).reset_index(drop=True)
    if len(index_names) > 0:
        df["__tmp"] = (df[unixtime_name] + df["interval"])
        df["__tmp"] = df.groupby(index_names)["__tmp"].shift(1)
    else:
        df["__tmp"] = (df[unixtime_name] + df["interval"]).shift(1)
    df = df.loc[~df["__tmp"].isna()]
    assert (df["__tmp"] == df[unixtime_name]).sum() == df.shape[0]
    assert sampling_rate % df["interval"].iloc[0] == 0
    LOGGER.info("END")

def indexes_to_aggregate(index_base: pd.Index | pd.MultiIndex, interval: int, sampling_rate: int):
    LOGGER.info("STRAT")
    assert type(index_base) in [pd.Index, pd.MultiIndex]
    assert isinstance(interval,      int)
    assert isinstance(sampling_rate, int)
    assert interval % sampling_rate == 0
    if type(index_base) == pd.Index:
        assert index_base.name == "timegrp"
        ndf_tg      = index_base.values.copy()
        index_names = [index_base.name, ]
    else:
        assert type(index_base) == pd.MultiIndex
        assert index_base.names[-1] == "timegrp"
        ndf_tg      = index_base.get_loc_level(index_base.droplevel(-1)[0])[1].values.copy()
        index_names = list(index_base.names)
    try:
        ndf_idx1 = np.unique(index_base.droplevel(-1).copy().values)
    except ValueError:
        # It means dfwk index is only "timegrp"
        ndf_idx1 = [slice(None), ]
    for idx in ndf_idx1:
        ndfwk = index_base.get_loc_level(idx)[1].values.copy()
        assert np.all(ndfwk == ndf_tg)
    df = pd.DataFrame(index=index_base).reset_index()
    if len(index_names) >= 2:
        df["__tmp"] = (df["timegrp"] + sampling_rate)
        df["__tmp"] = df.groupby(index_names[:-1])["__tmp"].shift(1)
    else:
        df["__tmp"] = (df["timegrp"] + sampling_rate).shift(1)
    df = df.loc[~df["__tmp"].isna()]
    assert (df["__tmp"] == df["timegrp"]).sum() == df.shape[0]
    n        = interval // sampling_rate
    ndf_idx2 = np.concatenate([np.arange(x - n + 1, x + 1, 1, dtype=int) for x in np.arange(ndf_tg.shape[0], dtype=int)])
    ndf_idx2 = ndf_idx2[n * (n - 1):]
    LOGGER.info("END")
    return ndf_idx1, ndf_idx2, ndf_tg, index_names, n

def create_ohlc(
    df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, date_fr: datetime.datetime, date_to: datetime.datetime,
    index_names: str | list[str]=None, from_tx: bool=False
):
    LOGGER.info("STRAT")
    if index_names is None: index_names = []
    df = check_common_input(df, unixtime_name, interval, sampling_rate, date_fr=date_fr, date_to=date_to, from_tx=from_tx, index_names=index_names)
    # create all index patterns
    ndf_tg = np.arange(int(date_fr.timestamp() - interval) // sampling_rate * sampling_rate, int(date_to.timestamp()) // sampling_rate * sampling_rate, sampling_rate, dtype=int)
    if len(index_names) > 0:
        ndf_idxs = [df[x].unique() for x in index_names]
        while len(ndf_idxs) >= 2:
            ndf_tmp  = np.concatenate([np.repeat(ndf_idxs[0], ndf_idxs[1].shape[0]).reshape(-1, 1), np.tile(ndf_idxs[1], ndf_idxs[0].shape[0]).reshape(-1, 1)], axis=-1)
            ndf_idxs = [ndf_tmp, ] + ndf_idxs[2:]
        ndf_idxs = ndf_idxs[0]
        ndf_idxs = np.concatenate([np.repeat(ndf_idxs, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idxs.shape[0]).reshape(-1, 1)], axis=-1)
    else:
        ndf_idxs = ndf_tg
    df_base = pd.DataFrame(ndf_idxs, columns=(index_names + ["timegrp"])).set_index(index_names + ["timegrp"])
    df_base = df_base.sort_values(index_names + ["timegrp"])
    # [ unixtime -> sampling_rate, sampling_rate ] open, high, low, close
    if from_tx:
        assert "price" in df.columns and df["price"].dtype == float
        df   = df.sort_values(index_names + [unixtime_name, "price"]).reset_index(drop=True)
        dfwk = pd.merge(df_base.reset_index(), df[index_names + ["timegrp", "price"]], how="left", on=(index_names + ["timegrp"]))
        dfwk["price"] = dfwk.groupby(index_names + ["timegrp"])["price"].ffill()
        df_smpl       = df_base.copy()
        df_smpl[["open", "high", "low", "close"]] = dfwk.groupby(index_names + ["timegrp"])["price"].aggregate(["first", "max", "min", "last"])
    else:
        df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
        check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
        if (df["interval"] == sampling_rate).sum() == df.shape[0]:
            df_smpl = df.copy().sort_values(index_names + [unixtime_name]).set_index(index_names + ["timegrp"])
        else:
            df_smpl = df_base.copy()
            df_smpl["open" ] = df.groupby(index_names + ["timegrp"])["open" ].first()
            df_smpl["close"] = df.groupby(index_names + ["timegrp"])["close"].last()
            df_smpl["high" ] = df.groupby(index_names + ["timegrp"])["high" ].max()
            df_smpl["low"  ] = df.groupby(index_names + ["timegrp"])["low"  ].min()
    if len(index_names) > 0:
        ## Below process means fill missing value in "open" -> "close" order by using pandas ffill method, in case original OHLC is not completed.
        df_smpl["open" ] = df_smpl.groupby(index_names)[["open", "close"]].apply(lambda x: pd.DataFrame(pd.DataFrame(np.concatenate(x[["open", "close"]].values)).ffill().values.reshape(-1, 2)[:, 0], index=x.index.get_level_values("timegrp")))
        df_smpl["close"] = df_smpl.groupby(index_names)[["open", "close"]].apply(lambda x: pd.DataFrame(pd.DataFrame(np.concatenate(x[["open", "close"]].values)).ffill().values.reshape(-1, 2)[:, 1], index=x.index.get_level_values("timegrp")))
        df_smpl["open"]  = df_smpl.groupby(index_names)["close"].shift(1)
    else:
        df_smpl["open"]  = pd.DataFrame(df_smpl[["open", "close"]].values.reshape(-1)).ffill().values.reshape(-1, 2)[:, 0]
        df_smpl["close"] = pd.DataFrame(df_smpl[["open", "close"]].values.reshape(-1)).ffill().values.reshape(-1, 2)[:, 1]
        df_smpl["open"]  = df_smpl["close"].shift(1)
    df_smpl["high"] = df_smpl[["open", "high", "low", "close"]].max(axis=1) # It might be (df_smpl["open"] > df_smpl["high"])
    df_smpl["low" ] = df_smpl[["open", "high", "low", "close"]].min(axis=1) # It might be (df_smpl["open"] < df_smpl["low" ])
    # [ sampling_rate -> sampling_rate, interval ] open, high, low, close
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    if sampling_rate != interval:
        df_itvl  = df_base.copy()
        _, _, _, index_names, n = indexes_to_aggregate(df_itvl.index.copy(), interval, sampling_rate)
        df_itvl["high" ] = df_smpl.groupby(index_names[:-1], as_index=False)[["high"]].rolling(n).max()["high"]
        df_itvl["low"  ] = df_smpl.groupby(index_names[:-1], as_index=False)[["low" ]].rolling(n).min()["low" ]
        df_itvl["close"] = df_smpl["close"].copy()
        df_itvl["open" ] = df_smpl.groupby(index_names[:-1], as_index=False)["open"].shift(n - 1)
    else:
        df_itvl = df_smpl.copy()
    ## Williams %R
    df_itvl["williams_r"]    = (df_itvl["close"] - df_itvl["low"]) / (df_itvl["high"] - df_itvl["low"])
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    LOGGER.info("END")
    return df_itvl

def ana_size_price(df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, index_base: pd.MultiIndex, from_tx: bool=False):
    LOGGER.info("STRAT")
    df = check_common_input(df, unixtime_name, interval, sampling_rate, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_tg, index_names, n = indexes_to_aggregate(index_base.copy(), interval, sampling_rate)
    df_base = pd.DataFrame(index=index_base.copy())
    # [ unixtime -> sampling_rate, sampling_rate ]
    colbase = ["size", "ntx", "volume"]
    columns = [f"{y}_{x}" for x in ["ask", "bid"] for y in colbase]
    df_smpl = df_base.copy()
    if from_tx:
        assert df.columns.isin([x for x in colbase if x != "ntx"]).sum() == (len(colbase) - 1)
        assert df.columns.isin(["price"]                         ).sum() == 1
        dfwk        = df.groupby(index_names + ["side"])[["size", "volume"]].sum()
        dfwk["ntx"] = df.groupby(index_names + ["side"]).size()
        for side, name in zip([0, 1], ["ask", "bid"]):
            df_smpl[[f"{x}_{name}" for x in colbase]] = dfwk.loc[(slice(None), slice(None), side)][colbase]
            df_smpl[[f"{x}_{name}" for x in colbase]] = df_smpl[[f"{x}_{name}" for x in colbase]].fillna(0)
            df_smpl[f"ntx_{name}"] = df_smpl[f"ntx_{name}"].fillna(0).astype(int)
        dfwk           = df.groupby(index_names)[["volume", "size"]].sum()
        df_smpl["ave"] = (dfwk["volume"] / dfwk["size"])
        dfwk           = pd.merge(df[index_names + ["price", "size"]], df_smpl.reset_index()[index_names + ["ave"]], how="left", on=index_names)
        dfwk["tmp"]    = (dfwk["price"] - dfwk["ave"]).pow(2) * dfwk["size"]
        dfwk           = dfwk.groupby(index_names)[["tmp", "size"]].sum()
        df_smpl["var"] = (dfwk["tmp"] / dfwk["size"])
    else:
        assert df.columns.isin(columns       ).sum() == len(columns)
        assert df.columns.isin(["ave", "var"]).sum() == 2
        df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
        check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
        if (df["interval"] == sampling_rate).sum() == df.shape[0]:
            df_smpl = df.copy().sort_values(index_names).set_index(index_names)
        else:
            dfwk    = df.groupby(index_names)[columns].sum()
            df_smpl = pd.concat([df_smpl, dfwk[columns]], axis=1, ignore_index=False, sort=False).loc[df_smpl.index]
            df_smpl[columns] = df_smpl[columns].fillna(0)
            for name in ["ask", "bid"]:
                df_smpl[f"ntx_{name}"] = df_smpl[f"ntx_{name}"].fillna(0).astype(int)
            df_smpl["ave"] = (df_smpl[["volume_ask", "volume_bid"]].sum(axis=1) / df_smpl[["size_ask", "size_bid"]].sum(axis=1))
            df["size"]     = df[["size_ask",   "size_bid"  ]].sum(axis=1)
            df["volume"]   = df[["volume_ask", "volume_bid"]].sum(axis=1)
            dfwk           = df[index_names + ["ave", "size", "var"]].copy()
            dfwk.columns   = dfwk.columns.str.replace("ave", "price")
            dfwk           = pd.merge(dfwk, df_smpl.reset_index()[index_names + ["ave"]], how="left", on=index_names)
            dfwk["tmp"]    = ((dfwk["price"] - dfwk["ave"]).pow(2) + dfwk["var"]) * dfwk["size"]
            df_smpl["var"] = dfwk.groupby(index_names)["tmp"].sum() / dfwk.groupby(index_names)["size"].sum()
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    if sampling_rate != interval:
        df_itvl = df_base.copy()
        df_smpl["size"]   = df_smpl[["size_ask",   "size_bid"  ]].sum(axis=1).fillna(0)
        df_smpl["volume"] = df_smpl[["volume_ask", "volume_bid"]].sum(axis=1).fillna(0)
        ndf_idx = np.concatenate([ndf_idx2 + ndf_tg.shape[0] * i for i in range(ndf_idx1.shape[0])])
        df_itvl = pd.DataFrame(index=index_base[ndf_idx.reshape(-1, n)[:, -1]])
        dfwkwk  = df_smpl[["ave", "size", "var", "volume"]].iloc[ndf_idx].copy()
        ndfwk1  = dfwkwk["ave"   ].values.reshape(-1, n)
        ndfwk2  = dfwkwk["size"  ].values.reshape(-1, n)
        ndfwk3  = dfwkwk["var"   ].values.reshape(-1, n)
        ndfwk4  = dfwkwk["volume"].values.reshape(-1, n)
        df_itvl["ave"] = np.nansum(ndfwk4, axis=-1) / np.nansum(ndfwk2, axis=-1)
        df_itvl["var"] = np.ma.average(ndfwk3 + (ndfwk1 - df_itvl["ave"].values.reshape(-1, 1)) ** 2, weights=ndfwk2, axis=1).data
        if len(index_names) > 1:
            df_itvl[columns] = df_smpl.groupby(index_names[:-1], as_index=False)[columns].rolling(n).sum()[columns]
        else:
            df_itvl[columns] = df_smpl[columns].rolling(n).sum()[columns]
    else:
        df_itvl = df_smpl.copy()
    if len(index_names) > 1:
        df_itvl["ave"] = df_itvl.groupby(index_names[:-1])["ave"].ffill()
    else:
        df_itvl["ave"] = df_itvl["ave"].ffill()
    df_itvl["var"]           = df_itvl["var"].fillna(0)
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    LOGGER.info("END")
    return df_itvl

def ana_quantile_tx_volume(df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, index_base: pd.MultiIndex, from_tx: bool=False, n_div: int=20):
    LOGGER.info("STRAT")
    df = check_common_input(df, unixtime_name, interval, sampling_rate, from_tx=from_tx)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100
    ndf_idx1, ndf_idx2, ndf_tg, index_names, n = indexes_to_aggregate(index_base.copy(), interval, sampling_rate)
    df_base = pd.DataFrame(index=index_base.copy())
    # [ unixtime -> sampling_rate, sampling_rate ]
    df_smpl  = df_base.copy()
    list_qtl = np.linspace(0.0, 1.0, n_div + 1)
    columns  = [f"volume_q{str(int(x * 1000)).zfill(4)}" for x in list_qtl]
    if from_tx:
        assert df.columns.isin(["side", "volume"]).sum() == 2
        dfwk = df.groupby(index_names + ["side"])["volume"].quantile(list_qtl)
        for side, name in zip([0, 1], ["ask", "bid"]):
            df_smpl[f"ntx_{name}"] = df.groupby(index_names + ["side"]).size().loc[tuple([slice(None)] * len(index_names) + [side, ])]
            df_smpl[f"ntx_{name}"] = df_smpl[f"ntx_{name}"].fillna(0).astype(int)
            for x, y in zip(columns, list_qtl):
                df_smpl[f"{x}_{name}"] = dfwk.loc[tuple([slice(None)] * len(index_names) + [side, y])]
    else:
        assert "ntx_ask" in df.columns and "ntx_bid" in df.columns
        assert df.columns.isin([f"{x}_{name}" for x in columns for name in ["ask", "bid"]]).sum() == ((n_div + 1) * 2)
        df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
        check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
        if (df["interval"] == sampling_rate).sum() == df.shape[0]:
            df_smpl = df.copy().sort_values(index_names).set_index(index_names)
        else:
            for name in ["ask", "bid"]:
                df[f"ntx_{name}"] = df[f"ntx_{name}"].fillna(0).astype(int)
                df["__func"] = df[[f"{x}_{name}" for x in columns]].apply(lambda x: NonLinearXY(list_qtl, x.values, is_ignore_nan=True), axis=1)
                df["__tmp"]  = df[["__func", f"ntx_{name}"]].apply(lambda x: x["__func"](np.linspace(0.0, 1.0, int(x[f"ntx_{name}"]))), axis=1)
                sewk = df.groupby(index_names)["__tmp"].apply(lambda x: np.concatenate(x.tolist()))
                df_smpl[f"ntx_{name}"] = df.groupby(index_names)[f"ntx_{name}"].sum()
                df_smpl[f"ntx_{name}"] = df_smpl[f"ntx_{name}"].fillna(0).astype(int)
                sewk = sewk.apply(lambda x: np.array([0,]) if x.shape[0] == 0 else x)
                sewk = sewk.apply(lambda x: np.quantile(x, list_qtl))
                sewk = sewk.apply(lambda x: np.quantile(x, list_qtl).tolist())
                assert sewk.str[n_div + 1].isna().sum() == sewk.shape[0]
                for i, _ in enumerate(list_qtl):
                    df_smpl[(columns[i] + f"_{name}")] = sewk.str[i]
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    if sampling_rate != interval:
        df_itvl = df_base.copy()
        ndf_idx = np.concatenate([ndf_idx2 + ndf_tg.shape[0] * i for i in range(ndf_idx1.shape[0])])
        df_itvl = pd.DataFrame(index=index_base[ndf_idx.reshape(-1, n)[:, -1]])
        for name in ["ask", "bid"]:
            df_smpl[f"ntx_{name}"] = df_smpl[f"ntx_{name}"].fillna(0).astype(int)
            df_smpl["__func"] = df_smpl[[f"{x}_{name}" for x in columns]].apply(lambda x: NonLinearXY(list_qtl, x.values, is_ignore_nan=True), axis=1)
            df_smpl["__tmp"]  = df_smpl[["__func", f"ntx_{name}"]].apply(lambda x: x["__func"](np.linspace(0.0, 1.0, int(x[f"ntx_{name}"]))), axis=1)
            df_itvl[f"ntx_{name}"] = df_smpl.groupby(index_names[:-1], as_index=False)[f"ntx_{name}"].rolling(n).sum()[f"ntx_{name}"]
            ndfwk = df_smpl["__tmp"].iloc[ndf_idx].values.reshape(-1, n)
            ndfwk = [np.concatenate(x) for x in ndfwk]
            ndfwk = [np.array([0,]) if x.shape[0] == 0 else x for x in ndfwk]
            ndfwk = [np.quantile(x, list_qtl) for x in ndfwk]
            ndfwk = np.stack(ndfwk)
            for i, _ in enumerate(list_qtl):
                df_itvl[(columns[i] + f"_{name}")] = ndfwk[:, i]
    else:
        df_itvl = df_smpl
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_price_over_time(df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, index_base: pd.MultiIndex, from_tx: bool=False, n_div: int=10):
    LOGGER.info("STRAT")
    df = check_common_input(df, unixtime_name, interval, sampling_rate, from_tx=from_tx)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and sampling_rate % n_div == 0
    ndf_idx1, ndf_idx2, ndf_tg, index_names, n = indexes_to_aggregate(index_base.copy(), interval, sampling_rate)
    df_base = pd.DataFrame(index=index_base.copy())
    # [ unixtime -> sampling_rate, sampling_rate ]
    df_smpl  = df_base.copy()
    list_div = np.arange(0.0 + 1.0 / n_div / 2, 1.0, 1.0 / n_div)
    ndf_tg2  = np.arange(ndf_tg.min() // sampling_rate * sampling_rate, (ndf_tg.max() // sampling_rate * sampling_rate) + sampling_rate + 1, sampling_rate // n_div)
    columnsv = [f"volume_p{str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnss = [f"size_p{  str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsa = [f"ave_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsr = [f"var_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    if from_tx:
        assert df.columns.isin(["side", "volume", "size", "price"]).sum() == 4
        dfwk            = pd.DataFrame(ndf_tg2, columns=["timegrp2"])
        dfwk["timegrp"] = (dfwk["timegrp2"] // sampling_rate * sampling_rate).astype(int)
        df_smpl2        = pd.merge(df_base.copy().reset_index(), dfwk, how="left", on="timegrp").set_index(index_names[:-1] + ["timegrp2"])
        df["timegrp2"]  = (df[unixtime_name] // (sampling_rate // n_div) * (sampling_rate // n_div)).astype(int)
        dfwkwk = df.groupby(index_names[:-1] + ["timegrp2", "side"])[["volume", "size"]].sum()
        for side, name in zip([0, 1], ["ask", "bid"]):
            for colname in ["volume", "size"]:
                df_smpl2[f"{colname}_{name}"] = dfwkwk.loc[(slice(None), slice(None), side)][colname]
                df_smpl2[f"{colname}_{name}"] = df_smpl2[f"{colname}_{name}"].fillna(0)
        for side, name in zip([0, 1], ["ask", "bid"]):
            for colname in ["volume", "size"]:
                sewk = df_smpl2.reset_index().groupby(index_names)[f"{colname}_{name}"].apply(lambda x: x.tolist())
                assert sewk.str[n_div].isna().sum() == sewk.shape[0]
                for i, x in enumerate({"volume": columnsv, "size": columnss}[colname]):
                    df_smpl[f"{x}_{name}"] = sewk.str[i]
                    df_smpl[f"{x}_{name}"] = df_smpl[f"{x}_{name}"].fillna(0)
        for cola, colv, cols in zip(columnsa, columnsv, columnss):
            df_smpl[cola] = (df_smpl[f"{colv}_ask"] + df_smpl[f"{colv}_bid"]) / (df_smpl[f"{cols}_ask"] + df_smpl[f"{cols}_bid"]).replace(0, float("nan"))
        dfwk              = df_smpl.groupby(index_names[:-1], group_keys=False)[columnsa].apply(lambda x: pd.DataFrame(pd.Series(x.values.reshape(-1)).ffill().values.reshape(-1, n_div), index=x.index))
        dfwk.columns      = columnsa
        df_smpl[columnsa] = dfwk[columnsa]
        dfwk        = df.groupby(index_names[:-1] + ["timegrp2"])[["volume", "size"]].sum()
        dfwk["ave"] = (dfwk["volume"] / dfwk["size"])
        dfwk        = pd.merge(df[index_names[:-1] + ["timegrp2", "price", "size"]], dfwk.reset_index()[index_names[:-1] + ["timegrp2", "ave"]], how="left", on=(index_names[:-1] + ["timegrp2"]))
        dfwk["tmp"] = (dfwk["price"] - dfwk["ave"]).pow(2) * dfwk["size"]
        dfwk        = dfwk.groupby(index_names[:-1] + ["timegrp2"])[["tmp", "size"]].sum()
        dfwk["var"] = (dfwk["tmp"] / dfwk["size"])
        df_smpl2["var"] = dfwk["var"]
        sewk = df_smpl2.reset_index().groupby(index_names)["var"].apply(lambda x: x.tolist())
        assert sewk.str[n_div].isna().sum() == sewk.shape[0]
        for i, x in enumerate(columnsr):
            df_smpl[x] = sewk.str[i]
            df_smpl[x] = df_smpl[x].fillna(0)
    else:
        assert df.columns.isin([f"{x}_{name}" for x in (columnsv + columnss) for name in ["ask", "bid"]]).sum() == (n_div * 2 * 2)
        assert df.columns.isin(columnsa + columnsr).sum() == (n_div * 2)
        df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
        check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
        if (df["interval"] == sampling_rate).sum() == df.shape[0]:
            df_smpl = df.copy().sort_values(index_names).set_index(index_names)
        else:
            for name in ["ask", "bid"]:
                for colname in ["volume", "size"]:
                    columns = {"volume": columnsv, "size": columnss}[colname]
                    sewk    = df.groupby(index_names)[[f"{x}_{name}" for x in columns]].apply(lambda y: np.concatenate(y.values.tolist()).reshape(n_div, -1).sum(axis=-1).tolist())
                    assert sewk.str[n_div].isna().sum() == sewk.shape[0]
                    for i, x in enumerate(columns):
                        df_smpl[f"{x}_{name}"] = sewk.str[i]
                        df_smpl[f"{x}_{name}"] = df_smpl[f"{x}_{name}"].fillna(0)
            for x in (columnss + columnsv): df[x] = (df[f"{x}_ask"] + df[f"{x}_bid"])
            sewkv = df.groupby(index_names)[columnsv].apply(lambda x: np.nansum(x.values.reshape(n_div, -1), axis=-1))
            sewks = df.groupby(index_names)[columnss].apply(lambda x: np.nansum(x.values.reshape(n_div, -1), axis=-1))
            sewka = sewkv / sewks
            for i, x in enumerate(columnsv): df_smpl[x] = sewkv.str[i]
            for i, x in enumerate(columnss): df_smpl[x] = sewks.str[i]
            for i, x in enumerate(columnsa): df_smpl[x] = sewka.str[i]
            sewks2 = df.groupby(index_names)[columnss].apply(lambda x: x.values.reshape(n_div, -1))
            sewka2 = df.groupby(index_names)[columnsa].apply(lambda x: x.values.reshape(n_div, -1))
            sewkr2 = df.groupby(index_names)[columnsr].apply(lambda x: x.values.reshape(n_div, -1))
            for i, x in enumerate(columnsr): df_smpl[x] = (((sewka2.str[i] - sewka.str[i]) ** 2 + sewkr2.str[i]) * sewks2.str[i]).apply(lambda x: np.nansum(x)) / sewks.str[i]
            dfwk              = df_smpl.groupby(index_names[:-1], group_keys=False)[columnsa].apply(lambda x: pd.DataFrame(pd.Series(x.values.reshape(-1)).ffill().values.reshape(-1, n_div), index=x.index))
            dfwk.columns      = columnsa
            df_smpl[columnsa] = dfwk[columnsa]
            df_smpl[columnsr] = df_smpl[columnsr].fillna(0)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    if sampling_rate != interval:
        for x in (columnss + columnsv): df_smpl[x] = (df_smpl[f"{x}_ask"] + df_smpl[f"{x}_bid"]).fillna(0)
        ndf_idx = np.concatenate([ndf_idx2 + ndf_tg.shape[0] * i for i in range(ndf_idx1.shape[0])])        
        df_itvl = pd.DataFrame(index=index_base[ndf_idx.reshape(-1, n)[:, -1]])
        for columns in [columnsv, columnss]:
            for name in ["ask", "bid"]:
                ndfwk = df_smpl[[f"{x}_{name}" for x in columns]].iloc[ndf_idx].values.reshape(-1, len(columns) * n)
                ndfwk = ndfwk.reshape(ndfwk.shape[0], n_div, -1)
                ndfwk = np.nansum(ndfwk, axis=-1)
                df_itvl[[f"{x}_{name}" for x in columns]] = ndfwk
        ndfwks  = df_smpl[columnss].iloc[ndf_idx].values.reshape(-1, n_div, n)
        ndfwka  = df_smpl[columnsa].iloc[ndf_idx].values.reshape(-1, n_div, n)
        ndfwkr  = df_smpl[columnsr].iloc[ndf_idx].values.reshape(-1, n_div, n)
        ndfwkv  = df_smpl[columnsv].iloc[ndf_idx].values.reshape(-1, n_div, n)
        ndfwk1  = np.nansum(ndfwkv, axis=-1) / np.nansum(ndfwks, axis=-1)
        ndfwk2  = np.ma.average((ndfwka - ndfwk1.reshape(-1, n_div, 1)) ** 2 + ndfwkr, weights=ndfwks, axis=-1).data
        ndfwk2[np.isnan(ndfwk1)] = float("nan")
        df_itvl[columnsa] = ndfwk1
        dfwk              = df_itvl.groupby(index_names[:-1], group_keys=False)[columnsa].apply(lambda x: pd.DataFrame(pd.Series(x.values.reshape(-1)).ffill().values.reshape(-1, n_div), index=x.index))
        dfwk.columns      = columnsa
        df_itvl[columnsa] = dfwk[columnsa]
        df_itvl[columnsr] = ndfwk2
        df_itvl[columnsr] = df_itvl[columnsr].fillna(0)
    else:
        df_itvl = df_smpl.copy()
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_over_price(df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, index_base: pd.MultiIndex, from_tx: bool=False, n_div: int=20):
    LOGGER.info("STRAT")
    df = check_common_input(df, unixtime_name, interval, sampling_rate, from_tx=from_tx)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100
    ndf_idx1, ndf_idx2, ndf_tg, index_names, n = indexes_to_aggregate(index_base.copy(), interval, sampling_rate)
    df_base  = pd.DataFrame(index=index_base.copy())
    list_div = np.linspace(0.0, 1.0, n_div + 1)
    list_ctr = (list_div[:-1] + list_div[1:]) / 2
    # [ unixtime -> sampling_rate, sampling_rate ]
    df_smpl  = df_base.copy()
    columns1 = [f"price_h{       str(int(x * 1000)).zfill(4)}" for x in list_div]
    columns2 = [f"volume_price_h{str(int(x * 1000)).zfill(4)}" for x in list_ctr]
    if from_tx:
        assert df.columns.isin(["side", "price", "volume"]).sum() == 3
        df["volume_ask"] = df["volume"] * (df["side"] == 0).astype(float)
        df["volume_bid"] = df["volume"] * (df["side"] == 1).astype(float)
        sewk = df.groupby(index_names)[["price", "volume_ask", "volume_bid"]].apply(
            lambda x: np.concatenate(np.histogram(x["price"], n_div, weights=x["volume_ask"])[::-1]).tolist() + np.histogram(x["price"], n_div, weights=x["volume_bid"])[0].tolist()
        )
        assert sewk.str[len(list_div) + (len(list_ctr) * 2)].isna().sum() == sewk.shape[0]
        for i, x in enumerate(columns1): df_smpl[x] = sewk.str[i]
        for name, i_offset in zip(["ask", "bid"], [len(list_div), len(list_div) + n_div]):
            for i, x in enumerate(columns2):
                df_smpl[f"{x}_{name}"] = sewk.str[i_offset + i]
                df_smpl[f"{x}_{name}"] = df_smpl[f"{x}_{name}"].fillna(0)
    else:
        assert df.columns.isin(columns1).sum() == len(columns1)
        assert df.columns.isin([f"{x}_{name}" for x in columns2 for name in ["ask", "bid"]]).sum() == (n_div * 2)
        df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
        check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
        if (df["interval"] == sampling_rate).sum() == df.shape[0]:
            df_smpl = df.copy().sort_values(index_names).set_index(index_names)
        else:
            df["tmpx" ] = df[columns1].apply(lambda x: NonLinearXY(list_div, x.values, is_ignore_nan=True)(list_ctr, return_nan_to_value=0), axis=1)
            df["tmpy1"] = df[[f"{x}_ask" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
            df["tmpy2"] = df[[f"{x}_bid" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
            df["tmpb" ] = df["tmpx"].apply(lambda x: x > 0)
            df["tmpx" ] = df[["tmpx",  "tmpb"]].apply(lambda x: x["tmpx" ][x["tmpb"]], axis=1)
            df["tmpy1"] = df[["tmpy1", "tmpb"]].apply(lambda x: x["tmpy1"][x["tmpb"]], axis=1)
            df["tmpy2"] = df[["tmpy2", "tmpb"]].apply(lambda x: x["tmpy2"][x["tmpb"]], axis=1)
            dfwk          = df.groupby(index_names)["tmpx" ].apply(lambda x: np.concatenate(x.values)).reset_index().set_index(index_names)
            dfwk["tmpy1"] = df.groupby(index_names)["tmpy1"].apply(lambda x: np.concatenate(x.values))
            dfwk["tmpy2"] = df.groupby(index_names)["tmpy2"].apply(lambda x: np.concatenate(x.values))
            sewk = dfwk[["tmpx", "tmpy1", "tmpy2"]].apply(
                lambda x: np.concatenate(np.histogram(x["tmpx"], n_div, weights=x["tmpy1"])[::-1]).tolist() + np.histogram(x["tmpx"], n_div, weights=x["tmpy2"])[0].tolist(), axis=1
            )
            # >>> np.histogram(np.array([]), 5)
            # (array([0, 0, 0, 0, 0]), array([0. , 0.2, 0.4, 0.6, 0.8, 1. ]))
            assert sewk.str[len(list_div) + (len(list_ctr) * 2)].isna().sum() == sewk.shape[0]
            sewk.loc[dfwk["tmpx"].str[0].isna()] = [[] for _ in range(dfwk["tmpx"].str[0].isna().sum())] # Force it to nan when access via sewk.str[i]
            for i, x in enumerate(columns1): df_smpl[x] = sewk.str[i]
            for name, i_offset in zip(["ask", "bid"], [len(list_div), len(list_div) + n_div]):
                for i, x in enumerate(columns2):
                    df_smpl[f"{x}_{name}"] = sewk.str[i_offset + i]
                    df_smpl[f"{x}_{name}"] = df_smpl[f"{x}_{name}"].fillna(0)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    if sampling_rate != interval:
        ndf_idx = np.concatenate([ndf_idx2 + ndf_tg.shape[0] * i for i in range(ndf_idx1.shape[0])])
        df_itvl = pd.DataFrame(index=index_base[ndf_idx.reshape(-1, n)[:, -1]])
        dfwk            = df_smpl[columns1 + [f"{x}_{name}" for x in columns2 for name in ["ask", "bid"]]].iloc[ndf_idx].copy().reset_index()
        dfwk["tmpx" ]   = dfwk[columns1].apply(lambda x: NonLinearXY(list_div, x.values, is_ignore_nan=True)(list_ctr, return_nan_to_value=0), axis=1)
        dfwk["tmpy1"]   = dfwk[[f"{x}_ask" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
        dfwk["tmpy2"]   = dfwk[[f"{x}_bid" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
        dfwk["tmpb" ]   = dfwk["tmpx"].apply(lambda x: x > 0)
        dfwk["tmpx" ]   = dfwk[["tmpx",  "tmpb"]].apply(lambda x: x["tmpx" ][x["tmpb"]], axis=1)
        dfwk["tmpy1"]   = dfwk[["tmpy1", "tmpb"]].apply(lambda x: x["tmpy1"][x["tmpb"]], axis=1)
        dfwk["tmpy2"]   = dfwk[["tmpy2", "tmpb"]].apply(lambda x: x["tmpy2"][x["tmpb"]], axis=1)
        dfwkwk          = pd.DataFrame(index=df_itvl.index.copy())
        dfwkwk["tmpx" ] = [np.concatenate(x) for x in dfwk["tmpx" ].values.reshape(-1, n)]
        dfwkwk["tmpy1"] = [np.concatenate(x) for x in dfwk["tmpy1"].values.reshape(-1, n)]
        dfwkwk["tmpy2"] = [np.concatenate(x) for x in dfwk["tmpy2"].values.reshape(-1, n)]
        sewk = dfwkwk[["tmpx", "tmpy1", "tmpy2"]].apply(
            lambda x: (
                np.concatenate(np.histogram(x["tmpx"], n_div, weights=x["tmpy1"])[::-1]).tolist() + 
                np.histogram(x["tmpx"], n_div, weights=x["tmpy2"])[0].tolist()
            ), axis=1
        )
        assert sewk.str[len(list_div) + (len(list_ctr) * 2)].isna().sum() == sewk.shape[0]
        sewk.loc[dfwkwk["tmpx"].str[0].isna()] = [[] for _ in range(dfwkwk["tmpx"].str[0].isna().sum())] # Force it to nan when access via sewk.str[i]
        for i, x in enumerate(columns1): df_itvl[x] = sewk.str[i]
        for name, i_offset in zip(["ask", "bid"], [len(list_div), len(list_div) + n_div]):
            for i, x in enumerate(columns2):
                df_itvl[f"{x}_{name}"] = sewk.str[i_offset + i]
                df_itvl[f"{x}_{name}"] = df_itvl[f"{x}_{name}"].fillna(0)
    else:
        df_itvl = df_smpl
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    LOGGER.info("END")
    return df_itvl

def ana_rank_corr_index(df: pd.DataFrame, unixtime_name: str, interval: int, sampling_rate: int, index_base: pd.MultiIndex):
    LOGGER.info("STRAT")
    df = check_common_input(df, unixtime_name, interval, sampling_rate, from_tx=False)
    ndf_idx1, ndf_idx2, ndf_tg, index_names, n = indexes_to_aggregate(index_base.copy(), interval, sampling_rate)
    # [ unixtime -> sampling_rate, sampling_rate ]
    assert df.columns.isin(["ave"]).sum() == 1
    df = df.sort_values(index_names + [unixtime_name]).reset_index(drop=True)
    check_interval(df, unixtime_name, sampling_rate, index_names=index_names)
    assert (df["interval"] == sampling_rate).sum() == df.shape[0]
    df_smpl = df.copy().sort_values(index_names).set_index(index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    assert interval > sampling_rate
    df_smpl = df_smpl.loc[df_smpl.index.get_level_values('timegrp').isin(ndf_tg)]
    ndf_idx = np.concatenate([ndf_idx2 + ndf_tg.shape[0] * i for i in range(ndf_idx1.shape[0])])
    df_itvl = pd.DataFrame(index=index_base[ndf_idx.reshape(-1, n)[:, -1]])
    ## RCI
    ndf     = df_smpl.iloc[ndf_idx]["ave"].values.reshape(-1, n)
    ndf_price_rank = -1 * np.argsort(np.argsort(ndf, axis=-1)) + n
    ndf_time_rank  = np.tile(np.arange(n, 0, -1, dtype=int), (ndf.shape[0], 1))
    df_itvl["rci"] = 1 - (6 * ((ndf_price_rank - ndf_time_rank) ** 2).sum(axis=-1)) / (n * (n ** 2 - 1))
    ## RSI
    ndfwk  = ndf[:, 1:] - ndf[:, :-1]
    ndfwk1 = ndfwk.copy()
    ndfwk1[ndfwk1 < 0] = 0
    ndfwk2 = ndfwk.copy()
    ndfwk2[ndfwk2 >= 0] = 0
    ndfwk1 = np.nansum(ndfwk1, axis=-1) / n
    ndfwk2 = np.nansum(ndfwk2, axis=-1) / n * -1
    df_itvl["rsi"] = ndfwk1 / (ndfwk1 + ndfwk2)
    ## Psychological line
    df_itvl["psy_line"] = (ndfwk > 0).sum(axis=-1) / n
    df_itvl["interval"]      = interval
    df_itvl["sampling_rate"] = sampling_rate
    return df_itvl
