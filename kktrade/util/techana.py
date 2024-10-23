import datetime, warnings
import pandas as pd
import numpy as np
# local package
from kkpsgre.util.com import check_type_list
from kkpsgre.util.logger import set_logger
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
    assert isinstance(interval,      int) and interval      >= 30
    assert isinstance(sampling_rate, int) and sampling_rate >= 30
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
    ndf_tg        = np.arange(int(date_fr.timestamp() - interval) // sampling_rate * sampling_rate, int(date_to.timestamp()) // sampling_rate * sampling_rate, sampling_rate, dtype=int)
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
            ## Below process means fill missing value in "open" -> "close" order by using pandas ffill method, in case original OHLC is not completed.
            df_smpl["open" ] = df_smpl.groupby(["symbol"])[["open", "close"]].apply(lambda x: pd.DataFrame(pd.DataFrame(np.concatenate(x[["open", "close"]].values)).ffill().values.reshape(-1, 2)[:, 0], index=x.index.get_level_values("timegrp")))
            df_smpl["close"] = df_smpl.groupby(["symbol"])[["open", "close"]].apply(lambda x: pd.DataFrame(pd.DataFrame(np.concatenate(x[["open", "close"]].values)).ffill().values.reshape(-1, 2)[:, 1], index=x.index.get_level_values("timegrp")))
    if len(index_names) > 0:
        df_smpl["open"] = df_smpl.groupby(index_names)["close"].shift(1)
    else:
        df_smpl["open"] = df_smpl["close"].shift(1)
    df_smpl["high"] = df_smpl[["open", "high", "low", "close"]].max(axis=1) # It might be (df_smpl["open"] > df_smpl["high"])
    df_smpl["low" ] = df_smpl[["open", "high", "low", "close"]].min(axis=1) # It might be (df_smpl["open"] < df_smpl["low" ])
    # [ sampling_rate -> sampling_rate, interval ] open, high, low, close
    if sampling_rate != interval:
        df_itvl  = df_base.copy()
        _, _, _, index_names, n = indexes_to_aggregate(df_itvl.index.copy(), interval, sampling_rate)
        df_itvl["high" ] = df_smpl.groupby(index_names[:-1], as_index=False)[["high"]].rolling(n).max()["high"]
        df_itvl["low"  ] = df_smpl.groupby(index_names[:-1], as_index=False)[["low" ]].rolling(n).min()["low" ]
        df_itvl["close"] = df_smpl["close"].copy()
        df_itvl["open" ] = df_smpl.groupby(index_names[:-1], as_index=False)["open"].shift(n - 1)
    else:
        df_itvl = df_smpl.copy()
    df_itvl["interval"] = interval
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
            df["size"]     = df[["size_ask", "size_bid"]].sum(axis=1)
            df_smpl["ave"] = df.groupby(index_names)[[       "ave", "size"]].apply(lambda x: np.average(x["ave"], weights=x["size"]))
            df_smpl["var"] = df.groupby(index_names)[["var", "ave", "size"]].apply(lambda x: np.average(((x["ave"] - np.average(x["ave"], weights=x["size"])) ** 2) + x["var"], weights=x["size"]))
    # [ sampling_rate -> sampling_rate, interval ]
    if sampling_rate != interval:
        df_itvl = df_base.copy()
        df_smpl["size"] = df_smpl[["size_ask", "size_bid"]].sum(axis=1)
        list_df = []
        for idx in ndf_idx1:
            if not isinstance(idx, tuple): idx = (idx, )
            dfwkwk = df_smpl[["ave", "size", "var"]].loc[idx].iloc[ndf_idx2]
            ndfwk1 = dfwkwk["ave" ].values.reshape(-1, n)
            ndfwk2 = dfwkwk["size"].values.reshape(-1, n)
            ndfwk3 = dfwkwk["var" ].values.reshape(-1, n)
            dfwk   = pd.DataFrame(ndf_tg[ndf_idx2.reshape(-1, n)[:, -1]], columns=["timegrp"])
            for x, y in zip(index_names, idx): dfwk[x] = y
            dfwk["ave"] = np.average(ndfwk1, weights=ndfwk2, axis=1)
            dfwk["var"] = np.average(ndfwk3 + (ndfwk1 - dfwk["ave"].values.reshape(-1, 1)) ** 2, weights=ndfwk2, axis=1)
            dfwk   = dfwk.set_index(index_names)
            list_df.append(dfwk)
        df_itvl = pd.concat([df_itvl, pd.concat(list_df, axis=0, ignore_index=False, sort=False)], axis=1, ignore_index=False, sort=False).loc[df_itvl.index]
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
    df_itvl["var"]      = df_itvl["var"].fillna(0)
    df_itvl["interval"] = interval
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
                df["__func"] = df[[f"{x}_{name}" for x in columns]].apply(lambda x: NonLinearXY(list_qtl, x.values, is_ignore_nan=True), axis=1)
                df["__tmp"]  = df[["__func", f"ntx_{name}"]].apply(lambda x: x["__func"](np.linspace(0.0, 1.0, int(x[f"ntx_{name}"]))), axis=1)
                sewk = df.groupby(index_names)["__tmp"].apply(lambda x: np.concatenate(x.tolist()))
                df_smpl[f"ntx_{name}"] = df.groupby(index_names)[f"ntx_{name}"].sum()
                sewk = sewk.apply(lambda x: np.array([0,]) if x.shape[0] == 0 else x)
                sewk = sewk.apply(lambda x: np.quantile(x, list_qtl))
                sewk = sewk.apply(lambda x: np.quantile(x, list_qtl).tolist())
                assert sewk.str[n_div + 1].isna().sum() == sewk.shape[0]
                for i, _ in enumerate(list_qtl):
                    df_smpl[(columns[i] + f"_{name}")] = sewk.str[i]
    # [ sampling_rate -> sampling_rate, interval ]
    if sampling_rate != interval:
        df_itvl = df_base.copy()
        for name in ["ask", "bid"]:
            df_smpl["__func"] = df_smpl[[f"{x}_{name}" for x in columns]].apply(lambda x: NonLinearXY(list_qtl, x.values, is_ignore_nan=True), axis=1)
            df_smpl["__tmp"]  = df_smpl[["__func", f"ntx_{name}"]].apply(lambda x: x["__func"](np.linspace(0.0, 1.0, int(x[f"ntx_{name}"]))), axis=1)
            df_itvl[f"ntx_{name}"] = df_smpl.groupby(index_names[:-1], as_index=False)[f"ntx_{name}"].rolling(n).sum()[f"ntx_{name}"]
            list_df = []
            for idx in ndf_idx1:
                if not isinstance(idx, tuple): idx = (idx, )
                ndfwk = df_smpl["__tmp"].loc[idx].iloc[ndf_idx2].values.reshape(-1, n)
                ndfwk = [np.concatenate(x) for x in ndfwk]
                ndfwk = [np.array([0,]) if x.shape[0] == 0 else x for x in ndfwk]
                ndfwk = [np.quantile(x, list_qtl) for x in ndfwk]
                ndfwk = np.stack(ndfwk)
                dfwk  = pd.DataFrame(ndf_tg[ndf_idx2.reshape(-1, n)[:, -1]], columns=["timegrp"])
                for x, y in zip(index_names, idx): dfwk[x] = y
                for i, _ in enumerate(list_qtl):
                    dfwk[(columns[i] + f"_{name}")] = ndfwk[:, i]
                dfwk  = dfwk.set_index(index_names)
                list_df.append(dfwk)
            df_itvl = pd.concat([df_itvl, pd.concat(list_df, axis=0, ignore_index=False, sort=False)], axis=1, ignore_index=False, sort=False).loc[df_itvl.index]
    else:
        df_itvl = df_smpl
    df_itvl["interval"] = interval
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
        for cola, colv, cols in zip(columnsa, columnsv, columnss):
            df_smpl[cola] = (df_smpl[f"{colv}_ask"] + df_smpl[f"{colv}_bid"]) / (df_smpl[f"{cols}_ask"] + df_smpl[f"{cols}_bid"]).replace(0, float("nan"))
        df_smpl[columnsa] = np.concatenate(df_smpl.groupby(index_names[:-1])[columnsa].apply(lambda x: pd.Series(x.values.reshape(-1)).ffill().values.reshape(-1, n_div)).tolist(), axis=0)
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
            for x in columnss: df[x] = (df[f"{x}_ask"] + df[f"{x}_bid"])
            dfwk         = df.groupby(index_names)[columnsa].apply(lambda x: x.values.reshape(n_div, -1))
            dfwk         = dfwk.reset_index().set_index(dfwk.index.names)
            dfwk.columns = ["price"]
            dfwk["size"] = df.groupby(index_names)[columnss].apply(lambda x: x.values.reshape(n_div, -1))
            dfwk["var" ] = df.groupby(index_names)[columnsr].apply(lambda x: x.values.reshape(n_div, -1))
            dfwk["ave" ] = dfwk[["price", "size"]].apply(lambda x: np.ma.average(x["price"], weights=x["size"], axis=-1).data, axis=1) # To avoid sum(size) == 0, which occur ZeroDivisionError, use np.ma.average. 
            assert dfwk["ave"].str[n_div].isna().sum() == dfwk.shape[0]
            dfwk["var2"] = dfwk[["ave", "price", "size", "var"]].apply(lambda x: np.ma.average((x["price"] - x["ave"].reshape(-1, 1)) ** 2 + x["var"], weights=x["size"], axis=-1).data, axis=1)
            assert dfwk["var2"].str[n_div].isna().sum() == dfwk.shape[0]
            ndfwka = np.stack(dfwk["ave" ].values, axis=0)
            ndfwkr = np.stack(dfwk["var2"].values, axis=0)
            boolwk = (ndfwka == 0)
            ndfwka[boolwk] = float("nan")
            ndfwkr[boolwk] = float("nan")
            dfwk[columnsa] = ndfwka
            dfwk[columnsr] = ndfwkr
            df_smpl = pd.concat([df_smpl, dfwk[columnsa + columnsr]], axis=1, ignore_index=False, sort=False).loc[df_smpl.index]
            df_smpl[columnsa] = np.concatenate(df_smpl.groupby(index_names[:-1])[columnsa].apply(lambda x: pd.Series(x.values.reshape(-1)).ffill().values.reshape(-1, n_div)).tolist(), axis=0)
            df_smpl[columnsr] = df_smpl[columnsr].fillna(0)
    # [ sampling_rate -> sampling_rate, interval ]
    if sampling_rate != interval:
        for x in columnss: df_smpl[x] = (df_smpl[f"{x}_ask"] + df_smpl[f"{x}_bid"]).fillna(0)
        df_itvl = df_base.copy()
        list_df = []
        for idx in ndf_idx1:
            if not isinstance(idx, tuple): idx = (idx, )
            dfwk = pd.DataFrame(ndf_tg[ndf_idx2.reshape(-1, n)[:, -1]], columns=["timegrp"])
            for x, y in zip(index_names, idx): dfwk[x] = y
            dfwk = dfwk.set_index(index_names)
            for columns in [columnsv, columnss]:
                for name in ["ask", "bid"]:
                    ndfwk = df_smpl[[f"{x}_{name}" for x in columns]].loc[idx].iloc[ndf_idx2].values.reshape(-1, len(columns) * n)
                    ndfwk = ndfwk.reshape(ndfwk.shape[0], n_div, -1)
                    ndfwk = np.nansum(ndfwk, axis=-1)
                    dfwk[[f"{x}_{name}" for x in columns]] = ndfwk.copy()
            ndfwks  = df_smpl[columnss].loc[idx].iloc[ndf_idx2].values.reshape(-1, n_div, n)
            ndfwka  = df_smpl[columnsa].loc[idx].iloc[ndf_idx2].values.reshape(-1, n_div, n)
            ndfwkr  = df_smpl[columnsr].loc[idx].iloc[ndf_idx2].values.reshape(-1, n_div, n)
            ndfwk1  = np.ma.average(ndfwka, weights=ndfwks, axis=-1).data
            ndfwk2  = np.ma.average((ndfwka - ndfwk1.reshape(-1, n_div, 1)) ** 2 + ndfwkr, weights=ndfwks, axis=-1).data
            ndfbool = (ndfwk1 == 0)
            ndfwk1[ndfbool] = float("nan")
            ndfwk2[ndfbool] = float("nan")
            dfwk[columnsa]  = pd.Series(ndfwk1.reshape(-1)).ffill().values.reshape(-1, n_div)
            dfwk[columnsr]  = ndfwk2
            dfwk[columnsr]  = dfwk[columnsr].fillna(0)
            list_df.append(dfwk)
        df_itvl = pd.concat([df_itvl, pd.concat(list_df, axis=0, ignore_index=False, sort=False)], axis=1, ignore_index=False, sort=False).loc[df_itvl.index]
    else:
        df_itvl = df_smpl.copy()
    df_itvl["interval"] = interval
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
            sewk = df.groupby(index_names)[["tmpx", "tmpy1", "tmpy2"]].apply(
                lambda x: (
                    np.concatenate(np.histogram(np.concatenate(x["tmpx"].values), n_div, weights=np.concatenate(x["tmpy1"].values))[::-1]).tolist() + 
                    np.histogram(np.concatenate(x["tmpx"].values), n_div, weights=np.concatenate(x["tmpy2"].values))[0].tolist()
                )
            )
            # >>> np.histogram(np.array([]), 5)
            # (array([0, 0, 0, 0, 0]), array([0. , 0.2, 0.4, 0.6, 0.8, 1. ]))
            assert sewk.str[len(list_div) + (len(list_ctr) * 2)].isna().sum() == sewk.shape[0]
            sewk.loc[df["tmpx"].str[0].isna()] = [[] for _ in range(df["tmpx"].str[0].isna().sum())] # Force it to nan when access via sewk.str[i]
            for i, x in enumerate(columns1): df_smpl[x] = sewk.str[i]
            for name, i_offset in zip(["ask", "bid"], [len(list_div), len(list_div) + n_div]):
                for i, x in enumerate(columns2):
                    df_smpl[f"{x}_{name}"] = sewk.str[i_offset + i]
                    df_smpl[f"{x}_{name}"] = df_smpl[f"{x}_{name}"].fillna(0)
    # [ sampling_rate -> sampling_rate, interval ]
    if sampling_rate != interval:
        df_itvl = df_base.copy()
        list_df = []
        for idx in ndf_idx1:
            if not isinstance(idx, tuple): idx = (idx, )
            dfwk            = df_smpl[columns1 + [f"{x}_{name}" for x in columns2 for name in ["ask", "bid"]]].loc[idx].iloc[ndf_idx2].copy().reset_index()
            dfwk["timegrp"] = ndf_tg[ndf_idx2.reshape(-1, n)[:, -1]].repeat(n)
            dfwk            = dfwk.set_index(["timegrp"])
            dfwk["tmpx" ]   = dfwk[columns1].apply(lambda x: NonLinearXY(list_div, x.values, is_ignore_nan=True)(list_ctr, return_nan_to_value=0), axis=1)
            dfwk["tmpy1"]   = dfwk[[f"{x}_ask" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
            dfwk["tmpy2"]   = dfwk[[f"{x}_bid" for x in columns2]].fillna(0).apply(lambda x: x.values, axis=1)
            dfwk["tmpb" ]   = dfwk["tmpx"].apply(lambda x: x > 0)
            dfwk["tmpx" ]   = dfwk[["tmpx",  "tmpb"]].apply(lambda x: x["tmpx" ][x["tmpb"]], axis=1)
            dfwk["tmpy1"]   = dfwk[["tmpy1", "tmpb"]].apply(lambda x: x["tmpy1"][x["tmpb"]], axis=1)
            dfwk["tmpy2"]   = dfwk[["tmpy2", "tmpb"]].apply(lambda x: x["tmpy2"][x["tmpb"]], axis=1)
            sewk = dfwk.groupby(["timegrp"])[["tmpx", "tmpy1", "tmpy2"]].apply(
                lambda x: (
                    np.concatenate(np.histogram(np.concatenate(x["tmpx"].values), n_div, weights=np.concatenate(x["tmpy1"].values))[::-1]).tolist() + 
                    np.histogram(np.concatenate(x["tmpx"].values), n_div, weights=np.concatenate(x["tmpy2"].values))[0].tolist()
                )
            )
            assert sewk.str[len(list_div) + (len(list_ctr) * 2)].isna().sum() == sewk.shape[0]
            sewk.loc[dfwk["tmpx"].str[0].isna()] = [[] for _ in range(dfwk["tmpx"].str[0].isna().sum())] # Force it to nan when access via sewk.str[i]
            dfwk = pd.DataFrame(ndf_tg[ndf_idx2.reshape(-1, n)[:, -1]], columns=["timegrp"]).set_index(["timegrp"])
            for i, x in enumerate(columns1): dfwk[x] = sewk.str[i]
            for name, i_offset in zip(["ask", "bid"], [len(list_div), len(list_div) + n_div]):
                for i, x in enumerate(columns2):
                    dfwk[f"{x}_{name}"] = sewk.str[i_offset + i]
                    dfwk[f"{x}_{name}"] = dfwk[f"{x}_{name}"].fillna(0)
            dfwk = dfwk.reset_index(drop=False)
            for x, y in zip(index_names, idx): dfwk[x] = y
            dfwk = dfwk.set_index(index_names)
            list_df.append(dfwk)
        df_itvl = pd.concat([df_itvl, pd.concat(list_df, axis=0, ignore_index=False, sort=False)], axis=1, ignore_index=False, sort=False).loc[df_itvl.index]
    else:
        df_itvl = df_smpl
    df_itvl["interval"] = interval
    LOGGER.info("END")
    return df_itvl
