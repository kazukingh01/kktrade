import datetime, re
import pandas as pd
import polars as pl
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
CHECK_DATETIME_MIN = datetime.datetime(2000,1,1).timestamp()
CHECK_DATETIME_MAX = datetime.datetime(2099,1,1).timestamp()


def check_common_input(
    df: pl.DataFrame, interval: int, sampling_rate: int, unixtime_name: str="unixtime", 
    date_fr: datetime.datetime=None, date_to: datetime.datetime=None, from_tx: bool=None,
    index_names: str | list[str]=None, 
) -> tuple[pl.DataFrame, list[str]]:
    LOGGER.info("STRAT")
    assert isinstance(df, pl.DataFrame)
    assert isinstance(interval,      int) and interval      >= 1
    assert isinstance(sampling_rate, int) and sampling_rate >= 1
    assert isinstance(unixtime_name, str) and unixtime_name != "timegrp"
    assert not ("timegrp" in df.columns)
    assert unixtime_name in df.columns and df.schema[unixtime_name] in [pl.Float64(), pl.Int64(), pl.Int128(), pl.Int32(), pl.Float32()]
    assert (df[unixtime_name] >= CHECK_DATETIME_MIN).sum() == (df[unixtime_name] <= CHECK_DATETIME_MAX).sum() == df.shape[0]
    assert interval % sampling_rate == 0
    if date_fr is not None:
        assert isinstance(date_fr, datetime.datetime) and date_fr.tzinfo == datetime.UTC
        assert isinstance(date_to, datetime.datetime) and date_to.tzinfo == datetime.UTC
        assert date_fr <= date_to
    assert isinstance(from_tx, bool)
    if index_names is not None:
        if isinstance(index_names, str): index_names = [index_names, ]
        assert isinstance(index_names, list) and check_type_list(index_names, str)
    else:
        index_names = []
    df = df.with_columns((pl.col(unixtime_name) // sampling_rate * sampling_rate).cast(pl.Int64).alias("timegrp"))
    LOGGER.info("END")
    return df, index_names

def check_interval(df: pl.DataFrame, unixtime_name: str="unixtime", index_names: str | list[str]=[]):
    LOGGER.info("STRAT")
    assert isinstance(unixtime_name, str)
    for x in ["sampling_rate", "interval", unixtime_name]:
        assert x in df.columns and df.schema[x] in [pl.Int32, pl.Int64, pl.Int128]
    assert df["sampling_rate"].unique().shape[0] == 1
    assert df["interval"     ].unique().shape[0] == 1
    assert df["sampling_rate"][0] == df["interval"][0]
    if index_names is not None:
        if isinstance(index_names, str): index_names = [index_names, ]
        assert isinstance(index_names, list) and check_type_list(index_names, str)
        if "timegrp" in index_names:
            index_names = [x for x in index_names if x != "timegrp"]
    else:
        index_names = []
    for x in index_names: assert x in df.columns
    if len(index_names) > 0:
        assert df.group_by(index_names).agg(pl.len().alias("__size")).select("__size").unique().shape[0] == 1 # Group size must be same
        dfwk = df.group_by(index_names).agg(pl.col(unixtime_name).shift(1)).explode(unixtime_name).with_columns((df[unixtime_name] - df["sampling_rate"]).alias("compare")) # Don't do >>> df = df.sort(index_names + ["timegrp"]). because this function is for check.
    else:
        dfwk = df.with_columns(pl.col(unixtime_name).shift(1).alias("compare"))
    assert ((dfwk[unixtime_name] == dfwk["compare"]) == False).sum() == 0 # difference between next timegrp and the timegrp is sampling_rete except null value.
    LOGGER.info("END")

def indexes_to_aggregate(df_base: pl.DataFrame, interval: int, sampling_rate: int):
    LOGGER.info("STRAT")
    assert isinstance(df_base, pl.DataFrame)
    assert isinstance(interval,      int)
    assert isinstance(sampling_rate, int)
    assert interval % sampling_rate == 0
    assert "timegrp" in df_base.columns
    if df_base.shape[1] > 1:
        sewk     = df_base.select(pl.struct(pl.col(df_base.columns[:-1])).alias("group"))["group"]
        ndf_idx1 = sewk.unique().sort().to_numpy()
        n_group  = sewk.unique().shape[0]
        ndf_tg   = df_base.filter(sewk.is_in([sewk[0]]))["timegrp"].to_numpy()
        n_tg     = ndf_tg.shape[0]
        assert (n_group * n_tg) == df_base.shape[0]
    else:
        ndf_idx1 = np.array([])
        ndf_tg   = df_base["timegrp"].to_numpy()
    index_names  = df_base.columns
    assert index_names[-1] == "timegrp"
    sewk = df_base.group_by(*df_base.columns[:-1]).map_groups(
        lambda x: x.with_columns((x["timegrp"].shift(1) + sampling_rate).alias("compare"))
    ).select((pl.col("timegrp") != pl.col("compare")).alias("compare"))["compare"]
    assert sewk.sum() == 0
    n        = interval // sampling_rate
    ndf_idx2 = np.stack([np.arange(x - n + 1, x + 1, 1, dtype=int) for x in np.arange(ndf_tg.shape[0], dtype=int)])
    ndf_idx2[ndf_idx2 < 0] = df_base.shape[0] + 1
    ndf_idx3 = np.concatenate([ndf_idx2 + (ndf_tg.shape[0] * i) for i in range(ndf_idx1.shape[0])], axis=0)
    ndf_idx3[ndf_idx3 > df_base.shape[0]] = -1
    ndf_bool = (ndf_idx3[:, 0] >= 0)
    assert ndf_bool.shape[0] == df_base.shape[0]
    LOGGER.info("END")
    return ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n

def create_ohlc(
    df: pl.DataFrame, interval: int, sampling_rate: int, date_fr: datetime.datetime, date_to: datetime.datetime,
    unixtime_name="unixtime", index_names: str | list[str]=None, from_tx: bool=False
) -> tuple[pl.DataFrame, pl.DataFrame]:
    LOGGER.info("STRAT")
    df, index_names = check_common_input(
        df, interval, sampling_rate,
        unixtime_name=unixtime_name, date_fr=date_fr, date_to=date_to, from_tx=from_tx, index_names=index_names
    )
    # create all index patterns
    ndf_tg = np.arange(
        int(date_fr.timestamp() - interval) // sampling_rate * sampling_rate,
        int(date_to.timestamp()           ) // sampling_rate * sampling_rate,
        sampling_rate, dtype=int
    )
    if len(index_names) > 0:
        ndf_idxs = [df[x].unique().to_numpy() for x in index_names]
        while len(ndf_idxs) >= 2:
            ndf_tmp  = np.concatenate([np.repeat(ndf_idxs[0], ndf_idxs[1].shape[0]).reshape(-1, 1), np.tile(ndf_idxs[1], ndf_idxs[0].shape[0]).reshape(-1, 1)], axis=-1)
            ndf_idxs = [ndf_tmp, ] + ndf_idxs[2:]
        ndf_idxs = ndf_idxs[0]
        ndf_idxs = np.concatenate([np.repeat(ndf_idxs, ndf_tg.shape[0]).reshape(-1, 1), np.tile(ndf_tg, ndf_idxs.shape[0]).reshape(-1, 1)], axis=-1)
    else:
        ndf_idxs = ndf_tg
    df_base = pl.DataFrame(ndf_idxs, schema=(index_names + ["timegrp"])).sort((index_names + ["timegrp"]))
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    # [ unixtime -> sampling_rate, sampling_rate ] open, high, low, close
    is_run = True
    if from_tx:
        assert "price" in df.columns and df.schema["price"] in [pl.Float32, pl.Float64]
        df      = df.sort(index_names + [unixtime_name, "price"])
        df_smpl = df.group_by(index_names).agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max()  .alias("high"),
            pl.col("price").min()  .alias("low"),
            pl.col("price").last() .alias("close"),
        )
    else:
        for x in ["open", "high", "low", "close"]: assert x in df.columns
        df = df.sort(index_names + [unixtime_name])
        check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
        if df["sampling_rate"][0] == sampling_rate:
            for x in index_names[:-1]:
                assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
            assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
            df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
            is_run  = False
        else:
            df_smpl = df.group_by(index_names).agg([
                pl.col("open" ).first().alias("open"),
                pl.col("high" ).max()  .alias("high"),
                pl.col("low"  ).min()  .alias("low"),
                pl.col("close").last() .alias("close"),
            ])
    if is_run:
        ### Partial common process.
        df_smpl = df_base.join(df_smpl, how="left", on=index_names)
        if len(index_names) > 1:
            df_smpl = df_smpl.group_by(*index_names[:-1], maintain_order=True).map_groups(
                lambda x: pl.DataFrame(
                    pl.Series(x.select("open", "close").to_numpy().reshape(-1)).fill_nan(None).fill_null(strategy="forward").to_numpy().reshape(-1, 2),
                    schema=["open", "close"]
                ).with_columns(x.select([y for y in x.columns if y not in ["open", "close"]]))
            )
        else:
            df_smpl = df_smpl.with_columns(pl.DataFrame(
                pl.Series(df_smpl.select("open", "close").to_numpy().reshape(-1)).fill_nan(None).fill_null(strategy="forward").to_numpy().reshape(-1, 2),
                schema=["open", "close"]
            ))
        df_smpl = df_smpl.with_columns([
            pl.max_horizontal(["open", "high", "low", "close"]).alias("high"),
            pl.min_horizontal(["open", "high", "low", "close"]).alias("low"),
        ])
    ### Copy close price to next open price.
    if len(index_names) > 1:
        df_smpl = df_smpl.group_by(*index_names[:-1]).map_groups(lambda x: x.with_columns(x["close"].shift(1).alias("_open")))
    else:
        df_smpl = df_smpl.with_columns(pl.col("open").shift(1).alias("_open"))
    df_smpl = df_smpl.with_columns(pl.when(pl.col("_open").fill_nan(None).is_null()).then("open").otherwise("_open").alias("open"))
    df_smpl = df_smpl.with_columns([
        pl.max_horizontal(["open", "high", "low", "close"]).alias("high"),
        pl.min_horizontal(["open", "high", "low", "close"]).alias("low"),
    ])
    # [ sampling_rate -> sampling_rate, interval ] open, high, low, close
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + ["open", "high", "low", "close"]).sort(index_names)
    if sampling_rate != interval:
        ndf_open  = df_smpl["open" ].to_numpy()[ndf_idx3][:, 0 ]
        ndf_close = df_smpl["close"].to_numpy()[ndf_idx3][:, -1]
        ndf_high  = np.nanmax(df_smpl["high"].to_numpy()[ndf_idx3], axis=-1)
        ndf_low   = np.nanmin(df_smpl["low" ].to_numpy()[ndf_idx3], axis=-1)
        df_itvl   = pl.DataFrame(np.stack([ndf_open, ndf_high, ndf_low, ndf_close]).T, schema=["open", "high", "low", "close"])
        df_itvl   = df_itvl.with_columns(df_base).filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    LOGGER.info("END")
    return df_itvl, df_base

def ana_size_price(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_tx: bool=False):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    # [ unixtime -> sampling_rate, sampling_rate ]
    columns_sum = [f"{a}_{b}" for a in ['size', 'volume', 'ntx'] for b in ["ask", "bid"]]
    columns_oth = ["size", "volume", "ave", "var"]
    if from_tx:
        for x in ['price', 'size', 'volume', 'side']: assert x in df.columns
        dfwk1 = df.filter(pl.col("side") == 0).group_by(index_names).agg([
            pl.len().alias("ntx_ask"),
            pl.col("size"  ).sum().alias("size_ask"),
            pl.col("volume").sum().alias("volume_ask"),
        ])
        dfwk2 = df.filter(pl.col("side") == 1).group_by(index_names).agg([
            pl.len().alias("ntx_bid"),
            pl.col("size"  ).sum().alias("size_bid"),
            pl.col("volume").sum().alias("volume_bid"),
        ])
        df_smpl = df_base.join(dfwk1.join(dfwk2, how="left", on=index_names), how="left", on=index_names)
        df_smpl = df_smpl.with_columns([
            (pl.col("volume_ask").fill_nan(0).fill_null(0) + pl.col("volume_bid").fill_nan(0).fill_null(0)).alias("volume"),
            (pl.col("size_ask"  ).fill_nan(0).fill_null(0) + pl.col("size_bid"  ).fill_nan(0).fill_null(0)).alias("size"),
        ])
        df_smpl = df_smpl.with_columns((pl.col("volume") / pl.col("size")).alias("ave"))
        dfwk    = df.select(index_names + [unixtime_name, "price", "size"]).join(df_smpl.select(index_names + ["ave"]), how="left", on=index_names)
        dfwk    = dfwk.group_by(index_names).agg((((pl.col("price") - pl.col("ave")).pow(2) * pl.col("size")).sum() / pl.col("size").sum()).alias("var"))
        df_smpl = df_smpl.join(dfwk, how="left", on=index_names)
    else:
        for x in (columns_sum + columns_oth): assert x in df.columns
        df = df.sort(index_names + [unixtime_name])
        check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
        if df["sampling_rate"][0] == sampling_rate:
            for x in index_names[:-1]: # index_names[-1] == "timegrp"
                assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
            assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
            df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
        else:
            df_smpl = df.group_by(index_names).agg([
                pl.col(x).fill_nan(0).fill_null(0).sum().alias(x) for x in columns_sum
            ])
            df_smpl = df_smpl.with_columns([
                (pl.col("size_ask"  ) + pl.col("size_bid"  )).alias("size"),
                (pl.col("volume_ask") + pl.col("volume_bid")).alias("volume"),
            ])
            df_smpl = df_smpl.with_columns((pl.col("volume") / pl.col("size")).alias("ave"))
            dfwk    = df.with_columns([
                pl.col("ave").alias("price"),
                pl.col("var").fill_nan(0).fill_null(0).alias("var"),
                (pl.col("size_ask").fill_nan(0).fill_null(0) + pl.col("size_bid").fill_nan(0).fill_null(0)).alias("size"),
            ]).select(index_names + ["price", "var", "size"])
            dfwk    = dfwk.join(df_smpl.select(index_names + ["ave"]), how="left", on=index_names)
            dfwk    = dfwk.group_by(index_names).agg((
                (((pl.col("price") - pl.col("ave")).pow(2) * pl.col("size")).sum() / pl.col("size").sum()) + 
                ((pl.col("var") * pl.col("size")).sum() / pl.col("size").sum())
            ).alias("var"))
            df_smpl = df_smpl.join(dfwk,    how="left", on=index_names)
            df_smpl = df_base.join(df_smpl, how="left", on=index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + columns_sum + columns_oth).sort(index_names)
    if sampling_rate != interval:
        df_itvl = pl.DataFrame({x: np.nansum(df_smpl[x].to_numpy()[ndf_idx3], axis=-1) for x in columns_sum})
        df_itvl = df_itvl.with_columns([
            (pl.col("size_ask"  ).fill_nan(0).fill_null(0) + pl.col("size_bid"  ).fill_nan(0).fill_null(0)).alias("size"),
            (pl.col("volume_ask").fill_nan(0).fill_null(0) + pl.col("volume_bid").fill_nan(0).fill_null(0)).alias("volume"),
        ])
        df_itvl = df_itvl.with_columns((pl.col("volume") / pl.col("size")).alias("ave"))
        ndfwk1  = np.nansum(((df_smpl["ave"].to_numpy()[ndf_idx3] - df_itvl["ave"].to_numpy().reshape(-1, 1)) ** 2) * (df_smpl["size"].to_numpy()[ndf_idx3]), axis=-1)
        ndfwk2  = np.nansum(df_smpl["size"].to_numpy()[ndf_idx3], axis=-1)
        ndfwk3  = np.nansum(df_smpl["var"].to_numpy()[ndf_idx3] * df_smpl["size"].to_numpy()[ndf_idx3], axis=-1)
        df_itvl = df_itvl.with_columns(pl.Series(ndfwk1 / ndfwk2 + ndfwk3 / ndfwk2).alias("var"))
        df_itvl = df_base.with_columns(df_itvl).filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    if len(index_names) > 1:
        df_itvl = df_itvl.group_by(*index_names[:-1]).map_groups(lambda x: x.with_columns(pl.col("ave").fill_nan(None).fill_null(strategy="forward").alias("ave")))
    else:
        df_itvl = df_itvl.with_columns(pl.col("ave").fill_nan(None).fill_null(strategy="forward").alias("ave"))
    df_itvl = df_itvl.select(index_names + columns_sum + columns_oth)
    df_itvl = df_itvl.with_columns(pl.col("var").fill_nan(0).fill_null(0).alias("var"))
    LOGGER.info("END")
    return df_itvl

def ana_quantile_tx_volume(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_tx: bool=False, n_div: int=20):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100
    # [ unixtime -> sampling_rate, sampling_rate ]
    list_qtl     = np.linspace(0.0, 1.0, n_div + 1)
    columns_base = [f"volume_q{str(int(x * 1000)).zfill(4)}" for x in list_qtl]
    if from_tx:
        for x in ["side", "volume"]: assert x in df.columns
        dfwk    = df.group_by(index_names + ["side"]).agg(
            [pl.col("volume").quantile(x, "nearest").alias(y) for x, y in zip(list_qtl, columns_base)] + 
            [pl.len().alias("ntx")]
        )
        df_smpl = df_base.join(
            dfwk.filter(pl.col("side") == 0).select([x for x in dfwk.columns if x != "side"]).rename({x: f"{x}_ask" for x in (columns_base + ["ntx"])}),
            how="left", on=index_names
        ).join(
            dfwk.filter(pl.col("side") == 1).select([x for x in dfwk.columns if x != "side"]).rename({x: f"{x}_bid" for x in (columns_base + ["ntx"])}),
            how="left", on=index_names
        )
    else:
        list_qtl_tmp     = [re.match(r"^volume_q([0-9]+)_ask$", x).groups()[0] for x in [x for x in dfwk.columns if len(re.findall(r"^volume_q[0-9]+_ask$", x)) > 0]]
        list_qtl_tmp     = list(sorted([int(x) / 1000 for x in list_qtl_tmp]))
        columns_base_tmp = [f"volume_q{str(int(x * 1000)).zfill(4)}" for x in list_qtl_tmp]
        for x in [f"{y}_{z}" for y in (columns_base_tmp + ["ntx"]) for z in ["ask", "bid"]]: assert x in dfwk.columns
        df = df.sort(index_names + [unixtime_name])
        check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
        if df["sampling_rate"][0] == sampling_rate:
            for x in index_names[:-1]: # index_names[-1] == "timegrp"
                assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
            assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
            assert columns_base_tmp == columns_base
            df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
        else:
            df_smpl = df_base.clone()
            for name in ["ask", "bid"]:
                dfwk      = df.select(index_names + [f"{x}_{name}" for x in (columns_base_tmp + ["ntx"])])
                ndf       = dfwk.select([f"{x}_{name}" for x in columns_base_tmp] + [f"ntx_{name}"]).to_numpy()
                list_fnuc = [NonLinearXY(list_qtl_tmp, x, is_ignore_nan=True)      for x in ndf[:, :-1]]
                list_tmp  = [np.linspace(0.0, 1.0, (0 if np.isnan(x) else int(x))) for x in ndf[:,  -1]]
                dfwk      = dfwk.with_columns(pl.Series([x(y) for x, y in zip(list_fnuc, list_tmp)]).alias("__volume")).explode("__volume")
                dfwkwk    = dfwk.group_by(index_names).agg([pl.col("__volume").quantile(x, "nearest").alias(y) for x, y in zip(list_qtl, columns_base)])
                df_smpl   = df_smpl.join(dfwkwk.rename({x: f"{x}_{name}" for x in (columns_base)}), how="left", on=index_names)
                df_smpl   = df_smpl.join(df.group_by(index_names).agg(pl.col(f"ntx_{name}").sum()), how="left", on=index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + [f"{x}_{y}" for x in (columns_base + ["ntx"]) for y in ["ask", "bid"]]).sort(index_names)
    if sampling_rate != interval:
        # estimate apploximately because the ntx might be super huge number.
        df_itvl = df_base.clone()
        for name in ["ask", "bid"]:
            ndf1    = df_smpl.select([f"{x}_{name}" for x in columns_base]).to_numpy()
            ndf2    = df_smpl[f"ntx_{name}"].to_numpy()
            ndf     = np.stack([np.nanquantile(np.concatenate([np.repeat(ndfwkwk, ntx // 10 + 1) for ndfwkwk, ntx in zip(ndfwk1, ndfwk2)]), list_qtl) for ndfwk1, ndfwk2 in zip(ndf1[ndf_idx3], ndf2[ndf_idx3])])
            df_itvl = df_itvl.with_columns(pl.DataFrame(ndf, schema=[f"{x}_{name}" for x in columns_base]))
            df_itvl = df_itvl.with_columns(pl.Series(np.nansum(ndf2[ndf_idx3], axis=-1)).alias(f"ntx_{name}"))
        df_itvl = df_itvl.filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + [f"{x}_{y}" for x in columns_base for y in ["ask", "bid"]])
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_price_over_time(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_tx: bool=False, n_div: int=10):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and sampling_rate % n_div == 0 and 1000 % n_div == 0
    # [ unixtime -> sampling_rate, sampling_rate ]
    list_div    = np.arange(0.0 + 1.0 / n_div / 2, 1.0, 1.0 / n_div)
    columnsv    = [f"volume_p{str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnss    = [f"size_p{  str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsa    = [f"ave_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsr    = [f"var_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    columns_all = ([f"{x}_{y}" for x in (columnsv + columnss) for y in ["ask", "bid"]] + (columnsa + columnsr))
    assert from_tx == False # Don't be created from tx
    df = df.sort(index_names + [unixtime_name])
    check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
    if df["sampling_rate"][0] == sampling_rate:
        for x in index_names[:-1]: # index_names[-1] == "timegrp"
            assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
        assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
        for x in columns_all: assert x in df.columns
        df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
    else:
        for z in ([f"{x}_{y}" for x in ["volume", "size"] for y in ["ask", "bid"]] + ["ave", "var"]): assert z in df.columns
        assert sampling_rate >= df["sampling_rate"][0]
        assert sampling_rate % df["sampling_rate"][0] == 0
        assert (sampling_rate // df["sampling_rate"][0]) % n_div == 0
        sr_tmp = int(sampling_rate // n_div)
        dfwk   = df.with_columns([
            (pl.col(unixtime_name) // sr_tmp * sr_tmp).cast(int).alias("timegrp2"),
            (pl.col("size_ask"  ).fill_nan(0).fill_null(0) + pl.col("size_bid"  ).fill_nan(0).fill_null(0)).alias("size"),
            (pl.col("volume_ask").fill_nan(0).fill_null(0) + pl.col("volume_bid").fill_nan(0).fill_null(0)).alias("volume"),
        ])
        df_tmp = dfwk.group_by(index_names + ["timegrp2"]).agg([
            pl.col("volume_ask").fill_nan(0).fill_null(0).sum(),
            pl.col("volume_bid").fill_nan(0).fill_null(0).sum(),
            pl.col("size_ask"  ).fill_nan(0).fill_null(0).sum(),
            pl.col("size_bid"  ).fill_nan(0).fill_null(0).sum(),
            (pl.col("volume").sum() / pl.col("size").sum()).alias("ave"),
        ])
        dfwk   = dfwk.rename({"ave": "price"}).join(df_tmp.select(index_names + ["timegrp2", "ave"]), how="left", on=(index_names + ["timegrp2"]))
        dfwk   = dfwk.with_columns(pl.when(pl.col("ave").fill_nan(None).is_null()).then("price").otherwise("ave").alias("ave"))
        df_tmp = df_tmp.join(
            dfwk.group_by(index_names + ["timegrp2"]).agg((
                (((pl.col("price") - pl.col("ave")).pow(2) * pl.col("size")).sum() / pl.col("size").sum()) + 
                ((pl.col("var") * pl.col("size")).sum() / pl.col("size").sum())            
            ).alias("var")), how="left", on=(index_names + ["timegrp2"])
        )
        df_tmp  = df_tmp.sort(index_names + ["timegrp2"])
        df_smpl = df_tmp.group_by(index_names, maintain_order=True).agg(
            [pl.col("volume_ask").slice(i).first().fill_nan(0).fill_null(0).alias(f"{x}_ask") for i, x in zip(range(n_div), columnsv)] + 
            [pl.col("size_ask"  ).slice(i).first().fill_nan(0).fill_null(0).alias(f"{x}_ask") for i, x in zip(range(n_div), columnss)] + 
            [pl.col("volume_bid").slice(i).first().fill_nan(0).fill_null(0).alias(f"{x}_bid") for i, x in zip(range(n_div), columnsv)] + 
            [pl.col("size_bid"  ).slice(i).first().fill_nan(0).fill_null(0).alias(f"{x}_bid") for i, x in zip(range(n_div), columnss)] + 
            [pl.col("ave"       ).slice(i).first().alias(x)                                   for i, x in zip(range(n_div), columnsa)] + 
            [pl.col("var"       ).slice(i).first().fill_nan(0).fill_null(0).alias(x)          for i, x in zip(range(n_div), columnsr)]
        )
        df_smpl = df_base.join(df_smpl, how="left", on=index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + columns_all).sort(index_names)
    if sampling_rate != interval:
        df_itvl = pl.concat([
            pl.DataFrame(np.nansum(df_smpl[[f"{x}_ask" for x in columnsv]].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=[f"{x}_ask" for x in columnsv]),
            pl.DataFrame(np.nansum(df_smpl[[f"{x}_bid" for x in columnsv]].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=[f"{x}_bid" for x in columnsv]),
            pl.DataFrame(np.nansum(df_smpl[[f"{x}_ask" for x in columnss]].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=[f"{x}_ask" for x in columnss]),
            pl.DataFrame(np.nansum(df_smpl[[f"{x}_bid" for x in columnss]].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=[f"{x}_bid" for x in columnss]),
        ], how="horizontal")
        df_itvl = df_itvl.with_columns(
            [(pl.col(f"{x}_ask").fill_nan(0).fill_null(0) + pl.col(f"{x}_bid").fill_nan(0).fill_null(0)).alias(x) for x in columnsv] + 
            [(pl.col(f"{x}_ask").fill_nan(0).fill_null(0) + pl.col(f"{x}_bid").fill_nan(0).fill_null(0)).alias(x) for x in columnss]
        )
        df_itvl = df_itvl.with_columns([(pl.col(f"{x.replace('ave', 'volume')}") / pl.col(f"{x.replace('ave', 'size')}")).alias(x) for x in columnsa])
        ndfwk1  = df_smpl[columnsa]                         .to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        ndfwk2  = df_smpl[columnsr].fill_nan(0).fill_null(0).to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        ndfwk3  = df_itvl[columnsa].to_numpy().reshape(-1, n_div, 1)
        ndfwk4  = (
            df_smpl[[f"{x}_ask" for x in columnss]].fill_nan(0).fill_null(0).to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1) + 
            df_smpl[[f"{x}_bid" for x in columnss]].fill_nan(0).fill_null(0).to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        )
        df_itvl = df_itvl.with_columns(
            pl.DataFrame(
                (np.nansum(((ndfwk1 - ndfwk3) ** 2) * ndfwk4, axis=-1) / np.nansum(ndfwk4, axis=-1)) + 
                (np.nansum(ndfwk2 * ndfwk4, axis=-1) / np.nansum(ndfwk4, axis=-1)), schema=columnsr
            )
        )
        df_itvl = df_base.with_columns(df_itvl).filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + columns_all)
    df_itvl = df_itvl.with_columns([pl.lit(interval).alias("interval"), pl.lit(sampling_rate).alias("sampling_rate")])
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_over_price(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_tx: bool=False, n_div: int=20):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and 1000 % n_div == 0


    list_div = np.linspace(0.0, 1.0, n_div + 1)
    list_ctr = (list_div[:-1] + list_div[1:]) / 2
    # [ unixtime -> sampling_rate, sampling_rate ]
    df_smpl  = df_base.copy()
    columns1 = [f"price_h{       str(int(x * 1000)).zfill(4)}" for x in list_div]
    columns2 = [f"volume_price_h{str(int(x * 1000)).zfill(4)}" for x in list_ctr]



    if from_tx:
        for x in ["side", "price", "volume"]: assert x in df.columns
        df.group_by(index_names + ["side"]).agg(pl.col("price").hist(bin_count=n_div).col("category"))
        pl.Series([1,2]).cut
        df.group_by(*(index_names + ["side"])).map_groups(lambda x: x.with_columns(pl.col("price").qcut(2).alias("category")))
        df.group_by(*(index_names + ["side"])).map_groups(lambda x: x.with_columns(pl.col("price").cut(np.linspace(x["price"].min(), x["price"].max() + 1, 11)).alias("category")))



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
    # others
    df_itvl = df_itvl.with_columns(((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low"))).alias("williams_r")) # Williams %R
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
