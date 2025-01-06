import datetime, re
import polars as pl
import numpy as np
# local package
from kklogger import set_logger
from kkpsgre.util.com import check_type_list


__all__ = [
    "create_ohlc",
    "ana_size_price",
    "ana_quantile_tx_volume",
    "ana_distribution_volume_over_time",
    "ana_distribution_volume_over_price",
    "ana_other_factor",
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

def calc_ave_var(df: pl.DataFrame, index_names: list[str]):
    LOGGER.info("STRAT")
    assert isinstance(df, pl.DataFrame)
    assert isinstance(index_names, list) and len(index_names) >= 2 and check_type_list(index_names, str)
    columns_sum = [f"{a}_{b}" for a in ["size", "volume", "ntx"] for b in ["ask", "bid"]]
    columns_oth = ["price", "var"]
    for x in (columns_sum + columns_oth): assert x in df.columns
    for x in ["ave"]: assert x not in df.columns
    df     = df.select(index_names + columns_sum + columns_oth).fill_nan(None).with_columns([
        pl.sum_horizontal([pl.col("volume_ask"), pl.col("volume_bid")]).fill_nan(0).fill_null(0).alias("volume"),
        pl.sum_horizontal([pl.col("size_ask"  ), pl.col("size_bid"  )]).fill_nan(0).fill_null(0).alias("size"  ),
        pl.col("var").fill_nan(0).fill_null(0).alias("var"),
    ])
    df_ret = df.group_by(index_names).agg([
        pl.col(x).fill_nan(0).fill_null(0).sum().alias(x) for x in columns_sum
    ])
    df_ret = df_ret.with_columns([
        pl.sum_horizontal([pl.col("volume_ask"), pl.col("volume_bid")]).alias("volume"),
        pl.sum_horizontal([pl.col("size_ask"  ), pl.col("size_bid"  )]).alias("size"  ),
    ]).with_columns((pl.col("volume") / pl.col("size")).fill_nan(None).alias("ave"))
    dfwk   = df.join(df_ret.select(index_names + ["ave"]), how="left", on=index_names)
    dfwk   = dfwk.group_by(index_names).agg([
        ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(2) * pl.col("size")).sum().alias("pow2"),
        ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(3) * pl.col("size")).sum().alias("pow3"),
        ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(4) * pl.col("size")).sum().alias("pow4"),
        (pl.col("var") * pl.col("size")).sum().alias("var"),
        pl.col("size").sum(),
    ])
    dfwk   = dfwk.with_columns(
        ((pl.col("pow2") + pl.col("var")) / pl.col("size")).alias("var")
    ).with_columns([
        (pl.col("pow3") / (pl.col("size") * pl.col("var").sqrt().pow(3))    ).alias("skewness"),
        (pl.col("pow4") / (pl.col("size") * pl.col("var").sqrt().pow(4)) - 3).alias("kurtosis"),
    ]).select(index_names + ["var", "skewness", "kurtosis"])
    df_ret = df_ret.join(dfwk, how="left", on=index_names)
    LOGGER.info("END")
    return df_ret

def ana_size_price(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_tx: bool=False):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_tx)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    # [ unixtime -> sampling_rate, sampling_rate ]
    columns_sum = [f"{a}_{b}" for a in ['size', 'volume', 'ntx'] for b in ["ask", "bid"]]
    columns_oth = ["size", "volume", "ave", "var", "skewness", "kurtosis"]
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
        dfwk    = dfwk.group_by(index_names).agg([
            ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(2) * pl.col("size")).sum().alias("pow2"),
            ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(3) * pl.col("size")).sum().alias("pow3"),
            ((pl.col("price") - pl.col("ave")).fill_nan(0).fill_null(0).pow(4) * pl.col("size")).sum().alias("pow4"),
            pl.col("size").sum(),
        ])
        dfwk    = dfwk.with_columns(
            (pl.col("pow2") / pl.col("size")).alias("var")
        ).with_columns([
            (pl.col("pow3") / (pl.col("size") * pl.col("var").sqrt().pow(3))    ).alias("skewness"),
            (pl.col("pow4") / (pl.col("size") * pl.col("var").sqrt().pow(4)) - 3).alias("kurtosis"),
        ]).select(index_names + ["var", "skewness", "kurtosis"])
        df_smpl = df_smpl.join(dfwk, how="left", on=index_names)
    else:
        for x in columns_sum: assert x in df.columns
        df = df.sort(index_names + [unixtime_name])
        check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
        if df["sampling_rate"][0] == sampling_rate:
            for x in (columns_sum + columns_oth): assert x in df.columns
            for x in index_names[:-1]: # index_names[-1] == "timegrp"
                assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
            assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
            df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
        else:
            df_smpl = calc_ave_var(df.rename({"ave": "price"}), index_names)
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
        ndfwk4  = np.nansum(((df_smpl["ave"].to_numpy()[ndf_idx3] - df_itvl["ave"].to_numpy().reshape(-1, 1)) ** 3) * (df_smpl["size"].to_numpy()[ndf_idx3]), axis=-1)
        ndfwk5  = np.nansum(((df_smpl["ave"].to_numpy()[ndf_idx3] - df_itvl["ave"].to_numpy().reshape(-1, 1)) ** 4) * (df_smpl["size"].to_numpy()[ndf_idx3]), axis=-1)
        ndfwk6  = (ndfwk1 + ndfwk3) / ndfwk2
        df_itvl = df_itvl.with_columns(pl.Series(ndfwk1 / ndfwk2 + ndfwk3 / ndfwk2).alias("var"))
        df_itvl = df_itvl.with_columns([
            pl.Series(ndfwk6).alias("var"),
            pl.Series(ndfwk4 / (ndfwk2 * (ndfwk6 ** 3))    ).alias("skewness"),
            pl.Series(ndfwk5 / (ndfwk2 * (ndfwk6 ** 4)) - 3).alias("kurtosis"),
        ])
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
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and 1000 % (n_div * 2) == 0
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
        list_qtl_tmp     = [re.match(r"^volume_q([0-9]+)_ask$", x).groups()[0] for x in [x for x in df.columns if len(re.findall(r"^volume_q[0-9]+_ask$", x)) > 0]]
        list_qtl_tmp     = list(sorted([int(x) / 1000 for x in list_qtl_tmp]))
        columns_base_tmp = [f"volume_q{str(int(x * 1000)).zfill(4)}" for x in list_qtl_tmp]
        n_tmp            = (len(list_qtl_tmp) - 1)
        assert 1000 % (n_tmp * 2) == 0
        assert columns_base_tmp == [f"volume_q{str(int(x * 1000)).zfill(4)}" for x in np.linspace(0.0, 1.0, n_tmp + 1)]
        for x in [f"{y}_{z}" for y in (columns_base_tmp + ["ntx"]) for z in ["ask", "bid"]]: assert x in df.columns
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
            # estimate apploximately because the ntx might be super huge number.
            for name in ["ask", "bid"]:
                ndfwk1 = df.select([f"{x}_{name}" for x in columns_base_tmp]).to_numpy()
                ndfwk2 = df[f"ntx_{name}"].fill_nan(0).fill_null(0).to_numpy()
                ndfwk3 = [(np.tile(x, (y // (n_tmp + 1)) + 1)[:y] if y >= (n_tmp + 1) else np.unique(x)) for x, y in zip(ndfwk1, ndfwk2)]
                dfwk   = df.with_columns(pl.Series([x[~np.isnan(x)] for x in ndfwk3]).alias("tmp"))
                dfwk   = dfwk.explode("tmp")
                dfwk   = dfwk.group_by(index_names).agg(
                    [pl.col("tmp").quantile(x, "nearest").alias(f"{y}_{name}") for x, y in zip(list_qtl, columns_base)] + 
                    [pl.col(f"ntx_{name}").sum()]
                )
                df_smpl = df_smpl.join(dfwk, how="left", on=index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + [f"{x}_{y}" for x in (columns_base + ["ntx"]) for y in ["ask", "bid"]]).sort(index_names)
    if sampling_rate != interval:
        # estimate apploximately because the ntx might be super huge number.
        df_itvl = df_base.clone()
        for name in ["ask", "bid"]:
            ndf1    = np.nan_to_num(df_smpl.select([f"{x}_{name}" for x in columns_base]).to_numpy(), 0)
            ndf2    = np.nan_to_num(df_smpl[f"ntx_{name}"].to_numpy(), 0)
            listwk  = [
                np.nanquantile(np.concatenate([np.repeat(ndfwkwk, ntx // n_div + 1) for ndfwkwk, ntx in zip(ndfwk1, ndfwk2)]), list_qtl)
                for ndfwk1, ndfwk2 in zip(ndf1[ndf_idx3], ndf2[ndf_idx3])
            ]
            ndfwk   = np.full(list_qtl.shape[0], float("nan"))
            ndf     = np.stack([(ndfwk if isinstance(x, float) and np.isnan(float("nan")) else x) for x in listwk])
            df_itvl = df_itvl.with_columns(pl.DataFrame(ndf, schema=[f"{x}_{name}" for x in columns_base]))
            df_itvl = df_itvl.with_columns(pl.Series(np.nansum(ndf2[ndf_idx3], axis=-1)).alias(f"ntx_{name}"))
        df_itvl = df_itvl.filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + [f"{x}_{y}" for x in columns_base for y in ["ask", "bid"]])
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_price_over_time(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", n_div: int=10):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=False)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and sampling_rate % n_div == 0 and 1000 % (n_div * 2) == 0
    # [ unixtime -> sampling_rate, sampling_rate ]
    list_div    = np.arange(0.0 + 1.0 / n_div / 2, 1.0, 1.0 / n_div)
    columnsv    = [f"volume_p{str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnss    = [f"size_p{  str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsa    = [f"ave_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsr    = [f"var_p{   str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsb    = [f"bband_p{ str(int(x * 1000)).zfill(4)}" for x in list_div]
    columnsc    = [f"autocorrelation_0{i}" for i in range(1, 6)] + ["autocorrelation_mean"]
    columns_all = columnsv + columnss + columnsa + columnsr + columnsb + columnsc
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
        assert (sampling_rate // df["sampling_rate"][0]) % 10 == 0
        assert (sampling_rate // df["sampling_rate"][0]) % n_div == 0
        sr_tmp  = int(sampling_rate // n_div)
        dfwk    = df.with_columns([
            (pl.col(unixtime_name) // sr_tmp * sr_tmp).cast(int).alias("timegrp2"),
        ])
        df_tmp  = calc_ave_var(dfwk.rename({"ave": "price"}), index_names + ["timegrp2"])
        df_tmp  = df_tmp.sort(index_names + ["timegrp2"])
        df_smpl = df_tmp.group_by(index_names, maintain_order=True).agg(
            [pl.col("volume").slice(i).first().fill_nan(0).fill_null(0).alias(x) for i, x in zip(range(n_div), columnsv)] + 
            [pl.col("size"  ).slice(i).first().fill_nan(0).fill_null(0).alias(x) for i, x in zip(range(n_div), columnss)] + 
            [pl.col("ave"   ).slice(i).first().alias(x)                          for i, x in zip(range(n_div), columnsa)] + 
            [pl.col("var"   ).slice(i).first().fill_nan(0).fill_null(0).alias(x) for i, x in zip(range(n_div), columnsr)]
        )
        df_smpl = df_smpl.join(
            df.group_by(index_names).agg([
                pl.col(unixtime_name),
                pl.col("ave").fill_nan(None).fill_null(strategy="forward")
            ]).explode([unixtime_name, "ave"]).sort(index_names + [unixtime_name]).group_by(index_names).agg([
                pl.corr(pl.col("ave"), pl.col("ave").shift(((pl.len() / 10) * 1).cast(pl.Int64))).fill_nan(None).alias("autocorrelation_01"),
                pl.corr(pl.col("ave"), pl.col("ave").shift(((pl.len() / 10) * 2).cast(pl.Int64))).fill_nan(None).alias("autocorrelation_02"),
                pl.corr(pl.col("ave"), pl.col("ave").shift(((pl.len() / 10) * 3).cast(pl.Int64))).fill_nan(None).alias("autocorrelation_03"),
                pl.corr(pl.col("ave"), pl.col("ave").shift(((pl.len() / 10) * 4).cast(pl.Int64))).fill_nan(None).alias("autocorrelation_04"),
                pl.corr(pl.col("ave"), pl.col("ave").shift(((pl.len() / 10) * 5).cast(pl.Int64))).fill_nan(None).alias("autocorrelation_05"),
            ]), how="left", on=index_names
        ).with_columns([
            pl.mean_horizontal([f"autocorrelation_0{i}" for i in range(1, 6)]).alias("autocorrelation_mean")
        ])
        dfwk    = calc_ave_var(df.rename({"ave": "price"}), index_names)
        df_smpl = df_smpl.join(dfwk.select(index_names + ["ave", "var"]), how="left", on=index_names)
        df_smpl = df_smpl.with_columns([
            ((pl.col(x) - pl.col("ave")) / pl.col("var").sqrt()).alias(y) for x, y in zip(columnsa, columnsb)
        ])
        df_smpl = df_base.join(df_smpl, how="left", on=index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + columns_all).sort(index_names)
    if sampling_rate != interval:
        df_itvl = pl.concat([
            pl.DataFrame(np.nansum(df_smpl[columnsv].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=columnsv),
            pl.DataFrame(np.nansum(df_smpl[columnss].to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1), axis=-1), schema=columnss),
        ], how="horizontal")
        df_itvl = df_itvl.with_columns([(pl.col(f"{x.replace('ave', 'volume')}") / pl.col(f"{x.replace('ave', 'size')}")).alias(x) for x in columnsa])
        ndfwk0  = df_smpl[columnsa].to_numpy()[ndf_idx3].reshape(df_base.shape[0], -1)
        ndfwk0  = pl.DataFrame(ndfwk0).select(pl.concat_list(pl.all()).alias("tmp")) \
                    .with_columns(pl.arange(pl.len()).alias("no")).explode("tmp") \
                    .group_by("no").agg(pl.col("tmp").fill_nan(None).fill_null(strategy="forward")) \
                    .select([pl.col("tmp").list.get(i).alias(f"{i}") for i in range(ndfwk0.shape[-1])]).to_numpy()
        df_itvl = df_itvl.with_columns([
            pl.Series(
                [np.corrcoef(x[bx & by], y[bx & by])[0][1] for x, y, bx, by in zip(ndfwk0[:, j:], ndfwk0[:, :-j], ~np.isnan(ndfwk0[:, j:]), ~np.isnan(ndfwk0[:, :-j]))]
            ).alias(f"autocorrelation_0{k}") for j, k in zip([int((ndfwk0.shape[-1] / 10) * i) for i in range(1, 6)], range(1, 6))
        ]).with_columns([
            pl.mean_horizontal([f"autocorrelation_0{i}" for i in range(1, 6)]).alias("autocorrelation_mean")
        ])
        ndfwk1  = df_smpl[columnsa]                         .to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        ndfwk2  = df_smpl[columnsr].fill_nan(0).fill_null(0).to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        ndfwk3  = df_itvl[columnsa].to_numpy().reshape(-1, n_div, 1)
        ndfwk4  = df_smpl[columnss].fill_nan(0).fill_null(0).to_numpy()[ndf_idx3].reshape(df_base.shape[0], n_div, -1)
        df_itvl = df_itvl.with_columns(
            pl.DataFrame(
                (np.nansum(((ndfwk1 - ndfwk3) ** 2) * ndfwk4, axis=-1) / np.nansum(ndfwk4, axis=-1)) + 
                (np.nansum(ndfwk2 * ndfwk4, axis=-1) / np.nansum(ndfwk4, axis=-1)), schema=columnsr
            )
        )
        ndfwk5  = df_itvl[columnsa].to_numpy()
        ndfwk6  = df_itvl[columnss].to_numpy()
        ndfwk7  = df_itvl[columnsr].to_numpy()
        ndfwk8  = np.nansum(ndfwk5 * ndfwk6, axis=-1) / np.nansum(ndfwk6, axis=-1)
        ndfwk9  = np.sqrt(np.nansum((((ndfwk5 - ndfwk8.reshape(-1, 1)) ** 2) * ndfwk6) + (ndfwk7 * ndfwk6), axis=-1) / np.nansum(ndfwk6, axis=-1))
        df_itvl = df_itvl.with_columns(
            pl.DataFrame((ndfwk5 - ndfwk8.reshape(-1, 1)) / ndfwk9.reshape(-1, 1), schema=columnsb)
        )
        df_itvl = df_base.with_columns(df_itvl).filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + columns_all)
    LOGGER.info("END")
    return df_itvl

def ana_distribution_volume_over_price(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime", from_small_sr: bool=False, n_div: int=20):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=from_small_sr)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    assert isinstance(n_div, int) and n_div > 1 and n_div <= 100 and 1000 % (n_div * 2) == 0
    def func_lst(x: int, xmin: float=0.0, xmax: float=1.0):
        return ((np.linspace(xmin, xmax, x + 1)[:-1] + np.linspace(xmin, xmax, x + 1)[1:]) / 2)
    list_ctr = func_lst(n_div)
    columns  = [f"volume_price_h{str(int(x * 1000)).zfill(4)}" for x in list_ctr]
    columnsv = ([f"{x}_ask" for x in columns] + [f"{x}_bid" for x in columns])
    columnsp = ["price_h0000", "price_h1000"]
    # [ sampling_rate ( small ) -> sampling_rate, sampling_rate ]
    df = df.sort(index_names + [unixtime_name])
    check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
    if from_small_sr:
        # This process is not for data from tx. it can be applied one from ohlc which is like with sampling_rate = 3
        assert df["sampling_rate"][0] <= 6
        for x in ["ave", "volume_ask", "volume_bid"]: assert x in df.columns
        dfwk = df.filter(pl.col("ave").fill_nan(None).is_not_null()).with_columns([
            pl.col("volume_ask").fill_nan(0).fill_null(0),
            pl.col("volume_bid").fill_nan(0).fill_null(0),
        ])
        dfwk = dfwk.group_by(index_names).agg([
            pl.col("ave"), pl.col("ave").min().alias("price_h0000"), pl.col("ave").max().alias("price_h1000"),
            pl.col("volume_ask"), pl.col("volume_bid"),

        ])
        df_smpl = df_base.join(dfwk.select(index_names + columnsp), how="left", on=index_names)
        dfwk    = dfwk.with_columns(pl.DataFrame(
            [
                np.concatenate([np.histogram(a, n_div, range=(an, ax), weights=va)[0], np.histogram(a, n_div, range=(an, ax), weights=vb)[0]])
                for a, va, vb, an, ax in dfwk.select(["ave", "volume_ask", "volume_bid", "price_h0000", "price_h1000"]).iter_rows()
            ], schema=(columnsv), orient="row"
        )).select(index_names + columnsv)
        df_smpl = df_smpl.join(dfwk, how="left", on=index_names)
        df_smpl = df_smpl.with_columns(pl.col(columnsv).fill_nan(0).fill_null(0))
    else:
        list_ctr_tmp     = [re.match(r"^volume_price_h([0-9]+)_ask$", x).groups()[0] for x in [x for x in df.columns if len(re.findall(r"^volume_price_h[0-9]+_ask$", x)) > 0]]
        list_ctr_tmp     = list(sorted([int(x) / 1000 for x in list_ctr_tmp]))
        columns_base_tmp = [f"volume_price_h{str(int(x * 1000)).zfill(4)}" for x in list_ctr_tmp]
        n_tmp            = len(list_ctr_tmp)
        assert 1000 % (n_tmp * 2) == 0
        assert columns_base_tmp == [f"volume_price_h{str(int(x * 1000)).zfill(4)}" for x in func_lst(n_tmp)]
        for x in [f"{y}_{z}" for y in columns_base_tmp for z in ["ask", "bid"]]: assert x in df.columns
        if df["sampling_rate"][0] == sampling_rate:
            for x in index_names[:-1]: # index_names[-1] == "timegrp"
                assert (df[x].unique().is_in(df_base[x].unique()) == False).sum() == 0
            assert (df_base["timegrp"].unique().is_in(df["timegrp"].unique()) == False).sum() == 0
            for x in (columnsv + columnsp): assert x in df.columns
            df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
        else:
            dfwk = df.with_columns([
                pl.concat_list([f"{x}_ask" for x in columns_base_tmp]).alias("volume_ask"),
                pl.concat_list([f"{x}_bid" for x in columns_base_tmp]).alias("volume_bid"),
                pl.concat_list(columnsp).map_elements(lambda x: pl.Series(func_lst(n_tmp, x[0], x[1])), return_dtype=pl.List(pl.Float64)).alias("ave"),
            ])
            dfwk = dfwk.explode(["ave", "volume_ask", "volume_bid"])
            dfwk = dfwk.group_by(index_names).agg([
                pl.col("ave"), pl.col("price_h0000").min().alias("price_h0000"), pl.col("price_h1000").max().alias("price_h1000"),
                pl.col("volume_ask"), pl.col("volume_bid"),
            ])
            df_smpl = df_base.join(dfwk.select(index_names + ["price_h0000", "price_h1000"]), how="left", on=index_names)
            dfwk    = dfwk.with_columns(pl.DataFrame(
                [
                    np.concatenate([np.histogram(a, n_div, range=(an, ax), weights=va)[0], np.histogram(a, n_div, range=(an, ax), weights=vb)[0]])
                    for a, va, vb, an, ax in dfwk.select(["ave", "volume_ask", "volume_bid", "price_h0000", "price_h1000"]).iter_rows()
                ], schema=(columnsv), orient="row"
            )).select(index_names + columnsv)
            df_smpl = df_smpl.join(dfwk, how="left", on=index_names)
            df_smpl = df_smpl.with_columns(pl.col(columnsv).fill_nan(0).fill_null(0))
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).select(index_names + columnsp + columnsv).sort(index_names)
    if sampling_rate != interval:
        ndfwk1  = df_smpl.select([f"{x}_ask" for x in columns]).fill_nan(0).fill_null(0).to_numpy()
        ndfwk2  = df_smpl.select([f"{x}_bid" for x in columns]).fill_nan(0).fill_null(0).to_numpy()
        dfwk    = df_smpl.select(pl.concat_list(pl.col(columnsp).fill_null(float("nan"))).map_elements(lambda x: pl.Series(func_lst(n_div, x[0], x[1])), return_dtype=pl.List(pl.Float64)).alias("tmp"))
        ndfwk3  = dfwk.select([pl.col("tmp").list.get(i).alias(f"col{i}") for i in range(n_div)]).to_numpy()
        ndfwk4  = df_smpl[columnsp].to_numpy()
        ndfmin  = np.nanmin(ndfwk4[ndf_idx3].reshape(df_base.shape[0], -1), axis=-1)
        ndfmax  = np.nanmax(ndfwk4[ndf_idx3].reshape(df_base.shape[0], -1), axis=-1)
        ndfask  = np.stack([np.histogram(p, n_div, range=((pmin, pmax) if np.isnan([pmin, pmax]).sum() == 0 else (0, 0)), weights=v)[0] for pmin, pmax, p, v in zip(ndfmin, ndfmax, ndfwk3[ndf_idx3].reshape(df_base.shape[0], -1), ndfwk1[ndf_idx3].reshape(df_base.shape[0], -1))])
        ndfbid  = np.stack([np.histogram(p, n_div, range=((pmin, pmax) if np.isnan([pmin, pmax]).sum() == 0 else (0, 0)), weights=v)[0] for pmin, pmax, p, v in zip(ndfmin, ndfmax, ndfwk3[ndf_idx3].reshape(df_base.shape[0], -1), ndfwk2[ndf_idx3].reshape(df_base.shape[0], -1))])
        df_itvl = df_base.with_columns(pl.DataFrame(np.concatenate([ndfask, ndfbid], axis=-1), schema=columnsv))
        df_itvl = df_itvl.with_columns([
            pl.Series(ndfmin).alias("price_h0000"), pl.Series(ndfmax).alias("price_h1000"), 
        ])
        df_itvl = df_itvl.filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + columnsp + columnsv)
    LOGGER.info("END")
    return df_itvl

def ana_other_factor(df: pl.DataFrame, interval: int, sampling_rate: int, df_base: pl.DataFrame, unixtime_name: str="unixtime"):
    LOGGER.info("STRAT")
    df, _ = check_common_input(df, interval, sampling_rate, unixtime_name=unixtime_name, from_tx=False)
    ndf_idx1, ndf_idx2, ndf_idx3, ndf_bool, ndf_tg, index_names, n = indexes_to_aggregate(df_base, interval, sampling_rate)
    columns_base = ["open", "high", "low", "close", "ave", "size_ask", "size_bid", "volume_ask", "volume_bid"]
    columns = [
        'ratio_volume', 'ratio_size', 'range', 'williams_r',
        'rci', 'rci_volume_ask', 'rci_volume_bid', 'rsi_mean_up', 'rsi_mean_down', 'rsi',
        'slope', 'sum_abs_diff', 'mean_abs_diff', 'mean_diff', 'cid_ce_norm',
        'n_above_ave', 'n_below_ave', 'is_above_first', 'n_cross_ave', 'n_cross_hl',
        'n_up', 'n_down', 'n_change', 'position_max', 'position_min',
    ]
    # [ sampling_rate ( small ) -> sampling_rate, sampling_rate ]
    df = df.sort(index_names + [unixtime_name]).fill_nan(None)
    check_interval(df, unixtime_name=unixtime_name, index_names=index_names)
    for x in columns_base: assert x in df.columns
    if df["sampling_rate"][0] == sampling_rate and sampling_rate != interval:
        df_smpl = df.filter(pl.col("timegrp").is_in(ndf_tg))
    else:
        df_smpl = df_base.clone()
        ## OHLC
        df_smpl = df_smpl.join(
            df.group_by(index_names).agg([
                pl.col("open" ).first(),
                pl.col("high" ).max(),
                pl.col("low"  ).min(),
                pl.col("close").last(),
                pl.col("volume_ask").fill_nan(None).sum(),
                pl.col("volume_bid").fill_nan(None).sum(),
                pl.col("size_ask"  ).fill_nan(None).sum(),
                pl.col("size_bid"  ).fill_nan(None).sum(),
            ]), how="left", on=index_names
        ).with_columns([
            (pl.col("volume_ask") / pl.col("volume_bid")).alias("ratio_volume"),
            (pl.col("size_ask"  ) / pl.col("size_bid"  )).alias("ratio_size"  ),
            (pl.col("high") - pl.col("low")).alias("range"),
            ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low"))).alias("williams_r"),
        ])
        df_smpl = df_smpl.join(
            df_smpl.with_columns(
                (
                    (pl.col("volume_ask").fill_nan(None) + pl.col("volume_bid").fill_nan(None)) / 
                    (pl.col("size_ask"  ).fill_nan(None) + pl.col("size_bid"  ).fill_nan(None))
                ).alias("ave")
            ).sort(index_names).group_by(index_names[:-1]).agg([
                pl.col("timegrp"),
                pl.col("ave").fill_nan(None).fill_null(strategy="forward").alias("ave"),
            ]).explode(["timegrp", "ave"]), how="left", on=index_names
        )
        ## RCI
        dfwk    = df.filter(pl.col("ave").fill_nan(None).is_not_null()).group_by(index_names).agg(pl.col("ave"))
        df_smpl = df_smpl.join(
            dfwk.with_columns(
                pl.col("ave").map_elements(
                    lambda x: 1 - (
                        (6 * ((np.arange(1, x.shape[0] + 1)[::-1] - (np.argsort(np.argsort(-1 * x.to_numpy())) + 1)) ** 2).sum()) / 
                        (x.shape[0] * (x.shape[0] ** 2 - 1))
                    ), return_dtype=pl.Float64
                ).alias("rci")
            ).select(index_names + ["rci"]), how="left", on=index_names
        )
        ## volume rank corr
        dfwk    = df.group_by(index_names).agg([
            pl.col("volume_ask").fill_nan(0).fill_null(0),
            pl.col("volume_bid").fill_nan(0).fill_null(0),
        ])
        df_smpl = df_smpl.join(
            dfwk.with_columns([
                pl.col("volume_ask").map_elements(
                    lambda x: 1 - (
                        (6 * ((np.arange(1, x.shape[0] + 1)[::-1] - (np.argsort(np.argsort(-1 * x.to_numpy())) + 1)) ** 2).sum()) /
                        (x.shape[0] * (x.shape[0] ** 2 - 1))
                    ), return_dtype=pl.Float64
                ).alias("rci_volume_ask"),
                pl.col("volume_bid").map_elements(
                    lambda x: 1 - (
                        (6 * ((np.arange(1, x.shape[0] + 1)[::-1] - (np.argsort(np.argsort(-1 * x.to_numpy())) + 1)) ** 2).sum()) /
                        (x.shape[0] * (x.shape[0] ** 2 - 1))
                    ), return_dtype=pl.Float64
                ).alias("rci_volume_bid"),
            ]).select(index_names + ["rci_volume_ask", "rci_volume_bid"]), how="left", on=index_names
        )
        ## RSI
        dfwk    = df.group_by(index_names).agg((pl.col("close") - pl.col("open")).alias("tmp")).explode("tmp")
        df_smpl = df_smpl.join(dfwk.filter(pl.col("tmp") > 0 ).group_by(index_names).agg(pl.col("tmp").mean().abs().alias("rsi_mean_up"  )), how="left", on=index_names)
        df_smpl = df_smpl.join(dfwk.filter(pl.col("tmp") < 0 ).group_by(index_names).agg(pl.col("tmp").mean().abs().alias("rsi_mean_down")), how="left", on=index_names)
        df_smpl = df_smpl.with_columns((
            pl.col("rsi_mean_up").fill_nan(0).fill_null(0) / (pl.col("rsi_mean_up").fill_nan(0).fill_null(0) + pl.col("rsi_mean_down").fill_nan(0).fill_null(0))
        ).alias("rsi"))
        ## signal analysis, autocorrelation
        dfwk = df.group_by(index_names).agg([
            pl.col(unixtime_name),
            pl.col("high").fill_nan(None),
            pl.col("low").fill_nan(None),
            pl.col("ave").fill_nan(None).alias("price"),
            pl.col("ave").fill_nan(None).mean().alias("ave"),
            pl.col("ave").fill_nan(None).var().alias("var"),
            (pl.col("ave") - pl.col("ave").shift(1)).alias("diff"),
            (pl.col("ave") > pl.col("ave").shift(1)).alias("is_up"),
            (pl.col("ave") < pl.col("ave").shift(1)).alias("is_down"),
        ]).explode([unixtime_name, "high", "low", "price", "diff", "is_up", "is_down"]).sort(index_names + [unixtime_name]).with_columns([
            (pl.col("price") > pl.col("ave")).alias("is_above"),
            (pl.col("price") < pl.col("ave")).alias("is_below"),
        ]).filter(pl.col("var") > 0).with_columns([
            pl.when((pl.col("is_up") == False) & (pl.col("is_down") == False)).then(None).otherwise("is_up"  ).alias("is_up"  ),
            pl.when((pl.col("is_up") == False) & (pl.col("is_down") == False)).then(None).otherwise("is_down").alias("is_down"),
            ((pl.col("price") - pl.col("ave")) / pl.col("var")).alias("z_score"),
        ])
        df_smpl = df_smpl.join(
            dfwk.group_by(index_names).agg([
                (
                    ((pl.col(unixtime_name) - pl.col(unixtime_name).mean()) * (pl.col("price") - pl.col("price").mean())).sum() / 
                    (pl.col(unixtime_name) - pl.col(unixtime_name).mean()).pow(2).sum()
                ).alias("slope"),
                pl.col("diff").fill_nan(None).abs().sum() .alias("sum_abs_diff"),
                pl.col("diff").fill_nan(None).abs().mean().alias("mean_abs_diff"),
                pl.col("diff").fill_nan(None).mean()      .alias("mean_diff"),
                ((pl.col("z_score") - pl.col("z_score").shift(1)).pow(2).sum() / (pl.len() - 1)).sqrt().alias("cid_ce_norm"),
                (pl.col("is_above").sum() / pl.len()).alias("n_above_ave"),
                (pl.col("is_below").sum() / pl.len()).alias("n_below_ave"),
                pl.col("is_above").first().alias("is_above_first"),
                ((pl.col("is_above") != pl.col("is_above").shift(1)).sum() / (pl.len() - 1)).alias("n_cross_ave"),
                (((pl.col("high") > pl.col("ave")) & (pl.col("low") < pl.col("ave"))).sum() / pl.len()).alias("n_cross_hl"),
                (pl.col("is_up"  ).sum() / pl.len()).alias("n_up"),
                (pl.col("is_down").sum() / pl.len()).alias("n_down"),
                ((pl.col("is_up") != pl.col("is_up").shift(1)).sum() / (pl.len() - 1)).alias("n_change"),
                (pl.col("price").arg_max() / pl.len()).alias("position_max"),
                (pl.col("price").arg_min() / pl.len()).alias("position_min"),
            ]), how="left", on=index_names
        )
        df_smpl = df_smpl.select(index_names + columns_base + columns).sort(index_names)
    # [ sampling_rate -> sampling_rate, interval ]
    df_smpl = df_smpl.filter(pl.col("timegrp").is_in(ndf_tg)).sort(index_names)
    if sampling_rate != interval:
        ndfo    = df_smpl["open" ].to_numpy()[ndf_idx3][:, 0]
        ndfh    = np.nanmax(df_smpl["high"].to_numpy()[ndf_idx3], axis=-1)
        ndfl    = np.nanmin(df_smpl["low" ].to_numpy()[ndf_idx3], axis=-1)
        ndfc    = df_smpl["close"].to_numpy()[ndf_idx3][:, -1]
        ndfa    = df_smpl["ave"].to_numpy()[ndf_idx3]
        ndfsa   = df_smpl["size_ask"  ].to_numpy()[ndf_idx3]
        ndfsb   = df_smpl["size_bid"  ].to_numpy()[ndf_idx3]
        ndfva   = df_smpl["volume_ask"].to_numpy()[ndf_idx3]
        ndfvb   = df_smpl["volume_bid"].to_numpy()[ndf_idx3]
        df_itvl = df_base.with_columns([
            pl.Series(ndfh - ndfl).alias("range"),
            pl.Series((ndfc - ndfl) / (ndfh - ndfl)).alias("williams_r"),
            pl.Series(np.nansum(ndfsa, axis=-1) / np.nansum(ndfsb, axis=-1)).fill_nan(None).alias("ratio_size"),
            pl.Series(np.nansum(ndfva, axis=-1) / np.nansum(ndfvb, axis=-1)).fill_nan(None).alias("ratio_volume"),
            pl.Series(1 - (6 * ((np.tile(np.arange(1, n+1)[::-1], (df_base.shape[0], 1)) - (np.argsort(np.argsort(-ndfa,  axis=-1), axis=-1) + 1)) ** 2).sum(axis=-1) / (n * (n ** 2 - 1)))).alias("rci"),
            pl.Series(1 - (6 * ((np.tile(np.arange(1, n+1)[::-1], (df_base.shape[0], 1)) - (np.argsort(np.argsort(-ndfva, axis=-1), axis=-1) + 1)) ** 2).sum(axis=-1) / (n * (n ** 2 - 1)))).alias("rci_volume_ask"),
            pl.Series(1 - (6 * ((np.tile(np.arange(1, n+1)[::-1], (df_base.shape[0], 1)) - (np.argsort(np.argsort(-ndfvb, axis=-1), axis=-1) + 1)) ** 2).sum(axis=-1) / (n * (n ** 2 - 1)))).alias("rci_volume_bid"),
        ])
        ndfo    = df_smpl["open" ].to_numpy()[ndf_idx3]
        ndfc    = df_smpl["close"].to_numpy()[ndf_idx3]
        ndfwk   = (ndfc - ndfo).copy()
        ndfwk[ndfwk <= 0] = float("nan")
        ndfup   = np.nanmean(ndfwk, axis=-1)
        ndfwk   = (ndfc - ndfo).copy()
        ndfwk[ndfwk >= 0] = float("nan")
        ndfdown = np.abs(np.nanmean(ndfwk, axis=-1))
        df_itvl = df_itvl.with_columns([
            pl.Series(ndfup  ).alias("rsi_mean_up"),
            pl.Series(ndfdown).alias("rsi_mean_down"),
            pl.Series(np.nan_to_num(ndfup, 0) / np.nansum(np.stack([ndfup, ndfdown]), axis=0)).alias("rsi"),
        ])
        ndfp    = ndfa.copy()
        ndfa    = np.nanmean(ndfp, axis=-1)
        ndfv    = np.nanvar( ndfp, axis=-1)
        ndfz    = (ndfp - ndfa.reshape(-1, 1)) / ndfv.reshape(-1, 1)
        ndfd    = ndfp[:, 1:] - ndfp[:, :-1]
        ndft    = df_smpl["timegrp"].to_numpy()[ndf_idx3]
        ndfta   = np.nanmean(ndft, axis=-1)
        ndfb    = (ndfp > ndfa.reshape(-1, 1))
        ndfh    = df_smpl["high"].to_numpy()[ndf_idx3]
        ndfl    = df_smpl["low" ].to_numpy()[ndf_idx3]
        ndfup   = (ndfp[:, 1:] > ndfp[:, :-1])
        ndfdw   = (ndfp[:, 1:] < ndfp[:, :-1])
        ndfwk   = (ndfup == False) & (ndfdw == False)
        ndfup2  = ndfup.copy().astype(float)
        ndfup2[ndfwk] = float("nan")
        df_itvl = df_itvl.with_columns([
            pl.Series(np.nansum((ndft - ndfta.reshape(-1, 1)) * (ndfp - ndfa.reshape(-1, 1)), axis=-1) / np.nansum((ndft - ndfta.reshape(-1, 1)) ** 2, axis=-1)).alias("slope"),
            pl.Series(np.nansum( np.abs(ndfd), axis=-1)).alias("sum_abs_diff"),
            pl.Series(np.nanmean(np.abs(ndfd), axis=-1)).alias("mean_abs_diff"),
            pl.Series(np.nanmean(       ndfd,  axis=-1)).alias("mean_diff"),
            pl.Series(np.sqrt(np.nansum((ndfz[:, 1:] - ndfz[:, :-1]) ** 2, axis=-1) / (n - 1))).alias("cid_ce_norm"),
            pl.Series((ndfp > ndfa.reshape(-1, 1)).sum(axis=-1) / n).alias("n_above_ave"),
            pl.Series((ndfp < ndfa.reshape(-1, 1)).sum(axis=-1) / n).alias("n_below_ave"),
            pl.Series(ndfb[:, 0]).alias("is_above_first"),
            pl.Series((ndfb[:, 1:] != ndfb[:, :-1]).sum(axis=-1) / (n - 1)).alias("n_cross_ave"),
            pl.Series(((ndfh > ndfa.reshape(-1, 1)) & (ndfl < ndfa.reshape(-1, 1))).sum(axis=-1) / n).alias("n_cross_hl"),
            pl.Series(ndfup.sum(axis=-1) / n).alias("n_up"),
            pl.Series(ndfdw.sum(axis=-1) / n).alias("n_down"),
            pl.Series(np.nansum((ndfup2[:, 1:] + ndfup2[:, :-1]) == 1, axis=-1) / (n - 1)).alias("n_change"), # nan == nan is True. so 1 + 0 or 0 + 1 == 1
            pl.Series(np.argmax(np.nan_to_num(ndfp, -1), axis=-1) / n).alias("position_max"),
            pl.Series(np.argmin(np.nan_to_num(ndfp, -1), axis=-1) / n).alias("position_min"),
        ])
        df_itvl = df_itvl.filter(ndf_bool)
    else:
        df_itvl = df_smpl.clone()
    df_itvl = df_itvl.select(index_names + columns)
    LOGGER.info("END")
    return df_itvl
