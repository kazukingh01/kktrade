import re
import pandas as pd
import numpy as np
from typing import List, Union
from joblib import Parallel, delayed
from functools import partial

# local package
from kktrade.util.com import check_str_is_integer, check_type_list


__all__ = [
    "parallel_apply",
    "drop_duplicate_columns",
    "apply_fill_missing_values",
    "check_column_is_integer",
    "check_column_is_float",
    "to_string_all_columns",
    "is_column_string",
    "is_columns_string",
    "compare_strings_faster",
    "query",
    "astype_faster",
]


def parallel_apply(df: pd.DataFrame, func, axis: int=0, group_key=None, func_aft=None, batch_size: int=1, n_jobs: int=1):
    """
    pandarallel is slow in some cases. It is twice as fast to use pandas.
    Params::
        func:
            ex) lambda x: x.rank()
        axis:
            axis=0: df.apply(..., axis=0)
            axis=1: df.apply(..., axis=1)
            axis=2: df.groupby(...)
        func_aft:
            input: (list_object, index, columns)
            ex) lambda x,y,z: pd.concat(x, axis=1, ignore_index=False, sort=False).loc[:, z]
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(axis, int) and axis in [0, 1, 2]
    if axis == 2: assert group_key is not None and check_type_list(group_key, str)
    assert isinstance(batch_size, int) and batch_size >= 1
    assert isinstance(n_jobs, int) and n_jobs > 0
    if   axis == 0: batch_size = min(df.shape[1], batch_size)
    elif axis == 1: batch_size = min(df.shape[0], batch_size)
    index, columns = df.index, df.columns
    list_object = None
    if   axis == 0:
        batch = np.arange(df.shape[1])
        if batch_size > 1: batch = np.array_split(batch, batch.shape[0] // batch_size)
        else: batch = batch.reshape(-1, 1)
        list_object = Parallel(n_jobs=n_jobs, backend="loky", verbose=10, batch_size="auto")([delayed(func)(df.iloc[:, i_batch]) for i_batch in batch])
    elif axis == 1:
        batch = np.arange(df.shape[0])
        if batch_size > 1: batch = np.array_split(batch, batch.shape[0] // batch_size)
        else: batch = batch.reshape(-1, 1)
        list_object = Parallel(n_jobs=n_jobs, backend="loky", verbose=10, batch_size="auto")([delayed(func)(df.iloc[i_batch   ]) for i_batch in batch])
    else:
        if len(group_key) == 1: group_key = group_key[0]
        list_object = Parallel(n_jobs=n_jobs, backend="loky", verbose=10, batch_size=batch_size)([delayed(func)(dfwk) for *_, dfwk in df.groupby(group_key)])
    if len(list_object) > 0 and func_aft is not None:
        return func_aft(list_object, index, columns)
    else:
        return list_object

def drop_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate columns.
    When retrieved from a database, the same column name can exist.
    """
    assert isinstance(df, pd.DataFrame)
    list_bool, listwk = [], []
    for x in df.columns:
        if (df.columns == x).sum() == 1:
            list_bool.append(True)
            listwk.append(x)
        else:
            if x in listwk:
                list_bool.append(False)
            else:
                list_bool.append(True)
                listwk.append(x)
    return df.loc[:, list_bool]

def apply_fill_missing_values(df: pd.DataFrame, rep_nan, rep_inf, rep_minf, dtype=object, batch_size: int=1, n_jobs: int=1) -> pd.DataFrame:
    assert isinstance(df, pd.DataFrame)
    assert isinstance(batch_size, int) and batch_size >= 1
    assert isinstance(n_jobs, int) and n_jobs >= 1
    func1 = partial(apply_fill_missing_values_func1, rep_nan=rep_nan, rep_inf=rep_inf, rep_minf=rep_minf, dtype=dtype)
    return parallel_apply(
        df, func1,
        func_aft=lambda x,y,z: pd.concat(x, axis=1, ignore_index=False, sort=False).loc[:, z], axis=0, batch_size=batch_size, n_jobs=n_jobs
    )
def apply_fill_missing_values_func1(x, rep_nan=None, rep_inf=None, rep_minf=None, dtype=None):
    return x.copy().fillna(rep_nan).replace(float("inf"), rep_inf).replace(float("-inf"), rep_minf).astype(dtype)

def check_column_is_integer(se: pd.Series, except_strings: List[str] = [""]) -> pd.Series:
    se_bool = (se.str.contains(r"^[0-9]$",     regex=True) | se.str.contains(r"^-[1-9]$",     regex=True) | \
               se.str.contains(r"^[0-9]\.0+$", regex=True) | se.str.contains(r"^-[0-9]+\.0+$", regex=True) | \
               se.str.contains(r"^[1-9][0-9]+$",     regex=True) | se.str.contains(r"^-[1-9][0-9]+$", regex=True) | \
               se.str.contains(r"^[1-9][0-9]+\.0+$", regex=True) | se.str.contains(r"^-[1-9][0-9]+\.0+$", regex=True))
    se_bool = se_bool & (se.str.zfill(len("9223372036854775807")) <= "9223372036854775807")
    for x in except_strings: se_bool = se_bool | (se == x)
    return se_bool

def check_column_is_float(se: pd.Series, except_strings: List[str] = [""]) -> pd.Series:
    se_bool = (se.str.contains(r"^[0-9]\.[0-9]+$",       regex=True) | se.str.contains(r"^-[0-9]\.[0-9]+$",       regex=True) | \
               se.str.contains(r"^[1-9][0-9]+\.[0-9]+$", regex=True) | se.str.contains(r"^-[1-9][0-9]+\.[0-9]+$", regex=True))
    for x in except_strings: se_bool = se_bool | (se == x)
    return se_bool

def correct_round_values(df: pd.DataFrame, strtmp: str=None, rep_nan: str=None, n_round: int=None):
    df = df.copy()
    list_se = []
    for x in df.columns:
        se = df[x]
        if   check_column_is_integer(se, except_strings=[rep_nan]).sum() == se.shape[0]:
            # don't use under astype(np.float32)
            se = se.replace(rep_nan, strtmp, inplace=False, regex=False).astype(np.float128).astype(np.int64).astype(str).replace(strtmp, rep_nan, regex=False)
        elif check_column_is_float(  se, except_strings=[rep_nan]).sum() == se.shape[0]:
            se =  se.replace(rep_nan, np.nan, inplace=False, regex=False).astype(np.float64).round(n_round).astype(str).replace(str(np.nan), rep_nan, regex=False)
        list_se.append(se)
    return pd.concat(list_se, axis=1).loc[:, df.columns]

def to_string_all_columns(
    df: pd.DataFrame, n_round=3, rep_nan: str="%%null%%", rep_inf: str="%%null%%", rep_minf: str="%%null%%", 
    strtmp: str="-9999999", batch_size: int=1, n_jobs: int=1
) -> pd.DataFrame:
    """
    We originally want to display int values as integers, for example when displaying them on the screen.
    However, if nan is included, the display of integers is broken because it becomes a float type column.
    So, we convert all nan and adjust the int ones to be integers.
    Params::
        df:
            input dataframe
        n_round:
            Number of digits to round numbers
        rep_nan:
            Special string to replace missing values
        rep_inf:
            Special string to replace infinity values
        rep_minf:
            Special string to replace minus infinity values
        strtmp:
            Special numeric character that temporarily replaces a missing value in order to convert all dataframe data to string.
    """
    assert isinstance(df, pd.DataFrame)
    assert isinstance(n_round, int) and n_round <= 5
    assert isinstance(rep_nan, str)
    assert isinstance(rep_inf, str)
    assert isinstance(rep_minf, str)
    assert isinstance(strtmp, str) and check_str_is_integer(strtmp)
    assert isinstance(batch_size, int) and batch_size >= 1
    assert isinstance(n_jobs, int) and n_jobs >= 1
    df = df.copy()
    columns_org = df.columns.copy()
    # 1) The float column rounds. The type is preserved and only the float type is processed.
    columns = df.columns[(df.dtypes == float) | (df.dtypes == np.float16) | (df.dtypes == np.float32) | (df.dtypes == np.float64)].values.copy()
    if len(columns) > 0:
        df_target = df.loc[:, columns].copy()
        df_target = df_target.round(n_round)
        for x in columns: df[x] = df_target[x].values
    # 2) I want to convert boolean to int in advance, and insert 0, 1 or null value when csv copy.
    columns = df.columns[(df.dtypes == bool)].values.copy()
    if len(columns) > 0:
        df_target = df.loc[:, columns].copy()
        df_target = df_target.astype(np.int8)
        for x in columns: df[x] = df_target[x].values
    # 3) Fill nan, inf, -inf value, and convert dataframe to string.
    df = apply_fill_missing_values(df, rep_nan, rep_inf, rep_minf, dtype=str, batch_size=batch_size, n_jobs=n_jobs)
    if (df == strtmp).sum(axis=0).sum() > 0:
        raise Exception(f"strtmp: {strtmp} exist.")
    # 4) For strings with only numbers, convert nan to strtmp once, then convert to numeric strings and correct the values.
    func1 = partial(correct_round_values, strtmp=strtmp, rep_nan=rep_nan, n_round=n_round)
    df = parallel_apply(
        df, func1,
        func_aft=lambda x,y,z: pd.concat(x, axis=1, ignore_index=False, sort=False).loc[:, z], axis=0, batch_size=1, n_jobs=n_jobs
    )
    df = df.loc[:, columns_org]
    return df

def is_column_string(se: pd.Series) -> bool:
    """ dataframe does not distinguish between str and object """
    if se.apply(lambda x: type(x) == str).sum() == se.shape[0]:
        return True
    else:
        return False

def is_columns_string(df: pd.DataFrame) -> np.ndarray:
    """
    Return::
        Return a list of whether each column is a string type or not.
        np.array([False, True, ...])
    """
    return df.apply(lambda se: is_column_string(se), axis=0).values

def compare_strings_faster(ndf: Union[pd.Series, np.ndarray], ndf_iters: np.ndarray, method: str) -> List[np.ndarray]:
    assert isinstance(ndf, pd.Series) or isinstance(ndf, np.ndarray)
    assert isinstance(ndf_iters, np.ndarray) and len(ndf_iters.shape) <= 2
    assert isinstance(method, str) and method in ["<", "<=", ">", ">=", "isin"]
    # Processing with integer is faster than with string.
    ndf = ndf.copy()
    if isinstance(ndf, pd.Series): ndf = ndf.values
    ndf_iters = ndf_iters.copy()
    dictwk1   = {i:x for i, x in enumerate(np.sort(np.unique(np.concatenate([np.unique(ndf), np.unique(ndf_iters)]))))} # {int: string}
    dictwk2   = {x:i for i, x in dictwk1.items()} # {string: int}
    ndf       = np.vectorize(dictwk2.get)(ndf)
    ndf_iters = np.vectorize(dictwk2.get)(ndf_iters)
    list_bool = None
    if   method == "<" :   list_bool = [ndf <  x for x in ndf_iters]
    elif method == "<=":   list_bool = [ndf <= x for x in ndf_iters]
    elif method == ">":    list_bool = [ndf >  x for x in ndf_iters]
    elif method == ">=":   list_bool = [ndf >= x for x in ndf_iters]
    elif method == "isin": list_bool = [np.isin(ndf, x) for x in ndf_iters]
    return list_bool

def query(df: pd.DataFrame, str_where: str):
    assert isinstance(str_where, str) and str_where.find("(") < 0 and str_where.find(")") < 0
    str_where = [x.strip() for x in re.split("(and|or)", str_where)]
    list_bool = []
    for phrase in str_where:
        if phrase in ["and", "or"]:
            list_bool.append(phrase)
        else:
            colname, operator, value = [x.strip() for x in re.split("( = | > | < | >= | <= | in )", phrase)]
            if len(re.findall("^\[.+\]$", value)) > 0:
                value = [x.strip() for x in value[1:-1].split(",")]
                value = [int(x) if x.find("'") < 0 else x for x in value]
            elif value.find("'") < 0:
                value = int(value)
            ndf_bool = None
            if   operator == "=":  ndf_bool = (df[colname] == value).values
            elif operator == ">":  ndf_bool = (df[colname] >  value).values 
            elif operator == "<":  ndf_bool = (df[colname] <  value).values 
            elif operator == ">=": ndf_bool = (df[colname] >= value).values 
            elif operator == "<=": ndf_bool = (df[colname] <= value).values 
            elif operator == "in": ndf_bool = (df[colname].isin(value)).values
            else: raise ValueError(f"'{operator}' is not supported.")
            list_bool.append(ndf_bool.copy())
    ndf_bool, operator = None, 0
    for x in list_bool:
        if isinstance(x, np.ndarray):
            if ndf_bool is None:
                ndf_bool = x
            else:
                if operator == 0:
                    ndf_bool = (ndf_bool & x)
                else:
                    ndf_bool = (ndf_bool | x)
        elif x == "and": operator = 0
        elif x == "or":  operator = 1
    return ndf_bool

def astype_faster(df: pd.DataFrame, list_astype: List[dict]=[], batch_size: int=1, n_jobs: int=1):
    """
    list_astype:
        [{"from": from_dtype, "to": to_dtype}, {...}]
        ex) [{"from": np.float64, "to": np.float16}, {"from": ["aaaa", "bbbb"], "to": np.float16}]
    """
    assert isinstance(n_jobs, int) and n_jobs >= 1
    assert check_type_list(list_astype, dict) and len(list_astype) > 0
    assert isinstance(batch_size, int) and batch_size >= 1
    df      = df.copy()
    columns = df.columns.copy()
    for dictwk in list_astype:
        from_dtype, to_dtype = dictwk["from"], dictwk["to"]
        colbool = None
        if from_dtype is None or (isinstance(from_dtype, slice) and from_dtype == slice(None)):
            colbool = np.ones(df.shape[1]).astype(bool)
        elif isinstance(from_dtype, type):
            colbool = (df.dtypes == from_dtype)
        elif isinstance(from_dtype, np.ndarray):
            assert len(from_dtype.shape) == 1
            if from_dtype.shape[0] == 0: continue
            if from_dtype.dtype == bool:
                assert from_dtype.shape[0] == df.columns.shape[0]
                colbool = from_dtype
            elif from_dtype.dtype in [str, object]:
                colbool = df.columns.isin(from_dtype)
        if colbool is None:
            raise Exception(f"from_dtype: {from_dtype} is not matched.")
        colbool = ((df.dtypes != to_dtype).values & colbool)
        if colbool.sum() > 0:
            func1 = partial(astype_faster_func1, to_dtype=to_dtype)
            dfwk  = parallel_apply(
                df.loc[:, colbool].copy(), func1, 
                func_aft=lambda x,y,z: pd.concat(x, axis=1, ignore_index=False, sort=False), 
                batch_size=batch_size, n_jobs=n_jobs
            )
            df = df.loc[:, ~colbool]
            df = pd.concat([df, dfwk], axis=1, ignore_index=False, sort=False)
    df = df.loc[:, columns]
    return df
def astype_faster_func1(x, to_dtype=None):
    return x.astype(to_dtype)
