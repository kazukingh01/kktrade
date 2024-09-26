import os, re, datetime, copy
from typing import List, Union


__all__ = [
    "makedirs",
    "correct_dirpath",
    "basename_url",
    "strfind",
    "str_to_datetime",
    "str_to_time",
    "check_type",
    "check_type_list",
    "check_str_is_integer",
    "check_str_is_float",
]


def makedirs(dirpath: str, exist_ok: bool = False, remake: bool = False):
    dirpath = correct_dirpath(dirpath)
    if remake and os.path.isdir(dirpath): shutil.rmtree(dirpath)
    os.makedirs(dirpath, exist_ok = exist_ok)

def correct_dirpath(dirpath: str) -> str:
    if os.name == "nt":
        return dirpath if dirpath[-1] == "\\" else (dirpath + "\\")
    else:
        return dirpath if dirpath[-1] == "/" else (dirpath + "/")

def basename_url(url: str) -> str:
    return url[url.rfind("/")+1:]

def strfind(pattern: str, string: str, flags=0) -> bool:
    if len(re.findall(pattern, string, flags=flags)) > 0:
        return True
    else:
        return False

def str_to_datetime(string: str, tzinfo: datetime.timezone=datetime.timezone.utc) -> datetime.datetime:
    if   strfind(r"^[0-9]+$", string) and len(string) == 8:
        return datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]), tzinfo=tzinfo)
    elif strfind(r"^[0-9][0-9][0-9][0-9]/([0-9]|[0-9][0-9])/([0-9]|[0-9][0-9])$", string):
        strwk = string.split("/")
        return datetime.datetime(int(strwk[0]), int(strwk[1]), int(strwk[2]), tzinfo=tzinfo)
    elif strfind(r"^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$", string):
        strwk = string.split("-")
        return datetime.datetime(int(strwk[0]), int(strwk[1]), int(strwk[2]), tzinfo=tzinfo)
    elif strfind(r"^[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]$", string):
        strwk = string.split("-")
        return datetime.datetime(int(strwk[2]), int(strwk[1]), int(strwk[0]), tzinfo=tzinfo)
    elif strfind(r"^[0-9]+$", string) and len(string) == 14:
        return datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]), int(string[8:10]), int(string[10:12]), int(string[12:14]), tzinfo=tzinfo)
    else:
        raise ValueError(f"{string} is not converted to datetime.")

def str_to_time(string: str) -> datetime.datetime:
    """
    Note::
        date is fix with "2000/01/01"
    """
    if   strfind(r"^[0-9]+$", string) and len(string) == 4:
        return datetime.datetime(2000, 1, 1, int(string[0:2]), int(string[2:4]), 0)
    elif strfind(r"^[0-9]+$", string) and len(string) == 6:
        return datetime.datetime(2000, 1, 1, int(string[0:2]), int(string[2:4]), int(string[4:6]))
    elif strfind(r"^[0-9]:[0-9][0-9]$", string):
        strwk = string.split(":")
        return datetime.datetime(2000, 1, 1, int(strwk[0]), int(strwk[1]), 0)
    elif strfind(r"^[0-9][0-9]:[0-9][0-9]$", string):
        strwk = string.split(":")
        return datetime.datetime(2000, 1, 1, int(strwk[0]), int(strwk[1]), 0)
    elif strfind(r"^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$", string):
        strwk = string.split(":")
        return datetime.datetime(2000, 1, 1, int(strwk[0]), int(strwk[1]), int(strwk[2]))
    else:
        raise ValueError(f"{string} is not converted to datetime.")

def check_type(instance: object, _type: Union[object, List[object]]):
    _type = [_type] if not (isinstance(_type, list) or isinstance(_type, tuple)) else _type
    is_check = [isinstance(instance, __type) for __type in _type]
    if sum(is_check) > 0:
        return True
    else:
        return False

def check_type_list(instances: List[object], _type: Union[object, List[object]], *args: Union[object, List[object]]):
    """
    Usage::
        >>> check_type_list([1,2,3,4], int)
        True
        >>> check_type_list([1,2,3,[4,5]], int, int)
        True
        >>> check_type_list([1,2,3,[4,5,6.0]], int, int)
        False
        >>> check_type_list([1,2,3,[4,5,6.0]], int, [int,float])
        True
    """
    if isinstance(instances, list) or isinstance(instances, tuple):
        for instance in instances:
            if len(args) > 0 and isinstance(instance, list):
                is_check = check_type_list(instance, *args)
            else:
                is_check = check_type(instance, _type)
            if is_check == False: return False
        return True
    else:
        return check_type(instances, _type)

def check_str_is_integer(string: str):
    boolwk = strfind(r"^[0-9]$", string) or strfind(r"^-[1-9]$", string) or \
             strfind(r"^[0-9]\.0+$", string) or strfind(r"^-[1-9]\.0+$", string) or \
             strfind(r"^[1-9][0-9]+$", string) or strfind(r"^-[1-9][0-9]+$", string) or \
             strfind(r"^[1-9][0-9]+\.0+$", string) or strfind(r"^-[1-9][0-9]+\.0+$", string)
    boolwk = boolwk & (string.zfill(len("9223372036854775807")) <= "9223372036854775807") # Is not integer over int64.
    return boolwk

def check_str_is_float(string: str):
    boolwk = strfind(r"^[0-9]\.[0-9]+$", string) or strfind(r"^-[0-9]\.[0-9]+$", string) or \
             strfind(r"^[1-9][0-9]+\.[0-9]+$", string) or strfind(r"^-[1-9][0-9]+\.[0-9]+$", string)
    return boolwk

def dict_override(_base: dict, _target: dict):
    """
    Usage::
        >>> x = {"a": 1, "b": 2, "c": [1,2,3],   "d": {"a": 2, "b": {"aa": 2, "bb": [2,3]}, "c": [1,2,3]}}
        >>> y = {        "b": 3, "c": [1,2,3,4], "d": {        "b": {"bb": [1,2,3]       }, "c": "aaa"  }}
        >>> dict_override(x, y)
    """
    base   = copy.deepcopy(_base)
    target = copy.deepcopy(_target)
    def work(a, b):
        for x, y in b.items():
            if isinstance(y, dict):
                work(a[x], y)
            else:
                a[x] = y
    work(base, target)
    return base