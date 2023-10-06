__all__ = [
    "SCALE_MST",
    "NAME_MST",
]

SCALE_MST = {
    0: [0, 5],
    1: [2, 2],
    2: [3, 2],
    3: [2, 6],
    4: [2, 5],
    5: [4, 2],
    6: [2, 3],
    7: [4, 0],
    8: [1, 0],
    9: [2, 0],
}

NAME_MST = {
    "BTC_JPY": 0,
    "XRP_JPY": 1,
    "ETH_JPY": 2,
    "XLM_JPY": 3,
    "MONA_JPY": 4,
    "FX_BTC_JPY": 5,
    "spot@BTCUSDT": 6,
    "spot@ETHUSDC": 7,
    "spot@BTCUSDC": 8,
    "spot@ETHUSDT": 9,
    "spot@XRPUSDT": 10,
    "linear@BTCUSDT": 11,
    "linear@ETHUSDT": 12,
    "linear@XRPUSDT": 13,
    "inverse@BTCUSD": 14,
    "inverse@ETHUSD": 15,
    "inverse@XRPUSD": 16,
}

assert sum(list(SCALE_MST.keys())) == sum(list(range(len(SCALE_MST))))
assert sum(list(NAME_MST.values())) == sum(list(range(len(NAME_MST))))
