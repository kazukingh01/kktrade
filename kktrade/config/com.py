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
    10: [9, 0],
    11: [3, 0],
    12: [0, 0],
    13: [5, 0],
    14: [6, 0],
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
    "USDJPY.FOREX":  17,
    "EURUSD.FOREX":  18,
    "GBPUSD.FOREX":  19,
    "USDCHF.FOREX":  20,
    "AUDUSD.FOREX":  21,
    "USDCAD.FOREX":  22,
    "NZDUSD.FOREX":  23,
    "EURGBP.FOREX":  24,
    "EURJPY.FOREX":  25,
    "EURCHF.FOREX":  26,
    "XAUUSD.FOREX":  27,
    "GSPC.INDX":     28,
    "DJI.INDX":      29,
    "IXIC.INDX":     30,
    "BUK100P.INDX":  31,
    "VIX.INDX":      32,
    "GDAXI.INDX":    33,
    "FCHI.INDX":     34,
    "STOXX50E.INDX": 35,
    "N100.INDX":     36,
    "BFX.INDX":      37,
    "IMOEX.INDX":    38,
    "N225.INDX":     39,
    "HSI.INDX":      40,
    "SSEC.INDX":     41,
    "AORD.INDX":     42,
    "BSESN.INDX":    43,
    "JKSE.INDX":     44,
    "NZ50.INDX":     45,
    "KS11.INDX":     46,
    "TWII.INDX":     47,
    "GSPTSE.INDX":   48,
    "BVSP.INDX":     49,
    "MXX.INDX":      50,
    "SPIPSA.INDX":   51,
    "MERV.INDX":     52,
    "TA125.INDX":    53,
}

assert sum(list(SCALE_MST.keys())) == sum(list(range(len(SCALE_MST))))
assert sum(list(NAME_MST.values())) == sum(list(range(len(NAME_MST))))
