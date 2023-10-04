import pandas as pd
import datetime
import requests
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import CONNECTION_STRING

URL_BASE = "https://api.bitflyer.com/v1/"

SCALE_MST = {
    0: [0, 5]
}

SCALE = {
    "BTC_JPY": 0
}

DB = Psgre(CONNECTION_STRING)

def getboard(symbol: str="BTC_JPY"):
    r    = requests.get(f"{URL_BASE}getboard", params={"product_code": symbol})
    time = int(datetime.datetime.now().timestamp())
    df1  = pd.DataFrame(r.json()["bids"])
    df1["type"] = "bids"
    df2 = pd.DataFrame(r.json()["asks"])
    df2["type"] = "asks"
    df = pd.concat([
        df1.sort_values(by="price")[-200:   ],
        df2.sort_values(by="price")[    :200],
        pd.DataFrame([{"type": "mprc", "price": r.json()["mid_price"]}])
    ], axis=0, ignore_index=True)
    df["unixtime"] = time
    df["symbol"]   = symbol
    df["scale"]    = SCALE[symbol]
    df["price"]    = (df["price"].fillna(-1) * (10 ** SCALE_MST[SCALE[symbol]][0])).astype(int)
    df["size"]     = (df["size" ].fillna(-1) * (10 ** SCALE_MST[SCALE[symbol]][1])).astype(int)
    DB.insert_from_df(df, "board", set_sql=True, str_null="")
    DB.execute_sql()