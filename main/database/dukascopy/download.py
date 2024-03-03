import datetime, asyncio, argparse, lzma, httpx, struct
from typing import List
from dataclasses import dataclass
import pandas as pd
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME


EXCHANGE = "dukascopy"
MST_SCALE = {
    "USA500IDXUSD": {"ask": 0.001,   "bid": 0.001  },
    "VOLIDXUSD":    {"ask": 0.01,    "bid": 0.01   },
    "CHIIDXUSD":    {"ask": 0.001,   "bid": 0.001  },
    "HKGIDXHKD":    {"ask": 0.001,   "bid": 0.001  },
    "JPNIDXJPY":    {"ask": 0.001,   "bid": 0.001  },
    "AUSIDXAUD":    {"ask": 0.001,   "bid": 0.001  },
    "INDIDXUSD":    {},
    "SGDIDXSGD":    {"ask": 0.001,   "bid": 0.001  },
    "FRAIDXEUR":    {"ask": 0.001,   "bid": 0.001  },
    "EUSIDXEUR":    {"ask": 0.001,   "bid": 0.001  },
    "ESPIDXEUR":    {"ask": 0.001,   "bid": 0.001  },
    "GBRIDXGBP":    {"ask": 0.001,   "bid": 0.001  },
    "NLDIDXEUR":    {"ask": 0.001,   "bid": 0.001  },
    "PLNIDXPLN":    {"ask": 0.001,   "bid": 0.001  },
    "SOAIDXZAR":    {"ask": 0.01,    "bid": 0.01   },
    "DEUIDXEUR":    {"ask": 0.001,   "bid": 0.001  },
    "CHEIDXCHF":    {"ask": 0.001,   "bid": 0.001  },
    "USDJPY":       {"ask": 0.001,   "bid": 0.001  },
    "EURUSD":       {"ask": 0.00001, "bid": 0.00001},
    "GBPUSD":       {"ask": 0.00001, "bid": 0.00001},
    "USDCHF":       {"ask": 0.00001, "bid": 0.00001},
    "AUDUSD":       {"ask": 0.00001, "bid": 0.00001},
    "USDCAD":       {"ask": 0.00001, "bid": 0.00001},
    "NZDUSD":       {"ask": 0.00001, "bid": 0.00001},
    "EURGBP":       {"ask": 0.00001, "bid": 0.00001},
    "EURJPY":       {"ask": 0.001,   "bid": 0.001  },
    "EURCHF":       {"ask": 0.00001, "bid": 0.00001},
    "XAUUSD":       {"ask": 0.001,   "bid": 0.001  },
    "XAGUSD":       {"ask": 0.001,   "bid": 0.001  },
}


@dataclass
class Http:
    status_code: int
    content: bytes

async def fetch_url_retry(client, url, retry: int=5, is_throw_exception: bool=False):
    for _ in range(retry):
        try:
            response = await client.get(url)
            print(_, url)
            if response.status_code == 200:
                return (url, response)
            await asyncio.sleep(0.5)
        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            continue
    print("Max retry", url)
    if is_throw_exception: raise httpx.ConnectTimeout(f"Max retry: {url}")
    return (url, Http(200, b''))

async def fetch_urls(urls: List[str], is_throw_exception: bool=False):
    async with httpx.AsyncClient() as client:
        responses = await asyncio.gather(*(fetch_url_retry(client, url, is_throw_exception=is_throw_exception) for url in urls))
    return responses

def tokenize(buffer):
    token_size = 20
    size = int(len(buffer) / token_size)
    tokens = []
    for i in range(0, size):
        tokens.append(struct.unpack('!IIIff', buffer[i * token_size: (i + 1) * token_size]))
    return tokens

def getticks(symbol: str, date: datetime.datetime, is_throw_exception: bool=False):
    results = asyncio.run(fetch_urls([
        f"https://datafeed.dukascopy.com/datafeed/{symbol}/{date.year}/{str(date.month-1).zfill(2)}/{str(date.day).zfill(2)}/{str(hour).zfill(2)}h_ticks.bi5" for hour in range(24)
    ], is_throw_exception=is_throw_exception))
    df = [pd.DataFrame(columns=['unixtime', 'ask', 'bid', 'ask_size', 'bid_size']), ]
    for url, result in results:
        assert result.status_code == 200
        buffer = result.content
        if len(buffer) == 0: continue
        buffer = lzma.LZMADecompressor().decompress(buffer)
        data   = tokenize(buffer)
        dfwk   = pd.DataFrame(data, columns=['unixtime', 'ask', 'bid', 'ask_size', 'bid_size'])
        dfwk["unixtime"] = datetime.datetime(date.year, date.month, date.day, int(url[-13:-11]), 0, 0, tzinfo=datetime.timezone.utc).timestamp() + (dfwk["unixtime"] // 1000)
        dfwk["unixtime"] = dfwk["unixtime"].astype(int)
        df.append(dfwk)
    df = pd.concat(df, axis=0, ignore_index=True, sort=False)
    return df

def correct_df(df: pd.DataFrame, dict_scale: dict=None):
    df = df.copy()
    for x in ['ask', 'bid', 'ask_size', 'bid_size']:
        df[x] = df[x].astype(float)
        if dict_scale is not None and x in dict_scale:
            df[x] = df[x] * dict_scale[x]
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--symbols", type=lambda x: [int(y) for y in x.split(",")], help="--symbols 63,64")
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    print(args)
    DB     = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    if args.symbols is not None and len(args.symbols) > 0: df_mst = df_mst.loc[df_mst["symbol_id"].isin(args.symbols)]
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    assert args.fr is not None and args.to is not None
    assert args.fr <= args.to
    for date in [args.fr + datetime.timedelta(days=x) for x in range((args.to - args.fr).days + 1)]:
        for symbol in mst_id.keys():
            DB.logger.info(f"{date}, {symbol}")
            try: df = getticks(symbol, date, is_throw_exception=True)
            except httpx.ConnectTimeout as e:
                DB.logger.info("Timeout error.")
                continue
            if df.shape[0] == 0:
                DB.logger.info("No data.")
                continue
            df["symbol"] = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
            df = correct_df(df, dict_scale=MST_SCALE.get(symbol))
            if args.update:
                DB.set_sql(f'DELETE FROM {EXCHANGE}_ticks where symbol = {df["symbol"].iloc[0]} and unixtime >= {df["unixtime"].min()} and unixtime <= {df["unixtime"].max()};')
                DB.insert_from_df(df, f"{EXCHANGE}_ticks", set_sql=True, str_null="", is_select=False, n_jobs=8)
                DB.execute_sql()
