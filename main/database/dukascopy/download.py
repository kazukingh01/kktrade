import datetime, asyncio, argparse, lzma, httpx, struct
from typing import List
from dataclasses import dataclass
import pandas as pd
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME


EXCHANGE = "dukascopy"


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
        dfwk["unixtime"] = datetime.datetime(date.year, date.month, date.day, int(url[-13:-11]), 0, 0, tzinfo=datetime.timezone.utc).timestamp() + (dfwk["unixtime"] / 1000)
        dfwk["unixtime"] = (dfwk["unixtime"] * 1000).astype(int)
        df.append(dfwk)
    df = pd.concat(df, axis=0, ignore_index=True, sort=False)
    return df

def correct_df(df: pd.DataFrame, scale_pre: dict=None):
    df = df.copy()
    for x in ['ask', 'bid', 'ask_size', 'bid_size']:
        if isinstance(scale_pre, dict) and scale_pre.get(x) is not None:
            df[x] = (df[x].astype(float) * scale_pre[x]).fillna(-1).astype(int)
        else:
            df[x] = df[x].astype(float).fillna(-1).astype(int)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=datetime.datetime.fromisoformat, help="--fr 20200101")
    parser.add_argument("--to", type=datetime.datetime.fromisoformat, help="--to 20200101")
    parser.add_argument("--update", action='store_true', default=False)
    args      = parser.parse_args()
    print(args)
    DB        = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", max_disp_len=200)
    df_mst    = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id    = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    scale_pre = {x:y for x, y in df_mst[["symbol_name", "scale_pre"]].values}
    for date in [args.fr + datetime.timedelta(days=x) for x in range((args.to - args.fr).days + 1)]:
        for symbol in mst_id.keys():
            print(date, symbol)
            try: df = getticks(symbol, date, is_throw_exception=True)
            except httpx.ConnectTimeout as e:
                print("Timeout error.")
                continue
            if df.shape[0] == 0:
                print("No data.")
                continue
            df["symbol"] = mst_id[symbol] if isinstance(mst_id, dict) and mst_id.get(symbol) is not None else symbol
            df = correct_df(df, scale_pre=scale_pre[symbol])
            if args.update:
                DB.set_sql(f'DELETE FROM {EXCHANGE}_ticks where symbol = {df["symbol"].iloc[0]} and unixtime >= {df["unixtime"].min()} and unixtime <= {df["unixtime"].max()};')
                DB.insert_from_df(df, f"{EXCHANGE}_ticks", set_sql=True, str_null="", is_select=False, n_jobs=8)
                DB.execute_sql()
