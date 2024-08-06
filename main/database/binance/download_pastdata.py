import datetime, requests, gzip, argparse
from tqdm import tqdm
from io import StringIO
import pandas as pd
import numpy as np
# local package
from kkpsgre.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS, DBNAME
from getdata import EXCHANGE


BASEURL  = "https://public.bybit.com/trading/"
MST_CONV = {
    "linear@BTCUSDT": "BTCUSDT",
    "inverse@BTCUSD": "BTCUSD",
    "linear@ETHUSDT": "ETHUSDT",
    "inverse@ETHUSD": "ETHUSD",
    "linear@XRPUSDT": "XRPUSDT",
    "inverse@XRPUSD": "XRPUSD",
}


import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.binance.com/en/landing/data")
    page.get_by_role("button", name="Spot").click()
    page.locator(".css-14vl80k > div:nth-child(2) > .landing-ui-simple-dropdown > .css-1cda2ax").click()
    page.get_by_text("Monthly", exact=True).click()
    page.get_by_label("Data Range").click()
    page.get_by_label("Data Range").fill("2019-01")
    page.get_by_label("Data Range").press("Tab")
    page.get_by_role("textbox", name="YYYY-MM").click()
    page.get_by_role("textbox", name="YYYY-MM").fill("2019-01")
    page.locator("label").filter(has_text="Data Range").click()
    page.get_by_role("textbox", name="YYYY-MM").press("Enter")
    page.locator("div:nth-child(2) > .css-1cda2ax").first.click()
    page.get_by_role("textbox", name="YYYY-MM").fill("TC")
    page.get_by_placeholder("Search for Symbol (Max: 5)").click()
    page.locator("div:nth-child(2) > .css-1cda2ax").first.click()
    page.locator("form").get_by_role("img").first.click()
    page.locator(".css-1lrh3c9 > path").click()
    page.locator("div:nth-child(2) > .css-1cda2ax > .bn-input-suffix > .css-13c2b5p > .css-rrv9q1 > path").first.click()
    page.get_by_placeholder("Search for Symbol (Max: 5)").click()
    page.get_by_placeholder("Search for Symbol (Max: 5)").fill("BTCUSDT")
    page.get_by_role("option", name="BTCUSDT", exact=True).locator("svg").click()
    page.locator("div").filter(has_text=re.compile(r"^BTCUSDTBTCUSDTWBTCUSDT$")).get_by_role("img").nth(3).click()
    page.get_by_placeholder("Search for Symbol (Max: 5)").click()
    page.get_by_placeholder("Search for Symbol (Max: 5)").fill("BTCUSDC")
    page.get_by_role("option", name="BTCUSDC").locator("svg").click()
    page.locator("form div").filter(has_text="Field InformationCollapseField NameDescriptionidtrade").nth(1).click()
    page.get_by_role("button", name="Reject Additional Cookies").click()
    page.get_by_role("button", name="Confirm").click()
    with page.expect_download() as download_info:
        with page.expect_popup() as page1_info:
            page.get_by_role("button", name="Download").click()
        page1 = page1_info.value
    download = download_info.value
    page1.close()
    page2.close()
    page.get_by_role("button", name="Spot").click()
    page.locator("div:nth-child(2) > .css-rk221x > path").click()
    page.locator("div").filter(has_text=re.compile(r"^Confirm$")).nth(1).click()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)


def download_trade(symbol: str, date: datetime.datetime, tmp_file_path: str="./test.gz", mst_id: dict=None):
    r = requests.get(f"{BASEURL}{MST_CONV[symbol]}/{MST_CONV[symbol]}{date.strftime('%Y-%m-%d')}.csv.gz", allow_redirects=True)
    if r.status_code != 200: return pd.DataFrame()
    open(tmp_file_path, 'wb').write(r.content)
    with gzip.open(tmp_file_path, mode="rt") as gzip_file:
        content = gzip_file.read()
        df = pd.read_csv(StringIO(content))
    df["id"]       = df["trdMatchID"].astype(str)
    df["symbol"]   = mst_id[symbol]
    df["side"]     = df["side"].map({"Buy": 0, "Sell": 1}).astype(float).fillna(-1).astype(int)
    df["unixtime"] = df["timestamp"].astype(int)
    df["is_block_trade"] = False
    for x in ["price", "size"]:
        df[x] = df[x].astype(float)
    df = df.groupby(["symbol", "id"]).first().reset_index()
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101", required=True)
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101", required=True)
    parser.add_argument("--num", type=int, default=100)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--update", action='store_true', default=False)
    kwargs_psgre = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 100,
    }
    args   = parser.parse_args()
    DB     = Psgre(f"host={HOST} port={PORT} dbname={DBNAME} user={USER} password={PASS}", kwargs_psgre=kwargs_psgre, max_disp_len=200)
    df_mst = DB.select_sql(f"select * from master_symbol where is_active = true and exchange = '{EXCHANGE}'")
    mst_id = {y:x for x, y in df_mst[["symbol_id", "symbol_name"]].values}
    date_st, date_ed = args.fr, args.to
    assert date_st <= date_ed
    for date in [date_st + datetime.timedelta(days=x) for x in range((date_ed - date_st).days + 1)]:
        for symbol in mst_id.keys():
            DB.logger.info(f"date: {date}, symbol: {symbol}")
            if MST_CONV.get(symbol) is None: continue
            df = download_trade(symbol, date, mst_id=mst_id)
            if df.shape[0] > 0:
                df_exist = DB.select_sql(f"select id from {EXCHANGE}_executions where symbol = {df['symbol'].iloc[0]} and unixtime >= {int(df['unixtime'].min())} and unixtime <= {int(df['unixtime'].max())};")
                df = df.loc[~df["id"].isin(df_exist["id"])]
            else:
                DB.logger.warning("Nothing data.")
                continue
            if df.shape[0] > 0 and args.update:
                if df.shape[0] >= args.num:
                    for indexes in tqdm(np.array_split(np.arange(df.shape[0]), df.shape[0] // args.num)):
                        DB.insert_from_df(df.iloc[indexes], f"{EXCHANGE}_executions", is_select=True, n_jobs=args.jobs)
                        DB.execute_sql()
                else:
                    DB.insert_from_df(df, f"{EXCHANGE}_executions", is_select=True, n_jobs=args.jobs)
                    DB.execute_sql()
