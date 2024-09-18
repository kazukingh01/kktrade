import bs4, datetime, argparse, requests, time, re
import pandas as pd
import playwright
from playwright.sync_api import sync_playwright
# local package
from kkpsgre.util.logger import set_logger


BASE_URL = "https://tradingeconomics.com/calendar"
LOGGER   = set_logger(__name__)


def get_html_via_playwright(date_fr: datetime.datetime=None, date_to: datetime.datetime=None, headless: bool=True):
    assert date_fr is None or isinstance(date_fr, datetime.datetime)
    assert date_to is None or isinstance(date_to, datetime.datetime)
    if date_fr is not None: assert date_to is not None
    assert isinstance(headless, bool)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        page.goto(BASE_URL)
        time.sleep(1)
        page.locator("#DropDownListTimezone").select_option("0")
        page.get_by_role("button", name="   Category").click()
        page.get_by_role("link", name="All Events").click()
        page.get_by_role("button", name="  Countries").click()
        page.locator("#te-c-main-countries span").filter(has_text="All").click()
        page.get_by_text("Save").click()
        page.get_by_role("button", name="  Impact").click()
        if date_fr is not None:
            page.get_by_role("cell", name="  Recent    Impact").get_by_role("list").locator("a").first.click()
            page.get_by_role("button", name="  Recent").click()
            page.get_by_role("menuitem", name="  Custom").click()
            page.locator("#startDate").click()
            page.locator("#startDate").fill(date_fr.strftime("%Y-%m-%d"))
            page.locator("#endDate").click()
            page.locator("#endDate").fill(date_to.strftime("%Y-%m-%d"))
            page.get_by_role("button", name="Submit").click()
        boolwk = False
        try:
            page.wait_for_selector('table#calendar', timeout=1000)
        except playwright._impl._errors.TimeoutError:
            div = page.locator('div.table-responsive > div', has_text="No Events Scheduled")
            if div.count() > 0: boolwk = True
        if boolwk:
            html = None
        else:
            page.wait_for_selector('table#calendar')
            html = page.content()
    return html

def geteconomicalcalendar(date_fr: datetime.datetime, date_to: datetime.datetime):
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    LOGGER.info(f"url: {BASE_URL}, from: {date_fr}, to: {date_to} # utc")
    html   = get_html_via_playwright(date_fr=date_fr, date_to=date_to)
    if html is None:
        LOGGER.warning("No result.")
        return pd.DataFrame()
    soup   = bs4.BeautifulSoup(html, 'html.parser')
    sptbl  = soup.find("table", id="calendar")
    theads = sptbl.find_all("thead", class_="table-header", recursive=False)
    tbodys = sptbl.find_all("tbody", recursive=False)
    assert len(theads) == len(tbodys)
    df = None
    for thead, tbody in zip(theads, tbodys):
        dfwk = pd.DataFrame()
        dfwk["unixtime"]   = [y.text.strip() for y in [x.find_all("td", recursive=False)[0] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["unixtime"]   = dfwk["unixtime"].str.strip().replace("", "11:59 PM")
        dfwk["unixtime"]   = thead.find_all("th")[0].text.strip() + " " + dfwk["unixtime"] + " +0000"
        dfwk["unixtime"]   = dfwk["unixtime"].apply(lambda x: str(int(datetime.datetime.strptime(x, "%A %B %d %Y %I:%M %p %z").timestamp())) + " ")
        dfwk["country"]    = [y.text.strip() for y in [x.find_all("td", recursive=False)[1] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["name"]       = [y.text.strip() for y in [x.find_all("td", recursive=False)[2] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["actual"]     = [y.text.strip() for y in [x.find_all("td", recursive=False)[3] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["previous"]   = [y.text.strip() for y in [x.find_all("td", recursive=False)[4] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["consensus"]  = [y.text.strip() for y in [x.find_all("td", recursive=False)[5] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["forecast"]   = [y.text.strip() for y in [x.find_all("td", recursive=False)[6] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["importance"] = [y.find("span").get("class") for y in [x.find_all("td", recursive=False)[0] for x in tbody.find_all("tr", recursive=False)]]
        dfwk["importance"] = dfwk["importance"].str[0].fillna(" ").str[-1].str.strip().replace("", float("nan")).fillna(-1)
        dfwk["id"]         = [x.get("data-id") for x in tbody.find_all("tr", recursive=False)]
        if df is None: df = dfwk.copy()
        else: df = pd.concat([df, dfwk], ignore_index=True, sort=False)
    return df

def correct_df(df: pd.DataFrame):
    df["unixtime"  ] = df["unixtime"  ].astype(int)
    df["importance"] = df["importance"].astype(int)
    df["id"        ] = df["id"        ].astype(int)
    for x in ["actual", "previous", "consensus", "forecast"]:
        df[x] = df[x].str.strip().replace(r"\s", "", regex=True).str.replace("®", "")
    df["unit"]  = ""
    df["unit2"] = ""
    for colname in ["consensus", "forecast", "previous", "actual"]:
        df["tmp1"] = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[ 0].str.strip()
        df["tmp2"] = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[-1].str.strip()
        df.loc[df["tmp2"] != "",  "unit"] = df.loc[df["tmp2"] != "", "tmp2"]
        df.loc[df["tmp1"] != "", "unit2"] = df.loc[df["tmp1"] != "", "tmp1"]
        df[colname] = df[colname].str.findall(r"[0-9\.\-]+", flags=re.IGNORECASE).str[0]
    for colname in ["consensus", "forecast", "previous", "actual"]:
        df[colname] = df[colname].replace("", float("nan")).astype(float)
    df["name"] = df["name"].replace("'", "''", regex=True)
    df = df.loc[:, ~df.columns.isin(["tmp1", "tmp2"])]
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--days",  type=int, help="--days 1", default=1)
    parser.add_argument("--ip",    type=str, default="127.0.0.1")
    parser.add_argument("--port",  type=int, default=8000)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.days <= 7 # The maximum records with 1 miniute interval is 5000. 3 days data is 60 * 24 * 3 = 4320
    if args.fn == "geteconomicalcalendar":
        assert args.fr is not None and args.to is not None
        assert args.fr < args.to
        list_dates1 = [args.fr + datetime.timedelta(days=x) for x in range(0,             (args.to - args.fr).days,             args.days)]
        list_dates2 = [args.fr + datetime.timedelta(days=x) for x in range(args.days - 1, (args.to - args.fr).days + args.days, args.days)]
        assert list_dates2[-1] >= args.to
        for date_fr, date_to in zip(list_dates1, list_dates2):
            LOGGER.info(f"{date_fr}, {date_to}")
            df = geteconomicalcalendar(date_fr, date_to)
            if df.shape[0] > 0: df = correct_df(df.copy())
            if df.shape[0] > 0 and args.update:
                res = requests.post(f"http://{args.ip}:{args.port}/insert", json={
                    "data": df.replace({float("nan"): None}).to_dict(), "tblname": "economic_calendar", "is_select": True,
                    "add_sql": f"delete from economic_calendar where (id, unixtime) in (" + ",".join(["(" + ",".join(x) + ")" for x in df[["id", "unixtime"]].values.astype(str)]) + ");"
                }, headers={'Content-type': 'application/json'})
                assert res.status_code == 200
