import bs4, datetime, argparse, time, re
import pandas as pd
import playwright
from playwright.sync_api import sync_playwright
# local package
from kkpsgre.util.logger import set_logger
from kkpsgre.comapi import insert
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


LOGGER   = set_logger(__name__)
WEBPAGES = [
    "investing.com",
    "fxstreet.com",
]
DICT_IGNORED = {}


def ignore_timeout_click(methodchain):
    if methodchain not in DICT_IGNORED:
        DICT_IGNORED[methodchain] = True
    if DICT_IGNORED[methodchain]:
        try:
            if methodchain.count() > 0:
                methodchain.click(timeout=200)
                DICT_IGNORED[methodchain] = False
        except playwright._impl._errors.TimeoutError:
            pass

def delete_ads(page: playwright.sync_api._generated.Page):
    time.sleep(2)
    # ignore_timeout_click(page.locator("#topAlertBarContainer").get_by_role("link").first)
    # page.mouse.move(0, 0)
    ignore_timeout_click(page.locator("#PromoteSignUpPopUp i").nth(4))
    page.mouse.move(0, 0)
    # ignore_timeout_click(page.get_by_role("banner").get_by_role("img").nth(3))
    # page.mouse.move(0, 0)
    ignore_timeout_click(page.locator("#sideNotificationZone span").nth(2))
    page.mouse.move(0, 0)
    # ignore_timeout_click(page.frame_locator("iframe[title=\"Side-Youtube-en_educational_screener_video\"]").get_by_label("Close Modal"))
    # ignore_timeout_click(page.locator("#closeIconHit"))
    # page.mouse.move(0, 0)

def loop_click(page, methodchain, nloop: int=3):
    for _ in range(nloop):
        delete_ads(page)
        ignore_timeout_click(methodchain)

def get_html_via_playwright(date_fr: datetime.datetime=None, date_to: datetime.datetime=None, headless: bool=True, getfrom: str="fxstreet.com"):
    """
    playwright codegen http://xxxxxxxxx...
    """
    assert date_fr is None or isinstance(date_fr, datetime.datetime)
    assert date_to is None or isinstance(date_to, datetime.datetime)
    if date_fr is not None: assert date_to is not None
    assert isinstance(headless, bool)
    assert isinstance(getfrom, str) and getfrom in WEBPAGES
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        BOOL_WORK = False
        def handle_new_page(page):
            if BOOL_WORK:
                page.close(run_before_unload=False)
        context.on("page", lambda page: handle_new_page(page))
        page = context.new_page()
        BOOL_WORK = True
        if getfrom == "fxstreet.com":
            page.goto("https://www.fxstreet.com/economic-calendar")
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
        elif getfrom == "investing.com":
            page.goto("https://www.investing.com/economic-calendar", wait_until="domcontentloaded", timeout=30000)
            delete_ads(page)
            page.get_by_role("link", name="Filters").click()
            delete_ads(page)
            page.locator("#calendarFilterBox_country").get_by_role("link", name="Select All").click()
            page.locator("#calendarFilterBox_category").get_by_role("link", name="Select All").click()
            page.locator("#importance1").check()
            page.locator("#importance2").check()
            page.locator("#importance3").check()
            page.get_by_role("link", name="Apply").click()
            loop_click(page, page.locator("#economicCurrentTime span").first, nloop=3)
            ignore_timeout_click(page.get_by_text("(GMT) Coordinated Universal"))
            delete_ads(page)
            if date_fr is not None:
                page.locator("#datePickerToggleBtn").click()
                delete_ads(page)
                loop_click(page, page.get_by_label("Start Date"), nloop=3)
                page.get_by_label("Start Date").fill("")
                page.type('input#startDate', date_fr.strftime("%m/%d/%Y"))
                loop_click(page, page.get_by_label("End Date"), nloop=3)
                page.get_by_label("End Date").fill("")
                page.type('input#endDate', date_to.strftime("%m/%d/%Y"))
                page.get_by_role("link", name="Apply").click()
                delete_ads(page)
            page.locator("#ecoCalLegend").scroll_into_view_if_needed()
            delete_ads(page)
            last_height = page.evaluate("() => document.body.scrollHeight")
            for _ in range(10):
                page.locator("#ecoCalLegend").scroll_into_view_if_needed()
                delete_ads(page)
                new_height = page.evaluate("() => document.body.scrollHeight")
                if new_height == last_height:
                    break
                else:
                    last_height = new_height
            boolwk = False
            page.wait_for_selector('table#economicCalendarData tbody', timeout=1000)
            delete_ads(page)
            div = page.locator('table#economicCalendarData tbody', has_text="No Events Scheduled")
            if div.count() > 0: boolwk = True
            if boolwk:
                html = None
            else:
                html = page.content()
        else:
            LOGGER.raise_error(f"Unexpected URL: {getfrom}")
    return html

def geteconomicalcalendar(date_fr: datetime.datetime, date_to: datetime.datetime, headless: bool=True, getfrom: str="fxstreet.com"):
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    assert isinstance(headless, bool)
    assert isinstance(getfrom, str) and getfrom in WEBPAGES
    LOGGER.info(f"url: {getfrom}, from: {date_fr}, to: {date_to} # utc")
    html   = get_html_via_playwright(date_fr=date_fr, date_to=date_to, headless=headless, getfrom=getfrom)
    if html is None:
        LOGGER.warning("No result.")
        return pd.DataFrame()
    soup   = bs4.BeautifulSoup(html, 'html.parser')
    if getfrom == "fxstreet.com":
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
            dfwk["unixtime"]   = dfwk["unixtime"].apply(lambda x: datetime.datetime.strptime(x, "%A %B %d %Y %I:%M %p %z"))
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
    elif getfrom == "investing.com":
        sptbl = soup.find("table", id="economicCalendarData").find("tbody")
        df    = []
        date  = None
        for spwk in sptbl.find_all("tr", recursive=False):
            classnames = spwk.find("td").get("class")
            if classnames is None: continue
            assert isinstance(classnames, list) and len(classnames) > 0
            if classnames[0] == "theDay":
                date = spwk.text.strip()
                continue
            listwk = spwk.find_all("td")
            dictwk = {}
            if len(listwk) >=3 and listwk[2].text.strip() == "Holiday":
                assert date is not None
                dictwk = {
                    "id":         -1,
                    "unixtime":   datetime.datetime.strptime(date + " +0000", '%A, %B %d, %Y %z'),
                    "unit2":      None,
                    "country":    listwk[1].find("span").get("data-img_key"),
                    "importance": None,
                    "name":       listwk[3].text.strip(),
                    "actual":     None,
                    "forecast":   None,
                    "previous":   None,
                    "consensus":  None,
                }
            elif len(listwk) == 8:
                dictwk = {
                    "id":         spwk.get("event_attr_id"),
                    "unixtime":   datetime.datetime.strptime(spwk.get("data-event-datetime").strip() + " +0000", '%Y/%m/%d %H:%M:%S %z'),
                    "unit2":      listwk[1].text.strip(),
                    "country":    listwk[1].find("span").get("data-img_key"),
                    "importance": listwk[2].get("data-img_key"),
                    "name":       listwk[3].text.strip(),
                    "actual":     listwk[4].text.strip(),
                    "forecast":   listwk[5].text.strip(),
                    "previous":   listwk[6].text.strip(),
                    "consensus":  None,
                }
            else:
                print(listwk)
                raise
            df.append(dictwk)
        df = pd.DataFrame(df)
    return df

def correct_df(df: pd.DataFrame, getfrom: str=None):
    assert isinstance(getfrom, str) and getfrom in WEBPAGES
    if getfrom == "fxstreet.com":
        df["importance"] = df["importance"].astype(int)
        df["id"        ] = df["id"        ].astype(int)
        for x in ["actual", "previous", "consensus", "forecast"]:
            df[x] = df[x].str.strip().replace(r"\s", "", regex=True).str.replace("®", "")
        df["unit"]  = ""
        df["unit2"] = ""
        for colname in ["consensus", "forecast", "previous", "actual"]:
            df[colname] = df[colname].replace("-¥",    "¥-",  regex=True) # Wednesday March 02 2016, Stock Investment by Foreigners FEB/27
            df[colname] = df[colname].replace(r"-\$",  "$-",  regex=True) # Friday September 30 2016, Current Account Q2
            df[colname] = df[colname].replace(r"-C\$", "C$-", regex=True)
            df[colname] = df[colname].replace(r"-€",   "€-",  regex=True)
            df["tmp1"]  = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[ 0].str.strip()
            df["tmp2"]  = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[-1].str.strip()
            df.loc[df["tmp2"] != "",  "unit"] = df.loc[df["tmp2"] != "", "tmp2"]
            df.loc[df["tmp1"] != "", "unit2"] = df.loc[df["tmp1"] != "", "tmp1"]
            df[colname] = df[colname].str.findall(r"[0-9\.\-]+", flags=re.IGNORECASE).str[0]
        df["previous"] = df["previous"].replace("--45.60", "-45.60")  # Friday March 06 2015, Balance of Trade JAN
        for colname in ["consensus", "forecast", "previous", "actual"]:
            df[colname] = df[colname].replace("-358.5.1", "-358.5") # Wednesday March 30 2016, Stock Investment by Foreigners MAR/26
            df[colname] = df[colname].replace("-235.4.1", "-235.4")
            df[colname] = df[colname].replace("2.2.",     "2.2")
            df[colname] = df[colname].replace("1.384.2",  "1.384")
            df[colname] = df[colname].replace("-395.2.8", "-395.2")
            df[colname] = df[colname].replace("-237.6.8", "-237.6")
            df[colname] = df[colname].replace("--284.24", "-284.24")
            df[colname] = df[colname].replace("-10.9.5",  "-10.9")
            df[colname] = df[colname].replace("65.5.0",   "65.5")
            df[colname] = df[colname].replace("-727.6.0", "-727.6")
            df[colname] = df[colname].replace("", float("nan")).astype(float)
        df["name"] = df["name"].replace("'", "''", regex=True)
    elif getfrom == "investing.com":
        df["id"]       = df["id"].astype(int)
        df["unixtime"] = pd.to_datetime(df["unixtime"], utc=True)
        df = df.groupby(["id", "unixtime", "country"]).first().reset_index()
        df["importance"] = df["importance"].map({"bull1": 1, "bull2": 2, "bull3": 3}).fillna(-1).astype(int)
        for x in ["actual", "previous", "consensus", "forecast"]:
            df[x] = df[x].str.strip().replace(r"\s", "", regex=True).str.replace("®", "")
        for colname in ["consensus", "forecast", "previous", "actual"]:
            df["tmp1"]  = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[ 0].str.strip()
            df["tmp2"]  = df[colname].str.replace(r"[0-9\.\-]+", " ", regex=True).str.split(" ").str[-1].str.strip()
            df.loc[df["tmp2"] != "",  "unit"] = df.loc[df["tmp2"] != "", "tmp2"]
            df[colname] = df[colname].str.findall(r"[0-9\.\-]+", flags=re.IGNORECASE).str[0]
            df[colname] = df[colname].replace("", float("nan")).astype(float)
        df["name"] = df["name"].replace("'", "''", regex=True)
    df = df.loc[:, ~df.columns.isin(["tmp1", "tmp2"])]
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--web",   type=int, help="--web 0", default=0)
    parser.add_argument("--days",  type=int, help="--days 1", default=1)
    parser.add_argument("--ip",    type=str, default="127.0.0.1")
    parser.add_argument("--port",  type=int, default=8000)
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--ishead", action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.web >= 0 and args.web < len(WEBPAGES)
    assert args.days <= 3 # The maximum records with 1 miniute interval is 5000. 3 days data is 60 * 24 * 3 = 4320
    src  = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    assert args.fr is not None and args.to is not None
    assert args.fr < args.to
    list_dates1 = [args.fr + datetime.timedelta(days=x) for x in range(0,             (args.to - args.fr).days,             args.days)]
    list_dates2 = [args.fr + datetime.timedelta(days=x) for x in range(args.days - 1, (args.to - args.fr).days + args.days, args.days)]
    assert list_dates2[-1] >= args.to
    for date_fr, date_to in zip(list_dates1, list_dates2):
        LOGGER.info(f"{date_fr}, {date_to}")
        n_limit = 5
        while True:
            try:
                if n_limit <= 0:
                    LOGGER.raise_error(f"TimeoutError limit is reached.")
                df = geteconomicalcalendar(date_fr, date_to, headless=(args.ishead == False), getfrom=WEBPAGES[args.web])
                break
            except playwright._impl._errors.TimeoutError:
                LOGGER.warning(f"TimeoutError occur [{n_limit}].")
                n_limit = n_limit - 1
                continue
        if df.shape[0] > 0: df = correct_df(df.copy(), getfrom=WEBPAGES[args.web])
        if df.shape[0] > 0 and args.update:
            df["_id"]       = df["id"].astype(str)
            df["_unixtime"] = df["unixtime"].dt.strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
            insert(
                src, df, f"economic_calendar", True,
                add_sql=(
                    f"(id, unixtime) in (" + ",".join(["(" + ",".join(x) + ")" for x in df[["_id", "_unixtime"]].values]) + ")"
                )
            )