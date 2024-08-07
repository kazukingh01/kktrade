import bs4, datetime, argparse, requests
import pandas as pd
from playwright.sync_api import sync_playwright
# local package
from kkpsgre.util.logger import set_logger


BASE_URL = "https://in.investing.com/economic-calendar/?timeFrame=custom&timeZone=55"
LOGGER   = set_logger(__name__)


def get_html_via_playwright(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        _    = page.goto(url, wait_until='domcontentloaded')
        html = page.content()
    return html

def geteconomicalcalendar(date_fr: datetime.datetime, date_to: datetime.datetime):
    assert isinstance(date_fr, datetime.datetime)
    assert isinstance(date_to, datetime.datetime)
    url = f"{BASE_URL}&startDate={int(date_fr.timestamp())}&endDate={int(date_to.timestamp())}"
    LOGGER.info(f"start: {url}")
    html       = get_html_via_playwright(url)
    soup       = bs4.BeautifulSoup(html, 'html.parser')
    sptbl      = soup.find("section", class_="instrument").find("table", class_="common-table").find("tbody")
    sptrs      = sptbl.find_all("tr", class_="common-table-item")
    sptrs      = [x for x in sptrs if "is-holiday" not in x.attrs["class"]]
    names      = [x.find("td", class_="col-event").text.strip() for x in sptrs]
    df         = pd.DataFrame(names, columns=["name"])
    df["country"]    = [x.find("td", class_="col-country").text.strip() for x in sptrs]
    df["importance"] = [x.find("td", class_="col-importance").find("div", class_="common-rating").attrs["data-print-value"].strip() for x in sptrs]
    df["actual"]     = [x.find("td", class_="col-actual").text.strip().replace("Act:", "").strip() for x in sptrs]
    df["forecast"]   = [x.find("td", class_="col-forecast").text.strip().replace("Cons:", "").strip() for x in sptrs]
    df["id"]         = [x.attrs["data-e-id"] for x in sptrs]
    df["unixtime"]   = [x.attrs["data-timestamp"] for x in sptrs]
    return df

def correct_df(df: pd.DataFrame):
    df["unixtime"  ] = df["unixtime"  ].astype(int)
    df["importance"] = df["importance"].astype(int)
    df["id"        ] = df["id"        ].astype(int)
    df["unit"]       = df["actual"  ].replace(",", "", regex=True).astype(str).str.extract(r"([^0-9\.\-]+$)", expand=True)
    df["actual"]     = df["actual"  ].replace(",", "", regex=True).astype(str).str.extract(r"^([0-9\.\-]+)",  expand=True).astype(float)
    df["forecast"]   = df["forecast"].replace(",", "", regex=True).astype(str).str.extract(r"^([0-9\.\-]+)",  expand=True).astype(float)
    df["name"]       = df["name"    ].replace("'", "''", regex=True)
    # The table has same data: https://in.investing.com/economic-calendar/?timeFrame=custom&timeZone=55&startDate=1211155200&endDate=1211241600
    df = df.groupby("id").last().reset_index(drop=False)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fn", type=str)
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--days",  type=int, help="--days 1", default=1)
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.days <= 3 # The maximum records with 1 miniute interval is 5000. 3 days data is 60 * 24 * 3 = 4320
    if args.fn == "geteconomicalcalendar":
        assert args.fr is not None and args.to is not None
        assert args.fr < args.to
        list_dates = [args.fr + datetime.timedelta(days=x) for x in range(0, (args.to - args.fr).days + args.days, args.days)]
        assert list_dates[-1] >= args.to
        for i_date, date in enumerate(list_dates):
            if i_date == (len(list_dates) - 1): break
            LOGGER.info(f"{date}, {list_dates[i_date + 1]}")
            df = geteconomicalcalendar(date, list_dates[i_date + 1])
            df = correct_df(df)
            if df.shape[0] > 0 and args.update:
                res = requests.post("http://127.0.0.1:8000/insert", json={
                    "data": df.replace({float("nan"): None}).to_dict(), "tblname": "economic_calendar", "is_select": True,
                    "add_sql": f"delete from economic_calendar where (id, unixtime) in (" + ",".join(["(" + ",".join(x) + ")" for x in df[["id", "unixtime"]].values.astype(str)]) + ");"
                }, headers={'Content-type': 'application/json'})
                assert res.status_code == 200
