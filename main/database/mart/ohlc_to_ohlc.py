import datetime, argparse
import pandas as pd
import numpy as np
# local package
from kktrade.core.mart import get_executions, EXCHANGES
from kktrade.util.techana import create_ohlc, ana_size_price, ana_quantile_tx_volume, \
    ana_distribution_volume_price_over_time, ana_distribution_volume_over_price, ana_rank_corr_index, indexes_to_aggregate, check_common_input, check_interval
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import \
    HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kkpsgre.util.logger import set_logger


LOGGER  = set_logger(__name__)
COLUMNS = [
    'williams_r', 'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid',
    'ave_p0050','ave_p0150','ave_p0250','ave_p0350','ave_p0450','ave_p0550','ave_p0650','ave_p0750','ave_p0850','ave_p0950',
    'var_p0050','var_p0150','var_p0250','var_p0350','var_p0450','var_p0550','var_p0650','var_p0750','var_p0850','var_p0950',
    'price_h0000','price_h0050','price_h0100','price_h0150','price_h0200','price_h0250','price_h0300','price_h0350','price_h0400','price_h0450','price_h0500','price_h0550','price_h0600','price_h0650','price_h0700','price_h0750','price_h0800','price_h0850','price_h0900','price_h0950','price_h1000',
    'size_p0050_ask','size_p0050_bid','size_p0150_ask','size_p0150_bid','size_p0250_ask','size_p0250_bid','size_p0350_ask','size_p0350_bid','size_p0450_ask','size_p0450_bid','size_p0550_ask','size_p0550_bid','size_p0650_ask','size_p0650_bid','size_p0750_ask','size_p0750_bid','size_p0850_ask','size_p0850_bid','size_p0950_ask','size_p0950_bid',
    'volume_p0050_ask','volume_p0050_bid','volume_p0150_ask','volume_p0150_bid','volume_p0250_ask','volume_p0250_bid','volume_p0350_ask','volume_p0350_bid','volume_p0450_ask','volume_p0450_bid','volume_p0550_ask','volume_p0550_bid','volume_p0650_ask','volume_p0650_bid','volume_p0750_ask','volume_p0750_bid','volume_p0850_ask','volume_p0850_bid','volume_p0950_ask','volume_p0950_bid',
    'volume_q0000_ask','volume_q0000_bid','volume_q0050_ask','volume_q0050_bid','volume_q0100_ask','volume_q0100_bid','volume_q0150_ask','volume_q0150_bid','volume_q0200_ask','volume_q0200_bid','volume_q0250_ask','volume_q0250_bid','volume_q0300_ask','volume_q0300_bid','volume_q0350_ask','volume_q0350_bid','volume_q0400_ask','volume_q0400_bid','volume_q0450_ask','volume_q0450_bid',
    'volume_q0500_ask','volume_q0500_bid','volume_q0550_ask','volume_q0550_bid','volume_q0600_ask','volume_q0600_bid','volume_q0650_ask','volume_q0650_bid','volume_q0700_ask','volume_q0700_bid','volume_q0750_ask','volume_q0750_bid','volume_q0800_ask','volume_q0800_bid','volume_q0850_ask','volume_q0850_bid','volume_q0900_ask','volume_q0900_bid','volume_q0950_ask','volume_q0950_bid','volume_q1000_ask','volume_q1000_bid',
    'volume_price_h0025_ask','volume_price_h0025_bid','volume_price_h0075_ask','volume_price_h0075_bid','volume_price_h0125_ask','volume_price_h0125_bid','volume_price_h0175_ask','volume_price_h0175_bid','volume_price_h0225_ask','volume_price_h0225_bid','volume_price_h0275_ask','volume_price_h0275_bid','volume_price_h0325_ask','volume_price_h0325_bid','volume_price_h0375_ask','volume_price_h0375_bid','volume_price_h0425_ask','volume_price_h0425_bid','volume_price_h0475_ask','volume_price_h0475_bid',
    'volume_price_h0525_ask','volume_price_h0525_bid','volume_price_h0575_ask','volume_price_h0575_bid','volume_price_h0625_ask','volume_price_h0625_bid','volume_price_h0675_ask','volume_price_h0675_bid','volume_price_h0725_ask','volume_price_h0725_bid','volume_price_h0775_ask','volume_price_h0775_bid','volume_price_h0825_ask','volume_price_h0825_bid','volume_price_h0875_ask','volume_price_h0875_bid','volume_price_h0925_ask','volume_price_h0925_bid','volume_price_h0975_ask','volume_price_h0975_bid',
]



if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--frsr", type=int, help="sampling rate. --frsr 60",  default=60)
    parser.add_argument("--tosr", type=int, help="sampling rate. --tosr 120", default=120)
    parser.add_argument("--type",   type=int)
    parser.add_argument("--itvls",  type=lambda x: [int(y) for y in x.split(",")], help="--itvls 120,480,2400,14400,86400", default="120,480,2400,14400")
    parser.add_argument("--update", action='store_true', default=False)
    args = parser.parse_args()
    assert args.type in [1, 2]
    DB   = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df   = DB.select_sql(
        f"SELECT symbol, unixtime, type, interval, sampling_rate, open, high, low, close, ave, attrs " + #+ ",".join([f"attrs->'{x}' as {x}" for x in COLUMNS]) + " " + 
        f"FROM mart_ohlc WHERE type = {0 if args.type == 1 else 1} AND interval = {args.frsr} AND sampling_rate = {args.frsr} AND " + 
        f"unixtime >= '{(args.fr - datetime.timedelta(seconds=(max(args.itvls) + args.tosr + args.frsr))).strftime('%Y-%m-%d %H:%M:%S.%f%z')}' AND " + 
        f"unixtime <  '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}';"
    )
    df   = pd.concat([df.iloc[:, :-1], pd.DataFrame(df["attrs"].tolist(), index=df.index.copy())[COLUMNS]], axis=1, ignore_index=False, sort=False)
    df["unixtime"] = (df["unixtime"].astype("int64") / 10e8).astype(int)
    for interval in args.itvls:
        LOGGER.info(f"processing sampling rate: {args.tosr}, interval: {interval}", color=["BOLD", "GREEN"])
        sampling_rate = args.tosr
        unixtime_name = "unixtime"
        from_tx = False
        df_ohlc    = create_ohlc(df[['symbol', 'unixtime', 'type', 'interval', 'open', 'high', 'low', 'close']], "unixtime", interval, args.tosr, args.fr, args.to, index_names=["symbol"], from_tx=False)
        index_base = df_ohlc.index.copy()
        list_df    = []
        list_df.append(ana_size_price(                         df[['symbol', 'unixtime', 'interval', 'ave', 'var','ntx_ask','ntx_bid','size_ask','size_bid','volume_ask','volume_bid']],                     "unixtime", interval, args.tosr, index_base, from_tx=False))
        list_df.append(ana_quantile_tx_volume(                 df[["symbol", "unixtime", "interval", "ntx_ask", "ntx_bid"] + df.columns[df.columns.str.find("volume_q") == 0].tolist()],                     "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=20))
        list_df.append(ana_distribution_volume_price_over_time(df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(volume|size|ave|var)_p[0-9]")]].tolist()], "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=10))
        list_df.append(ana_distribution_volume_over_price(     df[["symbol", "unixtime", "interval"] + df.columns[[len(x) > 0 for x in df.columns.str.findall(r"^(price|volume_price)_h[0-9]" )]].tolist()], "unixtime", interval, args.tosr, index_base, from_tx=False, n_div=20))
        if args.type == 2:
            list_df.append(ana_rank_corr_index(                df[['symbol', 'unixtime', 'interval', 'ave']],                                                                                                "unixtime", interval, args.tosr, index_base))
        df_ohlc = pd.concat([df_ohlc, ] + list_df, axis=1, ignore_index=False, sort=False)
        df_ohlc = df_ohlc.loc[:, ~df_ohlc.columns.duplicated()]
        df_ohlc = df_ohlc.reset_index()
        df_ohlc.columns     = df_ohlc.columns.str.replace("timegrp", "unixtime")
        df_ohlc = df_ohlc.loc[(df_ohlc["unixtime"] >= int(args.fr.timestamp())) & (df_ohlc["unixtime"] < int(args.to.timestamp()))]
        df_ohlc["type"]     = args.type
        df_ohlc["unixtime"] = (df_ohlc["unixtime"] + args.tosr)
        df_ohlc["unixtime"] = pd.to_datetime(df_ohlc["unixtime"], unit="s", utc=True)
        df_ohlc["attrs"]    = df_ohlc.loc[:, df_ohlc.columns[~df_ohlc.columns.isin(DB.db_layout["mart_ohlc"])]].apply(lambda x: str({y:z for y, z in x.to_dict().items() if not (z is None or np.isnan(z))}).replace("'", '"'), axis=1)
        if args.update and df_ohlc.shape[0] > 0:
            DB.delete_sql("mart_ohlc", str_where=(
                f"interval = {interval} and sampling_rate = {args.tosr} and type = {df_ohlc['type'].iloc[0]} and symbol in (" + ",".join(df_ohlc["symbol"].unique().astype(str).tolist()) + ") and " + 
                f"unixtime >= " + df_ohlc["unixtime"].min().strftime("'%Y-%m-%d %H:%M:%S.%f%z'") + " and " + 
                f"unixtime <= " + df_ohlc["unixtime"].max().strftime("'%Y-%m-%d %H:%M:%S.%f%z'")
            ))
            DB.insert_from_df(df_ohlc, "mart_ohlc", set_sql=True, n_round=10, is_select=True)
            DB.execute_sql()
