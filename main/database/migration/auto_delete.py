import argparse, datetime
# local package
from kkpsgre.comapi import select, insert, delete
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE
from kkpsgre.psgre import DBConnector
from kkpsgre.util.logger import set_logger


LOGGER = set_logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fr", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--fr 20200101")
    parser.add_argument("--to", type=lambda x: datetime.datetime.fromisoformat(str(x) + "T00:00:00Z"), help="--to 20200101")
    parser.add_argument("--tbl",  type=str, required=True)
    parser.add_argument("--ip",   type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db",     action='store_true', default=False)
    parser.add_argument("--update", action='store_true', default=False)
    args   = parser.parse_args()
    src    = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200) if args.db else f"{args.ip}:{args.port}"
    df_exist = select(src, f"SELECT * FROM {args.tbl} WHERE unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' LIMIT 10;")
    if df_exist.shape[0] > 0 and args.update:
        delete(src, args.tbl, str_where=(
            f"unixtime >= '{args.fr.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' and unixtime < '{args.to.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"
        ))
