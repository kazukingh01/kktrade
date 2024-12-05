import datetime, argparse
# local package
from kktrade.core.model import get_data_for_trainign, SYMBOLS
from kkpsgre.psgre import DBConnector
from kkpsgre.util.com import str_to_datetime
from kktrade.config.mart import HOST_TO, PORT_TO, USER_TO, PASS_TO, DBNAME_TO, DBTYPE_TO
from kklogger import set_logger


LOGGER  = set_logger(__name__)


if __name__ == "__main__":
    timenow = datetime.datetime.now(tz=datetime.UTC)
    parser  = argparse.ArgumentParser()
    parser.add_argument("--fr", type=str_to_datetime, help="--fr 20200101", default=(timenow - datetime.timedelta(hours=1)))
    parser.add_argument("--to", type=str_to_datetime, help="--to 20200101", default= timenow)
    parser.add_argument("--save", type=str)
    args = parser.parse_args()
    LOGGER.info(f"args: {args}")
    DB = DBConnector(HOST_TO, PORT_TO, DBNAME_TO, USER_TO, PASS_TO, dbtype=DBTYPE_TO, max_disp_len=200)
    df = get_data_for_trainign(
        DB, args.fr, args.to, SYMBOLS, [(120, 120), (120, 480), (120, 2400), (2400, 14400), (2400, 86400)]
    )
    if args.save is not None:
        df.to_pickle(f"./{args.save}/df_{df.index.min()}_{df.index.max()}.pickle")

