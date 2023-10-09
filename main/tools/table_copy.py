import sys
# local package
from kktrade.database.psgre import Psgre
from kktrade.config.psgre import HOST, PORT, USER, PASS


if __name__ == "__main__":
    args = sys.argv[1:]
    db_from, table_from, db_to, table_to = args
    DB_from = Psgre(f"host={HOST} port={PORT} dbname={db_from} user={USER} password={PASS}", max_disp_len=200)
    DB_to   = Psgre(f"host={HOST} port={PORT} dbname={db_to}   user={USER} password={PASS}", max_disp_len=200)
    df      = DB_from.select_sql(f"SELECT * FROM {table_from};")
    DB_to.execute_copy_from_df(df, table_to, system_colname_list=[], str_null="", n_jobs=12)
