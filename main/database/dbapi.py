from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
# local package
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


app = FastAPI()
DB  = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200)


class Insert(BaseModel):
    data: dict
    tblname: str
    is_select: bool
    add_sql: str = None


@app.post('/insert/')
async def insert(insert: Insert):
    df = pd.DataFrame(insert.data)
    if insert.add_sql is not None:
        DB.set_sql(insert.add_sql)
    DB.insert_from_df(df, insert.tblname, set_sql=True, str_null="", is_select=insert.is_select)
    DB.execute_sql()


class Select(BaseModel):
    sql: str


@app.post('/select/')
async def select(select: Select):
    df = DB.select_sql(select.sql)
    return df.to_json()
