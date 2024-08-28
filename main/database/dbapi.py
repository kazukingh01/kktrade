import argparse, requests, json
from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
# local package
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


app = FastAPI()
if __name__ != "__main__":
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


class ReConnect(BaseModel):
    logfilepath: str=None,
    is_newlogfile: bool=False,


@app.post('/reconnect/')
async def connect(reconnect: ReConnect):
    DB.__del__()
    DB.__init__(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200, logfilepath=reconnect.logfilepath, is_newlogfile=reconnect.is_newlogfile)
    return True


@app.post('/disconnect/')
async def disconnect(disconnect: BaseModel):
    DB.__del__()
    return True


@app.post('/test/')
async def test(test: BaseModel):
    df = DB.read_table_layout()
    return df.to_json()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reconnect",     action='store_true', default=False)
    parser.add_argument("--logfilepath",   type=str)
    parser.add_argument("--is_newlogfile", action='store_true', default=False)
    parser.add_argument("--disconnect",    action='store_true', default=False)
    parser.add_argument("--test",          action='store_true', default=False)
    args   = parser.parse_args()
    if   args.reconnect:
        res = requests.post("http://127.0.0.1:8000/reconnect", json={"logfilepath": args.logfilepath, "is_newlogfile": args.is_newlogfile}, headers={'Content-type': 'application/json'})
        print(res.text)
    elif args.disconnect:
        res = requests.post("http://127.0.0.1:8000/disconnect", json={}, headers={'Content-type': 'application/json'})
        print(res.text)
    elif args.test:
        res = requests.post("http://127.0.0.1:8000/test", json={}, headers={'Content-type': 'application/json'})
        print(pd.DataFrame(json.loads(res.json())))