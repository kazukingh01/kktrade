import argparse, requests, json, asyncio, datetime
from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
# local package
from kkpsgre.psgre import DBConnector
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


app  = FastAPI()
lock = asyncio.Lock()
if __name__ != "__main__":
    DB = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200)


class Insert(BaseModel):
    data: dict
    tblname: str
    is_select: bool
    add_sql: str = None
    

@app.post('/insert/')
async def insert(insert: Insert):
    df = pd.DataFrame(insert.data)
    async with lock:
        if insert.add_sql is not None:
            DB.set_sql(insert.add_sql)
        DB.insert_from_df(df, insert.tblname, set_sql=True, str_null="", is_select=insert.is_select)
        DB.execute_sql()
    return True


class Select(BaseModel):
    sql: str


@app.post('/select/')
async def select(select: Select):
    async with lock:
        df = DB.select_sql(select.sql)
    return df.to_json()


class Exec(BaseModel):
    sql: str | list[str]


@app.post('/exec/')
async def exec(exec: Exec):
    async with lock:
        DB.set_sql(exec.sql)
        DB.execute_sql()
    return True


class ReConnect(BaseModel):
    logfilepath: str=""
    log_level: str="info"
    is_newlogfile: bool=False


@app.post('/reconnect/')
async def connect(reconnect: ReConnect):
    if reconnect.logfilepath == "": reconnect.logfilepath = None
    async with lock:
        DB.__del__()
        DB.__init__(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200, logfilepath=reconnect.logfilepath, log_level=reconnect.log_level, is_newlogfile=reconnect.is_newlogfile)
    return True


@app.post('/disconnect/')
async def disconnect(disconnect: BaseModel):
    async with lock:
        DB.__del__()
    return True


@app.post('/test/')
async def test(test: BaseModel):
    async with lock:
        df = DB.read_table_layout()
    return df.to_json()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reconnect",     action='store_true', default=False)
    parser.add_argument("--logfilepath",   type=str, default="")
    parser.add_argument("--log_level",     type=str, default="info")
    parser.add_argument("--is_newlogfile", action='store_true', default=False)
    parser.add_argument("--disconnect",    action='store_true', default=False)
    parser.add_argument("--test",          action='store_true', default=False)
    parser.add_argument("--check",         action='store_true', default=False)
    parser.add_argument("--db",            action='store_true', default=False)
    args   = parser.parse_args()
    def manual_connect(args):
        res = requests.post("http://127.0.0.1:8000/reconnect", json={"logfilepath": args.logfilepath, "log_level": args.log_level, "is_newlogfile": args.is_newlogfile}, headers={'Content-type': 'application/json'})
        return res
    if   args.reconnect:
        res = manual_connect(args)
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}")
    elif args.disconnect:
        res = requests.post("http://127.0.0.1:8000/disconnect", json={}, headers={'Content-type': 'application/json'})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}")
    elif args.test:
        res = requests.post("http://127.0.0.1:8000/test", json={}, headers={'Content-type': 'application/json'})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}.")
        print(pd.DataFrame(json.loads(res.json())))
    elif args.check:
        res = requests.post("http://127.0.0.1:8000/test", json={}, headers={'Content-type': 'application/json'})
        if res.status_code != 200:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} try to reconnect... [1]")
            res = manual_connect(args)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}.")
        else:
            df = pd.DataFrame(json.loads(res.json()))
            if df.shape[0] == 0:
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} try to reconnect... [2]")
                res = manual_connect(args)
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}.")
            else:
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status is fine. status_code: {res.status_code}.")
    elif args.db:
        DB = DBConnector(HOST, PORT, DBNAME, USER, PASS, dbtype=DBTYPE, max_disp_len=200)