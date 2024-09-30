import argparse, requests, json, datetime
import pandas as pd
# local package
from kkpsgre.psgre import DBConnector
from kkpsgre.webapi import create_app
from kktrade.config.psgre import HOST, PORT, DBNAME, USER, PASS, DBTYPE


if __name__ != "__main__":
    app  = create_app(HOST, PORT, DBNAME, USER, PASS, DBTYPE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reconnect",     action='store_true', default=False)
    parser.add_argument("--logfilepath",   type=str, default="")
    parser.add_argument("--log_level",     type=str, default="info")
    parser.add_argument("--ip",            type=str, default="127.0.0.1")
    parser.add_argument("--port",          type=int, default=8000)
    parser.add_argument("--is_newlogfile", action='store_true', default=False)
    parser.add_argument("--disconnect",    action='store_true', default=False)
    parser.add_argument("--test",          action='store_true', default=False)
    parser.add_argument("--check",         action='store_true', default=False)
    parser.add_argument("--db",            action='store_true', default=False)
    args   = parser.parse_args()
    def manual_connect(args):
        res = requests.post(f"http://{args.ip}:{args.port}/reconnect", json={"logfilepath": args.logfilepath, "log_level": args.log_level, "is_newlogfile": args.is_newlogfile}, headers={'Content-type': 'application/json'})
        return res
    if   args.reconnect:
        res = manual_connect(args)
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}")
    elif args.disconnect:
        res = requests.post(f"http://{args.ip}:{args.port}/disconnect", json={}, headers={'Content-type': 'application/json'})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}")
    elif args.test:
        res = requests.post(f"http://{args.ip}:{args.port}/dbinfo", json={}, headers={'Content-type': 'application/json'})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}.")
        print(res.text)
        res = requests.post(f"http://{args.ip}:{args.port}/test", json={}, headers={'Content-type': 'application/json'})
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} status_code: {res.status_code}.")
        print(pd.DataFrame(json.loads(res.json())))
    elif args.check:
        res = requests.post(f"http://{args.ip}:{args.port}/test", json={}, headers={'Content-type': 'application/json'})
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