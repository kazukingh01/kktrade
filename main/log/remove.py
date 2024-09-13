import glob, argparse, re, datetime, os, subprocess


def strfind(pattern: str, string: str, flags=0) -> bool:
    if len(re.findall(pattern, string, flags=flags)) > 0:
        return True
    else:
        return False

def str_to_datetime(string: str) -> datetime.datetime:
    if   strfind(r"^[0-9]+$", string) and len(string) == 8:
        return datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]))
    elif strfind(r"^[0-9][0-9][0-9][0-9]/([0-9]|[0-9][0-9])/([0-9]|[0-9][0-9])$", string):
        strwk = string.split("/")
        return datetime.datetime(int(strwk[0]), int(strwk[1]), int(strwk[2]))
    elif strfind(r"^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$", string):
        strwk = string.split("-")
        return datetime.datetime(int(strwk[0]), int(strwk[1]), int(strwk[2]))
    elif strfind(r"^[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]$", string):
        strwk = string.split("-")
        return datetime.datetime(int(strwk[2]), int(strwk[1]), int(strwk[0]))
    elif strfind(r"^[0-9]+$", string) and len(string) == 14:
        return datetime.datetime(int(string[0:4]), int(string[4:6]), int(string[6:8]), int(string[8:10]), int(string[10:12]), int(string[12:14]))
    else:
        raise ValueError(f"{string} is not converted to datetime.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, help="glob.globe(**) path. ex) --path \*YYYYMMDD.log", required=True)
    parser.add_argument("--fr",   type=str, help="remove target files from  date. ex) --fr 20210101")
    parser.add_argument("--ut",   type=str, help="remove target files until date. ex) --ut 20210101")
    parser.add_argument("--time", type=int, help="remove target files by time. if '--time 10', 10 files left. ex) --time 10")
    parser.add_argument("--rm",   action='store_true', help="remove files. ex) --rm", default=False)
    args = parser.parse_args()
    assert not ((args.fr is None) and (args.ut is None) and (args.time is None))
    if args.time is not None:
        assert args.time >= 1
        proc = subprocess.run(f"ls -t {args.path}", stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        for x in proc.stdout.strip().split("\n")[args.time:]:
            print(f"remove: {x}")
            if args.rm: os.remove(x)
    else:
        args.fr = str_to_datetime(args.fr) if args.fr is not None else datetime.datetime(1900,1,1)
        args.ut = str_to_datetime(args.ut) if args.ut is not None else datetime.datetime(1900,1,1)
        path = args.path.replace("YYYY", "*").replace("MM", "*").replace("DD", "*").replace("X", "*")
        list_files = glob.glob(path)
        regex = args.path.replace(".", r"\.").replace("*", ".*"). \
                    replace("YYYY", "(?P<year>[0-9][0-9][0-9][0-9])"). \
                    replace("MM",   "(?P<month>[0-9][0-9])"). \
                    replace("DD",   "(?P<day>[0-9][0-9])"). \
                    replace("X",    "[0-9]")
        def work(x, regex):
            m = re.search(regex, x)
            if m is None:
                return False
            else:
                return datetime.datetime(int(m.group("year")), int(m.group("month")), int(m.group("day")))
        list_date = [work(x, regex) for x in list_files]
        for x, y in zip(list_files, list_date):
            if isinstance(y, bool) and y == False:
                continue
            if args.fr <= y <= args.ut:
                print(f"remove: {x}")
                if args.rm: os.remove(x)
