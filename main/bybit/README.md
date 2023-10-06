# bybit

[bybit](https://bybit-exchange.github.io/docs/) 

# Create Database

```bash
sudo docker exec -it postgres /bin/bash
su postgres
cd ~
/usr/lib/postgresql/16/bin/createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 bybit
psql
\l
# postgres=# \l
#                                                       List of databases
#    Name    |  Owner   | Encoding | Locale Provider |  Collate   |   Ctype    | ICU Locale | ICU Rules |   Access privileges
# -----------+----------+----------+-----------------+------------+------------+------------+-----------+-----------------------
#  bitflyer  | postgres | UTF8     | libc            | ja_JP.utf8 | ja_JP.utf8 |            |           |
#  bybit     | postgres | UTF8     | libc            | ja_JP.utf8 | ja_JP.utf8 |            |           |
#  postgres  | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           |
#  template0 | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           | =c/postgres          +
#            |          |          |                 |            |            |            |           | postgres=CTc/postgres
#  template1 | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           | =c/postgres          +
#            |          |          |                 |            |            |            |           | postgres=CTc/postgres
# (5 rows)
\q
```

### Initialize Schema

```bash
DBNAME="bybit"
cp ~/kktrade/main/${DBNAME}/schema.sql /home/share/
sudo docker exec --user=postgres postgres psql -U postgres -d ${DBNAME} -c 'DROP TABLE board,ticker,executions CASCADE;'
sudo docker exec --user=postgres postgres psql -U postgres -d ${DBNAME} -f /home/share/schema.sql 
```

#### Dump Schema

```bash
DBNAME="bitflyer"
FILEPATH="~/kktrade/main/${DBNAME}/schema.sql"
sudo docker exec --user=postgres postgres pg_dump -U postgres -d ${DBNAME} -s | sed -n -e "1,/SET default_tablespace/p" > ${FILEPATH} # this command is for functions
echo "" >> ${FILEPATH}
sudo docker exec --user=postgres postgres pg_dump -U postgres -d ${DBNAME} -s -t board -t ticker -t executions >> ${FILEPATH}
```
