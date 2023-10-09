# bitFlyer

[bitFlyer](https://bitflyer.com/ja-jp/) 

# Create Database

### ( for docker )

```bash
sudo docker exec -it postgres /bin/bash
```

```bash
sudo su postgres
cd ~
/usr/lib/postgresql/16/bin/createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 --port 55432 bitflyer
psql
\l
# postgres=# \l
#                                                       List of databases
#    Name    |  Owner   | Encoding | Locale Provider |  Collate   |   Ctype    | ICU Locale | ICU Rules |   Access privileges
# -----------+----------+----------+-----------------+------------+------------+------------+-----------+-----------------------
#  bitflyer  | postgres | UTF8     | libc            | ja_JP.utf8 | ja_JP.utf8 |            |           |
#  postgres  | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           |
#  template0 | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           | =c/postgres          +
#            |          |          |                 |            |            |            |           | postgres=CTc/postgres
#  template1 | postgres | UTF8     | libc            | C.UTF-8    | C.UTF-8    |            |           | =c/postgres          +
#            |          |          |                 |            |            |            |           | postgres=CTc/postgres
# (4 rows)
\q
```

### Initialize Schema

#### For Host

```bash
sudo su postgres
cd ~
git clone https://github.com/kazukingh01/kktrade.git
DBNAME="bitflyer"
psql -U postgres -d ${DBNAME} -c 'DROP TABLE orderbook,ticker,executions CASCADE;'
psql -U postgres -d ${DBNAME} -f ~/kktrade/main/${DBNAME}/schema.sql
```

#### For Docker 

```bash
DBNAME="bitflyer"
cp ~/kktrade/main/${DBNAME}/schema.sql /home/share/
sudo docker exec --user=postgres postgres psql -U postgres -d ${DBNAME} -c 'DROP TABLE orderbook,ticker,executions CASCADE;'
sudo docker exec --user=postgres postgres psql -U postgres -d ${DBNAME} -f /home/share/schema.sql 
```

### Dump Schema

#### For Host 

```bash
sudo su postgres
cd ~
DBNAME="bitflyer"
FILEPATH="~/kktrade/main/${DBNAME}/schema.sql"
pg_dump -U postgres -d ${DBNAME} -s | sed -n -e "1,/SET default_tablespace/p" > ${FILEPATH} # this command is for functions
echo "" >> ${FILEPATH}
pg_dump -U postgres -d ${DBNAME} -s -t orderbook -t ticker -t executions >> ${FILEPATH}
```

#### For Docker 
```bash
DBNAME="bitflyer"
FILEPATH="~/kktrade/main/${DBNAME}/schema.sql"
sudo docker exec --user=postgres postgres pg_dump -U postgres -d ${DBNAME} -s | sed -n -e "1,/SET default_tablespace/p" > ${FILEPATH} # this command is for functions
echo "" >> ${FILEPATH}
sudo docker exec --user=postgres postgres pg_dump -U postgres -d ${DBNAME} -s -t orderbook -t ticker -t executions >> ${FILEPATH}
```

# Database Backup/Restore

### Backup

```bash
sudo docker exec --user=postgres postgres pg_dump -U postgres \
    -t orderbook \
    -t ticker \
    -t executions \
    -Fc bitflyer > db_bitflyer_`date "+%Y%m%d"`.dump
```

### Restore

```bash
sudo su postgres
psql bitflyer -c "DELETE FROM orderbook;"
psql bitflyer -c "DELETE FROM ticker;"
psql bitflyer -c "DELETE FROM executions;"
pg_restore -a -d bitflyer -t orderbook -t ticker -t executions -Fc /home/share/db_bitflyer_20231007.dump
```