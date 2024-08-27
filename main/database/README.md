# Database Install

see: https://github.com/kazukingh01/kkpsgre/blob/89412ed0d2a2e5f90eb2322034a9ee9c99e98b27/README.md

# Create Database

### PostgreSQL

( Host )

```bash
sudo su postgres
# dropdb --port 5432 trade
createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 --port 5432 trade
```

( Docker )

```bash
# sudo docker exec --user=postgres postgres dropdb --port 5432 trade
sudo docker exec --user=postgres postgres createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 --port 5432 trade
```

### MySQL

( Docker )

```bash
# sudo docker exec mysql mysql --password=mysql -e "DROP DATABASE trade;"
sudo docker exec mysql mysql --password=mysql -e "CREATE DATABASE trade;"
```

# Insert SQL

### PostgreSQL

( Host )

```bash
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/schema_main.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/master_symbol.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/binance/schema.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/bitflyer/schema.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/bybit/schema.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/dukascopy/schema.psgre.sql'
sudo su - postgres -c 'psql -d trade --port 5432 -f ${HOME}/kktrade/main/database/economic_calendar/schema.psgre.sql'
```

( Docker )

```bash
cp ~/kktrade/main/database/schema_main.psgre.sql              /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/master_symbol.psgre.sql            /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/binance/schema.psgre.sql           /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/bitflyer/schema.psgre.sql          /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/bybit/schema.psgre.sql             /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/dukascopy/schema.psgre.sql         /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
cp ~/kktrade/main/database/economic_calendar/schema.psgre.sql /home/share/psgre.sql && sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 5432 -f /home/share/psgre.sql
```

### MySQL

( Host )

```bash
sudo apt update && sudo apt-get install mysql-client
MYSQLPASS="AAAAAAAAAAAAAAA"
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS}
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} -e "DROP DATABASE trade;"
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} -e "CREATE DATABASE trade;"
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/schema_main.tidb.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/master_symbol.mysql.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/binance/schema.mysql.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/bitflyer/schema.mysql.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/bybit/schema.mysql.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/dukascopy/schema.mysql.sql
mysql -h 192.168.10.1 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/economic_calendar/schema.mysql.sql
```

( Docker )

```bash
cp ~/kktrade/main/database/schema_main.mysql.sql              /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/master_symbol.mysql.sql            /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/binance/schema.mysql.sql           /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/bitflyer/schema.mysql.sql          /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/bybit/schema.mysql.sql             /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/dukascopy/schema.mysql.sql         /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
cp ~/kktrade/main/database/economic_calendar/schema.mysql.sql /home/share/mysql.sql && sudo docker exec mysql /bin/sh -c "mysql --password=mysql --database=trade < /home/share/mysql.sql"
```

# Cron

```bash
echo "0   0   * * *   ubuntu  sleep 30 && sudo /etc/init.d/postgresql restart" | sudo tee -a /etc/crontab
# echo "0   0   * * *   ubuntu  sleep 30 && sudo docker exec postgres /etc/init.d/postgresql restart" | sudo tee -a /etc/crontab # for docker 
sudo /etc/init.d/cron restart
sudo systemctl restart rsyslog
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

# Fastapi

```bash
cd ~/kktrade/main/database && nohup ~/kktrade/venv/bin/uvicorn dbapi:app > dbapi.log &
```