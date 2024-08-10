# Database Install

see: https://github.com/kazukingh01/kkpsgre/blob/30d7022749b70cdae939707aad9448430fdd70f8/README.md

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


### Import Schema

```bash
cp ~/kktrade/main/database/schema_main.sql   /home/share/schema_main.sql
cp ~/kktrade/main/database/master_symbol.sql /home/share/master_symbol.sql
```

##### For Host

```bash
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -f /home/share/schema_main.sql'
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -f /home/share/master_symbol.sql'
```

##### For Docker 

```bash
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -f /home/share/schema_main.sql
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -f /home/share/master_symbol.sql
```

### Dump Schema

##### For Host 

```bash
sudo su postgres
cd ~
pg_dump -U postgres --port 55432 -d trade -s > ./schema.sql
```

##### For Docker 

```bash
sudo docker exec --user=postgres postgres pg_dump -U postgres -d trade --port 55432 -s > ./schema.sql
```

### Dump Data

```bash
sudo docker exec --user=postgres postgres pg_dump -U postgres -d trade --port 55432 -t master_symbol -Fp > ./db_`date "+%Y%m%d"`.dump
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