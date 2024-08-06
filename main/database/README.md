# Database

There are 2 options.

- Create database by using docker 
- Create database on host server

I support below exchanges.

- [bitflyer](https://bitflyer.com/ja-jp/) 
- [bybit](https://bybit-exchange.github.io/docs/) 

# Install PostgreSQL ( Host )

### Install

```bash
sudo apt-get update
UBUNTU_CODENAME=`cat /etc/os-release | grep UBUNTU_CODENAME | cut -d '=' -f 2`
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU_CODENAME}-pgdg main" | sudo tee -a /etc/apt/sources.list.d/pgdg.list
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y postgresql-16 # check "apt search postgresql"
```

### Locale setting

```bash
sudo apt-get install -y language-pack-ja
locale -a
# C
# C.utf8
# POSIX
# ja_JP.utf8
```

### DB initialize

```bash
sudo su postgres
cd ~
mkdir /var/lib/postgresql/data
/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/data -E UTF8
```

### Start & Check

```bash
exit
sudo /etc/init.d/postgresql restart # for ubuntu user
sudo su postgres
psql
\l
# postgres=# \l
#                                                    List of databases
#    Name    |  Owner   | Encoding | Locale Provider | Collate |  Ctype  | ICU Locale | ICU Rules |   Access privileges
# -----------+----------+----------+-----------------+---------+---------+------------+-----------+-----------------------
#  postgres  | postgres | UTF8     | libc            | C.UTF-8 | C.UTF-8 |            |           |
#  template0 | postgres | UTF8     | libc            | C.UTF-8 | C.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |         |         |            |           | postgres=CTc/postgres
#  template1 | postgres | UTF8     | libc            | C.UTF-8 | C.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |         |         |            |           | postgres=CTc/postgres
# (3 rows)
\q
```

### PostgreSQL password setting

```bash
psql
alter role postgres with password 'postgres';
\q
```

### Config for connection

In order to be accessed all user, setting below.

```bash
echo 'host    all             all             0.0.0.0/0               md5' >> /etc/postgresql/16/main/pg_hba.conf
```

To protect network.

```bash
echo 'host    all             all             172.128.128.0/24        md5' >> /etc/postgresql/16/main/pg_hba.conf
```

### Config for memory

```bash
vi /etc/postgresql/16/main/postgresql.conf
```

```
shared_buffers = 2GB                    # Set 40% of RAM
work_mem = 256MB                        # min 64kB
effective_cache_size = 16GB
listen_addresses = '*'                  # what IP address(es) to listen on;
port = 55432                            # (change requires restart)
autovacuum = on                         # Enable autovacuum subprocess?  'on'
autovacuum_max_workers = 4              # max number of autovacuum subprocesses
maintenance_work_mem = 1GB              # min 1MB
autovacuum_work_mem = -1                # min 1MB, or -1 to use maintenance_work_mem
max_wal_size = 8GB
```

```bash
exit
sudo /etc/init.d/postgresql restart
```

### Create Database

```bash
sudo su postgres
cd ~
/usr/lib/postgresql/16/bin/createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 --port 55432 trade
psql
\l
# postgres=# \l
#                                                        List of databases
#    Name    |  Owner   | Encoding | Locale Provider |   Collate   |    Ctype    | ICU Locale | ICU Rules |   Access privileges
# -----------+----------+----------+-----------------+-------------+-------------+------------+-----------+-----------------------
#  postgres  | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           |
#  template0 | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |             |             |            |           | postgres=CTc/postgres
#  template1 | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |             |             |            |           | postgres=CTc/postgres
#  trade     | postgres | UTF8     | libc            | ja_JP.utf8  | ja_JP.utf8  |            |           |
# (4 rows)
\q
```

# Install PostgreSQL ( Docker )

### Docker run

```bash
cd ~/kktrade/main/database/
sudo docker image build -t postgres:16.1.jp .
sudo mkdir -p /var/local/postgresql/data
sudo docker run --name postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_INITDB_ARGS="--encoding=UTF8 --locale=ja_JP.utf8" \
    -e TZ=Asia/Tokyo \
    -v /var/local/postgresql/data:/var/lib/postgresql/data \
    -v /home/share:/home/share \
    --shm-size=4g \
    -d postgres:16.1.jp
```

### Other Config

```bash
sudo docker exec -it postgres /bin/bash
apt update
apt install -y vim
su postgres
cd ~
vi /var/lib/postgresql/data/postgresql.conf
```

[Config for memory](#config-for-memory)

```bash
exit
exit
sudo docker restart postgres
```

### Create Database

```bash
sudo docker exec --user=postgres postgres dropdb --port 55432 trade
sudo docker exec --user=postgres postgres createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 --port 55432 trade
```

# Schema

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