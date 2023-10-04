# bitFlyer

[bitFlyer](https://bitflyer.com/ja-jp/) 

# Server Basic setup

### Vim

```bash
sudo apt-get update
sudo apt-get install vim
```

### SSH

Change port no.

```bash
sudo vi /etc/ssh/sshd_config
```

```ssh
Port XXXXX
Protocol 2
PermitRootLogin no
PasswordAuthentication no
ChallengeResponseAuthentication no
PermitEmptyPasswords no
SyslogFacility AUTHPRIV
LogLevel VERBOSE
```

```bash
sudo /etc/init.d/ssh restart
```

### Firewall

if there is no module, install it.

```bash
sudo apt update
sudo apt install -y ufw
```

```bash
sudo ufw enable
sudo ufw logging on
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow XXXXX # set ssh port
sudo ufw reload
sudo ufw status
```

### Time zone

```bash
sudo apt update
sudo apt install -y tzdata
sudo tzselect # select Asis time zone
echo "TZ='Asia/Tokyo'; export TZ" >> ~/.bashrc
source ~/.bashrc
date
```

### Docker

see: https://docs.docker.com/engine/install/ubuntu/#install-using-the-convenience-script

```bash
sudo apt install curl
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

### Set NO automatic upgrede

```bash
sudo vi /etc/apt/apt.conf.d/20auto-upgrades
```

```
APT::Periodic::Update-Package-Lists "0";
APT::Periodic::Unattended-Upgrade "0";
```

```bash
sudo reboot
```

### Python

```bash
sudo apt-get update
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev git iputils-ping net-tools vim cron rsyslog
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
cd ~/.pyenv/plugins/python-build
sudo bash ./install.sh
INSTALL_PYTHON_VERSION="3.11.2"
/usr/local/bin/python-build -v ${INSTALL_PYTHON_VERSION} ~/local/python-${INSTALL_PYTHON_VERSION}
echo 'export PATH="$HOME/local/python-'${INSTALL_PYTHON_VERSION}'/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
cd ~
python -m venv ~/venv
source ~/venv/bin/activate
```

### Other

```bash
sudo mkdir /home/share
sudo chown -R ubuntu:ubuntu /home/share/
```

# Create Database Image

### Docker start

```bash
sudo docker pull ubuntu:22.04
sudo docker run -itd -v /home/share:/home/share -p 65432:5432 --name postgres ubuntu:22.04 /bin/sh
sudo docker exec -it postgres /bin/bash
```

### Install PostgreSQL

```bash
apt-get update
UBUNTU_CODENAME=`cat /etc/os-release | grep UBUNTU_CODENAME | cut -d '=' -f 2`
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU_CODENAME}-pgdg main" | tee -a /etc/apt/sources.list.d/pgdg.list
# "focal" is Ubuntu CODE Name. check with `cat /etc/os-release` "UBUNTU_CODENAME"
apt-get install -y apt-transport-https ca-certificates curl software-properties-common openssh-client vim
curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get update
apt-get install postgresql-16 # check "apt search postgresql"
```

### Locale setting

```bash
apt-get install -y language-pack-ja
locale -a
# C
# C.utf8
# POSIX
# ja_JP.utf8
```

### DB initialize

```bash
su postgres
cd ~
mkdir /var/lib/postgresql/data
/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/data -E UTF8
```

### Start & Check

```bash
/etc/init.d/postgresql restart
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

### Config

```bash
echo 'host    all             all             0.0.0.0/0               md5' >> /etc/postgresql/16/main/pg_hba.conf
```

```bash
vi /etc/postgresql/16/main/postgresql.conf
```

```
shared_buffers = 4GB                    # min 128kB
work_mem = 64MB                         # min 64kB
effective_cache_size = 16GB
listen_addresses = '*'                  # what IP address(es) to listen on;
port = 5432                             # (change requires restart)
autovacuum = on                 # Enable autovacuum subprocess?  'on'
autovacuum_max_workers = 4              # max number of autovacuum subprocesses
maintenance_work_mem = 1GB              # min 1MB
autovacuum_work_mem = -1                # min 1MB, or -1 to use maintenance_work_mem
```

```bash
/etc/init.d/postgresql restart
```

### Docker Save

```bash
exit
sudo docker stop postgres
sudo docker commit postgres postgres:XX.X # save image
sudo docker save postgres:XX.X > postgres_XX.X.tar # export tar
sudo docker rm postgres
```

# Database Container

### Docker run

```bash
sudo docker run -itd --name postgres --shm-size=4g -v /home/share:/home/share postgres:16.0 /bin/bash --login
sudo docker exec postgres /etc/init.d/postgresql restart
```

### Create Database

```bash
sudo docker exec -it postgres /bin/bash
su postgres
cd ~
/usr/lib/postgresql/16/bin/createdb --encoding=UTF8 --locale=ja_JP.utf8 --template=template0 bitflyer
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
\1
```

### Create Schema

```bash
su postgres
psql
\c bitflyer
```

```sql
CREATE OR REPLACE FUNCTION public.update_sys_updated()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.sys_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;
ALTER FUNCTION public.update_sys_updated() OWNER TO postgres;
```

```sql
-- TABLE board
DROP TABLE public.board CASCADE;
CREATE TABLE public.board (
    symbol character varying(8) NOT NULL,
    unixtime bigint NOT NULL,
    type character varying(4) NOT NULL,
    price integer,
    size integer,
    scale smallint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);
ALTER TABLE public.board OWNER TO postgres;
CREATE INDEX board_0 ON public.board USING btree (unixtime);
CREATE TRIGGER trg_update_sys_updated_board BEFORE UPDATE ON public.board FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();
-- TABLE ticker
DROP TABLE public.ticker CASCADE;
CREATE TABLE public.ticker (
    symbol character varying(8) NOT NULL,
    tick_id integer NOT NULL,
    state smallint,
    scale smallint,
    unixtime bigint,
    best_bid integer,
    best_ask integer,
    best_bid_size integer,
    best_ask_size integer,
    total_ask_depth integer,
    total_ask_depth integer,
    market_bid_size integer,
    market_ask_size integer,
    last_traded_price integer,
    volume bigint,
    volume_by_product bigint,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);
ALTER TABLE public.ticker OWNER TO postgres;
ALTER TABLE ONLY public.ticker ADD CONSTRAINT ticker_pkey PRIMARY KEY (symbol, tick_id);
CREATE INDEX ticker_0 ON public.ticker USING btree (symbol);
CREATE INDEX ticker_1 ON public.ticker USING btree (tick_id);
CREATE INDEX ticker_2 ON public.ticker USING btree (unixtime);
CREATE TRIGGER trg_update_sys_updated_ticker BEFORE UPDATE ON public.ticker FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();
-- TABLE executions
DROP TABLE public.executions CASCADE;
CREATE TABLE public.executions (
    symbol character varying(8) NOT NULL,
    id bigint NOT NULL,
    type character varying(4) NOT NULL,
    scale smallint,
    unixtime bigint,
    price integer,
    size integer,
    sys_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);
ALTER TABLE public.executions OWNER TO postgres;
ALTER TABLE ONLY public.executions ADD CONSTRAINT executions_pkey PRIMARY KEY (symbol, id);
CREATE INDEX executions_0 ON public.executions USING btree (symbol);
CREATE INDEX executions_1 ON public.executions USING btree (id);
CREATE INDEX executions_2 ON public.executions USING btree (unixtime);
CREATE TRIGGER trg_update_sys_updated_executions BEFORE UPDATE ON public.executions FOR EACH ROW EXECUTE FUNCTION public.update_sys_updated();
```