# YugabyteDB

```bash
sudo docker run -itd --name test  -v /home/share:/home/share ubuntu:22.04 /bin/bash --login
```

### python

```bash
sudo apt-get update
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev git iputils-ping net-tools vim less
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
cd ~/.pyenv/plugins/python-build
sudo bash ./install.sh
INSTALL_PYTHON_VERSION="3.11.9"
/usr/local/bin/python-build -v ${INSTALL_PYTHON_VERSION} ~/local/python-${INSTALL_PYTHON_VERSION}
echo 'export PATH="$HOME/local/python-'${INSTALL_PYTHON_VERSION}'/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
cd ~
```

```bash
wget https://downloads.yugabyte.com/releases/2.21.1.0/yugabyte-2.21.1.0-b271-linux-x86_64.tar.gz
tar xvfz yugabyte-2.21.1.0-b271-linux-x86_64.tar.gz && cd yugabyte-2.21.1.0/
ln -s ~/yugabyte-2.21.1.0 ~/yugabyte
~/yugabyte/bin/post_install.sh
~/yugabyte/bin/yugabyted destroy
~/yugabyte/bin/yugabyted destroy --base_dir=${HOME}/var/node
~/yugabyte/bin/yugabyted destroy --base_dir=${HOME}/yugabyte/node
~/yugabyte/bin/yugabyted cert generate_server_certs --hostnames=192.168.10.2,192.168.10.3,192.168.10.4
mkdir -p $HOME/yugabyte/node/certs
cp $HOME/var/generated_certs/192.168.10.4/* $HOME/yugabyte/node/certs
sudo mkdir -p /home/share && sudo chown -R ubuntu:ubuntu /home/share && cp -r $HOME/var/generated_certs /home/share/
~/yugabyte/bin/yugabyted start --secure --advertise_address=192.168.10.4                     --base_dir=$HOME/yugabyte/node --fault_tolerance none
~/yugabyte/bin/yugabyted start --secure --advertise_address=192.168.10.2 --join=192.168.10.4 --base_dir=$HOME/yugabyte/node --fault_tolerance none
~/yugabyte/bin/yugabyted start --secure --advertise_address=192.168.10.3 --join=192.168.10.4 --base_dir=$HOME/yugabyte/node --fault_tolerance none
```

```bash
sudo ufw allow from 192.168.10.0/24 to any port YYYYY # set db port if you need
```

change password.

```bash
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte # password is in log as installation.
\password # This password is shared to all nodes. Check here: /home/ubuntu/yugabyte/node/data/yugabyted_credentials.txt
CREATE DATABASE trade ENCODING UTF8 TEMPLATE template0;
```


```bash
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/schema_main.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/master_symbol.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/binance/schema.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/bitflyer/schema.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/bybit/schema.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/dukascopy/schema.sql
~/yugabyte/bin/ysqlsh -h 192.168.10.4 -U yugabyte -d trade -f ~/kktrade/main/database/economic_calendar/schema.sql
```

```bash
./bin/yb-admin -master_addresses 192.168.10.4:7100 --certs_dir_name ~/yugabyte/node/certs modify_placement_info cloud1.datacenter1.rack1 1
```


⚠ WARNINGS:
- ntp/chrony package is missing for clock synchronization. For centos 7, we recommend installing either ntp or chrony package and for centos 8, we recommend installing chrony package.
- Transparent hugepages disabled. Please enable transparent_hugepages.

Please review the following docs and rerun the start command:
- Quick start for Linux: https://docs.yugabyte.com/preview/quick-start/linux/

+----------------------------------------------------------------------------------------------------------------+
|                                                   yugabyted                                                    |
+----------------------------------------------------------------------------------------------------------------+
| Status              : Running.                                                                                 |
| Replication Factor  : 1                                                                                        |
| Security Features   : Encryption-in-transit, Password Authentication                                           |
| Web console         : http://192.168.10.2:7000                                                                 |
| JDBC                : jdbc:postgresql://192.168.10.2:5433/yugabyte?user=yugabyte&password=<DB_PASSWORD>        |
| YSQL                : bin/ysqlsh -h 192.168.10.2  -U yugabyte -d yugabyte                                      |
| YCQL                : bin/ycqlsh 192.168.10.2 9042 -u cassandra --ssl                                          |
| Data Dir            : /home/ubuntu/yugabyte-2.21.1.0/node1/data                                                |
| Log Dir             : /home/ubuntu/yugabyte-2.21.1.0/node1/logs                                                |
| Universe UUID       : e749752c-fa25-4b06-ba34-c30d94b3b351                                                     |
+----------------------------------------------------------------------------------------------------------------+
🚀 YugabyteDB started successfully! To load a sample dataset, try 'yugabyted demo'.
🎉 Join us on Slack at https://www.yugabyte.com/slack
👕 Claim your free t-shirt at https://www.yugabyte.com/community-rewards/




⚠ WARNINGS:
- Transparent hugepages disabled. Please enable transparent_hugepages.
- ntp/chrony package is missing for clock synchronization. For centos 7, we recommend installing either ntp or chrony package and for centos 8, we recommend installing chrony package.

Please review the following docs and rerun the start command:
- Quick start for Linux: https://docs.yugabyte.com/preview/quick-start/linux/

+----------------------------------------------------------------------------------------------------------------+
|                                                   yugabyted                                                    |
+----------------------------------------------------------------------------------------------------------------+
| Status              : Running.                                                                                 |
| Replication Factor  : 1                                                                                        |
| Security Features   : Encryption-in-transit, Password Authentication                                           |
| Web console         : http://192.168.10.3:7000                                                                 |
| JDBC                : jdbc:postgresql://192.168.10.3:5433/yugabyte?user=yugabyte&password=<DB_PASSWORD>        |
| YSQL                : bin/ysqlsh -h 192.168.10.3  -U yugabyte -d yugabyte                                      |
| YCQL                : bin/ycqlsh 192.168.10.3 9042 -u cassandra --ssl                                          |
| Data Dir            : /home/ubuntu/yugabyte-2.21.1.0/node2/data                                                |
| Log Dir             : /home/ubuntu/yugabyte-2.21.1.0/node2/logs                                                |
| Universe UUID       : e749752c-fa25-4b06-ba34-c30d94b3b351                                                     |
+----------------------------------------------------------------------------------------------------------------+
🚀 YugabyteDB started successfully! To load a sample dataset, try 'yugabyted demo'.
🎉 Join us on Slack at https://www.yugabyte.com/slack
👕 Claim your free t-shirt at https://www.yugabyte.com/community-rewards/



# TiDB

```bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.bashrc
tiup cluster
tiup update --self && tiup update cluster
sudo vi /etc/ssh/sshd_config
```

```diff
- #MaxSessions 10
+ MaxSessions 20
```

```bash
sudo /etc/init.d/ssh restart
```

```bash
vi ~/topo.yaml
```

```
# # Global variables are applied to all deployments and used as the default value of
# # the deployments if a specific deployment value is missing.
global:
 user: "tidb"
 ssh_port: 12345
 deploy_dir: "/var/local/tidb-deploy"
 data_dir: "/var/local/tidb-data"

# # Monitored variables are applied to all the machines.
monitored:
 node_exporter_port: 9100
 blackbox_exporter_port: 9115

server_configs:
 tidb:
   instance.tidb_slow_log_threshold: 300
 tikv:
   readpool.storage.use-unified-pool: false
   readpool.coprocessor.use-unified-pool: true
 pd:
   replication.enable-placement-rules: true
   replication.location-labels: ["host"]
 tiflash:
   logger.level: "info"

pd_servers:
 - host: 192.168.10.4

tidb_servers:
 - host: 192.168.10.4

tikv_servers:
 - host: 192.168.10.4
   port: 20160
   status_port: 20180
   config:
     server.labels: { host: "host-main" }

 - host: 192.168.10.2
   port: 20161
   status_port: 20181
   config:
     server.labels: { host: "host-1" }

 - host: 192.168.10.3
   port: 20162
   status_port: 20182
   config:
     server.labels: { host: "host-2" }

tiflash_servers:
 - host: 192.168.10.4

monitoring_servers:
 - host: 192.168.10.4

grafana_servers:
 - host: 192.168.10.4
```

```bash
tiup cluster deploy trade v8.3.0 ~/topo.yaml --user root -i ~/.ssh/id_rsa  # -p is option for inputting password
tiup cluster start trade --init
```

```bash
MYSQLPASS="AAAAAAAAAAAAAAA"
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS}
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} -e "DROP DATABASE trade;"
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} -e "CREATE DATABASE trade;"
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/schema_main.tidb.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/master_symbol.mysql.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/binance/schema.mysql.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/bitflyer/schema.mysql.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/bybit/schema.mysql.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/dukascopy/schema.mysql.sql
mysql -h 192.168.10.4 -P 4000 -u root --password=${MYSQLPASS} --database=trade < ~/kktrade/main/database/economic_calendar/schema.mysql.sql

```

If you want to remove it, type follow.

```bash
tiup cluster destroy trade
```
