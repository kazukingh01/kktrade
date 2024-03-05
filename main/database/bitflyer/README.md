# bitFlyer

[bitFlyer](https://bitflyer.com/ja-jp/) 

# Schema

```bash
cp ~/kktrade/main/database/bitflyer/schema.sql /home/share/schema.sql
```

##### For Host

```bash
sudo su - postgres -c 'psql -U postgres -d trade -f /home/share/schema.sql'
```

##### For Docker 

```bash
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -f /home/share/schema.sql 
```

# Drop Table

```bash
# for host
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE bitflyer_executions, bitflyer_orderbook, bitflyer_ticker CASCADE"'
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE bitflyer_executions, bitflyer_orderbook, bitflyer_ticker CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/bitflyer/crontab | sudo tee -a /etc/crontab
```
