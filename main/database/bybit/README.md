# ByBit

[bybit](https://bybit-exchange.github.io/docs/)

# Schema

```bash
cp ~/kktrade/main/database/bybit/schema.sql /home/share/schema.sql
```

##### For Host

```bash
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -f /home/share/schema.sql'
```

##### For Docker 

```bash
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -f /home/share/schema.sql 
```

# Drop Table

```bash
# for host
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE bybit_executions, bybit_kline, bybit_orderbook, bybit_ticker CASCADE"' 
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE bybit_executions, bybit_kline, bybit_orderbook, bybit_ticker CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/bybit/crontab | sudo tee -a /etc/crontab
```
