# Binance

[Binance](https://www.binance.com/en/binance-api)

### APIs

Binance's APIs are many endpoints. We support below APIs.

- Spot Trading / Spot
- Derivatives Trading / USDS-M
- Derivatives Trading / COIN-M ( This is same as invers trading of Bybit )

# Schema

```bash
cp ~/kktrade/main/database/binance/schema.sql /home/share/schema.sql
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
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE binance_XXXXXXXXXXX, binance_YYYYYYYYYYYYY CASCADE"' 
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE binance_XXXXXXXXXXX, binance_YYYYYYYYYYYYY CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/binance/crontab | sudo tee -a /etc/crontab
```
