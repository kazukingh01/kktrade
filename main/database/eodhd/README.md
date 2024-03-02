# EODHD

[EODHD](https://eodhd.com/)

# Schema

```bash
cp ~/kktrade/main/database/eodhd/schema.sql /home/share/schema.sql
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
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE eodhd_ohlcv CASCADE"' 
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE eodhd_ohlcv CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/eodhd/crontab | sudo tee -a /etc/crontab
```
