# DUKASCOPY

The reference pages aare below.

https://www.dukascopy.com/plugins/fxMarketWatch/?historical_data
https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed

I have made this program while referencing below library.

https://github.com/giuse88/duka

```bash
source ~/kktrade/venv/bin/activate
git clone https://github.com/giuse88/duka
cd duka
pip install -e .
```

### DUKASCOPY VS EODHD

DUKASCOPY is free but it's difficult to get realtime forex data.
So I use both.

DUKASCOPY allow us to download ticks (executions) data for forex data.
However, this data is extreamly huge so I have given up to collect it. 

# Schema

```bash
cp ~/kktrade/main/database/dukascopy/schema.sql /home/share/schema.sql
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
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE dukascopy_ohlcv, dukascopy_ticks CASCADE"' 
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE dukascopy_ohlcv, dukascopy_ticks CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/dukascopy/crontab | sudo tee -a /etc/crontab
```
