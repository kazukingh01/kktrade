# bitFlyer

[bitFlyer](https://bitflyer.com/ja-jp/) 

# Schema

##### For Host

```bash
psql -U postgres -d trade -f ~/kktrade/main/database/bitflyer/schema.sql
```

##### For Docker 

```bash
cp ~/kktrade/main/database/bitflyer/schema.sql /home/share/schema.sql
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -f /home/share/schema.sql 
```

# Cron

```bash
cat ~/kktrade/main/database/bitflyer/crontab | sudo tee -a /etc/crontab
```
