# Economic Calendar

https://tradingeconomics.com/calendar

~~[investing](https://in.investing.com/)~~

~~I'm not sure if this is same as investing.com.~~

# Schema

```bash
cp ~/kktrade/main/database/economic_calendar/schema.sql /home/share/schema.sql
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
sudo su - postgres -c 'psql -U postgres -d trade --port 55432 -c "DROP TABLE economic_calendar CASCADE"' 
# for docker 
sudo docker exec --user=postgres postgres psql -U postgres -d trade --port 55432 -c "DROP TABLE economic_calendar CASCADE"
```

# Cron

```bash
cat ~/kktrade/main/database/economic_calendar/crontab | sudo tee -a /etc/crontab
```
