# Kktrade

### Server setup & Python

https://github.com/kazukingh01/kkenv/blob/main/ubuntu/README.md

### Setup

```bash
cd ~
git clone https://github.com/kazukingh01/kktrade.git
python -m venv ~/kktrade/venv
source ~/kktrade/venv/bin/activate
cd ~/kktrade
pip install -e .
playwright install-deps
playwright install
```

### Cron Schedule

```bash
cat ~/kktrade/main/database/crontab                   | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
cat ~/kktrade/main/database/binance/crontab           | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
cat ~/kktrade/main/database/bitflyer/crontab          | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
cat ~/kktrade/main/database/bybit/crontab             | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
cat ~/kktrade/main/database/dukascopy/crontab         | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
cat ~/kktrade/main/database/economic_calendar/crontab | sudo tee -a /etc/crontab > /dev/null
sudo bash -c "echo \"\" >> /etc/crontab"
sudo /etc/init.d/cron restart
```
