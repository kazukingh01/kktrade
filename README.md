# kktrade

for trading

# Server Basic setup

### Vim

```bash
sudo apt-get update
sudo apt-get install -y vim
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
sudo ufw allow from 172.128.128.0/24 to any port YYYYY # set db port if you need
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

### Remove sound when type TAB key

```bash
sudo sed -i 's/^# set bell-style none/set bell-style none/' /etc/inputrc
echo "set belloff=all" >> ~/.vimrc
```

### Docker ( If you need )

see: https://docs.docker.com/engine/install/ubuntu/#install-using-the-convenience-script

```bash
sudo apt install curl
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

### Cron

```bash
sudo apt-get update && sudo apt-get install -y cron rsyslog
```

In order to get cron log, rsyslog config is changed.

```bash
sudo vi /etc/rsyslog.d/50-default.conf
```

change below.

```
cron.*                         /var/log/cron.log
```

```bash
sudo cp /etc/crontab /etc/crontab.`date "+%Y%m%d%H%M%S"`
sudo /etc/init.d/cron stop
sudo systemctl restart rsyslog
sudo /etc/init.d/cron start
```

### Local Network Setting

```bash
ip a
sudo vi /etc/netplan/50-cloud-init.yaml
```

add below. ( mac address should be changed the number as same as one which can be checked by 'ip a' )

```
        eth1:
            addresses:
            - 172.128.128.10/24
            set-name: eth1
            match:
                macaddress: fa:16:3e:94:58:36
```

```bash
sudo netplan apply
ip a
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
INSTALL_PYTHON_VERSION="3.12.4"
/usr/local/bin/python-build -v ${INSTALL_PYTHON_VERSION} ~/local/python-${INSTALL_PYTHON_VERSION}
echo 'export PATH="$HOME/local/python-'${INSTALL_PYTHON_VERSION}'/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
cd ~
```

### Other

```bash
sudo mkdir /home/share
sudo chown -R ubuntu:ubuntu /home/share/
```

# Run Database Container ( For Docker )

### Docker run

```bash
sudo docker run -itd --name postgres --shm-size=4g -v /home/share:/home/share postgres:16.0 /bin/bash --login
sudo docker exec postgres /etc/init.d/postgresql restart
```

# Kktrade

### Setup

```bash
cd ~
git clone https://github.com/kazukingh01/kktrade.git
python -m venv ~/kktrade/venv
source ~/kktrade/venv/bin/activate
cd ~/kktrade
pip install -e .
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
