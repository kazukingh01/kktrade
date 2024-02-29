FROM postgres:16.1
RUN apt-get update
RUN apt-get install -y locales
RUN rm -rf /var/lib/apt/lists/*
RUN localedef -i ja_JP -c -f UTF-8 -A /usr/share/locale/locale.alias ja_JP.UTF-8
ENV LANG ja_JP.utf8