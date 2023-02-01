# Ubuntu
FROM ubuntu:18.04

RUN apt update -y  &&  apt upgrade -y && apt-get update 
RUN apt install -y curl git openjdk-8-jdk unixodbc-dev

RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
# Install py39
ENV TZ="Asia/Ho_Chi_Minh"
RUN date

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y tzdata
RUN apt-get install -y python3.9
# Install pip
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN apt install -y python3.9-distutils
RUN python3.9 get-pip.py

# Add SQL Server ODBC Driver 18 for Ubuntu 18.04
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# optional: for unixODBC development headers
RUN apt-get install -y unixodbc-dev

# Install cron
RUN apt-get install -y cron 
# Copy file ma nguon va cac file khac vao image
COPY . /stock_etl
# Tao thu muc lam viec
WORKDIR /stock_etl

# Install packages
RUN pip3 install -r setup.txt

# Set up va chay crontab

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN touch /opt/cafef_etl_daily_foreign_transactions.log

CMD cron && tail -f /opt/cafef_etl_daily_foreign_transactions.log


# Custom time
# CMD python3.9 -m stock cafef-etl-daily-foreign-transactions SQL_SERVER PERIOD --from-date 2019-07-01 --to-date 2019-07-01 >> /opt/mount/cafef_etl_daily_foreign_transactions.log
# CMD ["/bin/sh", "-c","python3.9 -m stock cafef-etl-daily-history-lookup SQL_SERVER TODAY --business-date 2020-04-01 > /stock/data/cafef_log/cafef_etl_daily_history_lookup.log 2>&1"]

# Manual
# ENTRYPOINT ["/bin/bash", "-c"]
# CMD [""]