from ubuntu:20.04

MAINTAINER stephen.knox@manchester.ac.uk

RUN apt-get update

RUN apt-get -y install locales
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8 

# Env variable for telnet
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y software-properties-common vim && \
    apt-get install -y --no-install-recommends apt-utils telnet && \
    apt-get install -y curl nano vim less tree locate inetutils-ping && \
    apt-get install -y mysql-client && \
    apt-get install -y git libpq-dev libmysqlclient-dev && \
    apt-get install -y sqlite3 libsqlite3-dev && \
    apt-get install -y python3-pip && \
    apt-get install -y libltdl7 && \
    apt-get install -y python3-cffi libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info && \
    rm -rf /var/lib/apt/lists/*
