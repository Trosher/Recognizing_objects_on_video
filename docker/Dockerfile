FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

ARG HADOOP_VERSION
ARG SPARK_VERSION

RUN apt update && \
    apt-get install -y wget && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y --no-install-recommends software-properties-common && \
    apt-get install -y --no-install-recommends gpg-agent && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y --no-install-recommends python3.11 && \
    apt-get install -y --no-install-recommends python3-pip && \
    apt-get install -y --no-install-recommends python3.11-venv && \
    apt-get install -y openssh-server unzip curl && \
    service ssh start && \
    apt install -y sudo && \
    apt-get install -y --no-install-recommends python3.11 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xvzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION /usr/local/hadoop

RUN wget https://www.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    tar -xvzf spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop3 /usr/local/spark

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin

RUN useradd -m host_u && \
    echo 'host_u:qwer' | chpasswd && \
    adduser host_u sudo

WORKDIR /app
COPY /app/requirements.txt /app
RUN pip3 install -r requirements.txt

COPY /app /app
WORKDIR /app/src
COPY .env .
EXPOSE 9999

ENTRYPOINT service ssh start && /bin/sh
