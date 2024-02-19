FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt-get -y install firefox && \
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

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xvzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop

#Spark (/usr/local/spark/bin/pyspark --master spark://172.18.0.7:7077)
RUN wget https://www.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /usr/local/spark

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin

COPY /app /app
WORKDIR /app
RUN pip3 install -r requirements.txt
ENTRYPOINT /bin/sh