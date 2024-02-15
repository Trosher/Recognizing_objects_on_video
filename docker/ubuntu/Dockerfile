# Установка базового образа
FROM ubuntu:latest

# Обновление списка пакетов и установка необходимых утилит
RUN apt-get -y update && \
    apt-get -y install firefox && \
    apt-get install -y wget && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y python3 && \
    apt-get install -y python3-pip && \
    apt-get install -y openssh-server unzip curl && \
    service ssh restart && \
    apt install -y sudo


# Установка Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xvzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop

# Установка Spark (/usr/local/spark/bin/pyspark --master spark://172.18.0.7:7077 "Подключение к кластеру")
RUN wget https://www.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /usr/local/spark

# Настройка переменных среды для JDK, Hadoop и Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin

# Копирование папки и выполнение requirements.txt
COPY app/ /app
WORKDIR /app/src
RUN pip3 install -r requirements.txt
ENTRYPOINT /bin/sh