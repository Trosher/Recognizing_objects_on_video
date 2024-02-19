from asyncio import run
from signal import signal, SIGINT, SIGTERM
from loguru import logger
import cv2
import confluent_kafka
from json import loads
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from os import getcwd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

# dobavit funkcional otklucenia stream
def get_frame(msg):
    conf_p = {'bootstrap.servers': '172.18.0.3:29092'}
    producer = confluent_kafka.Producer(conf_p)
    redis = Redis(host="172.18.0.6", port=6379)
    url = loads(msg.value.decode('utf-8'))["url"]
    stream = cv2.VideoCapture(url)
    ret, frame = stream.read()
    if ret:
        redis.set(url, m.packb(frame))
        redis.expire(url, 10)
        producer.produce("frame_to_analyze", value=f"{url}".encode('utf-8'))
        producer.poll(1)

@logger.catch(level='INFO')
def get_time():
    time = datetime.now()
    return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

@logger.catch(level='INFO')
def create_log_file():
    path_to_log = f"{getcwd()}/logs/server_get_vidio_log/runtime_server_get_vidio_{get_time()}.log"
    logger.add(path_to_log, retention="1 days")
    logger.info(path_to_log)

@logger.catch(level='INFO')
def end_work(a, b):
    logger.info("End work get_vidio_server")
    exit(0)

def init_spark_cons(spark):
    schema = StructType([
        StructField("url", StringType(), True)
    ])
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.18.0.3:29092") \
        .option("subscribe", "url_video") \
        .option("startingOffsets", "latest") \
        .load()
    return kafka_stream.writeStream.foreach(get_frame).start()

@logger.catch(level='INFO')
async def start_server_get_vidio():
    create_log_file()
    logger.info("Start get_video_server")
    signal(SIGINT, end_work)
    signal(SIGTERM, end_work)

    spark = SparkSession.builder \
        .appName("KafkaStream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

    query_cons = init_spark_cons(spark)

    query_cons.awaitTermination()

if __name__ == "__main__":
    run(start_server_get_vidio())
else:
    logger.error("The get_vidio_server.py module cannot be run by module")
    