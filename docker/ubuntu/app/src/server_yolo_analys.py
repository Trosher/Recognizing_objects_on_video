import confluent_kafka
from loguru import logger
from asyncio import run
from signal import signal, SIGINT, SIGTERM
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from os import getcwd
import yolov5
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

@logger.catch(level='INFO')
def get_time():
    time = datetime.now()
    return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

@logger.catch(level='INFO')
def create_log_file():
    path_to_log = f"{getcwd()}/logs/server_yolo_analys_log/runtime_server_yolo_analys_{get_time()}.log"
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
        .option("subscribe", "frame_to_analyze") \
        .option("startingOffsets", "latest") \
        .load()
    return kafka_stream

def analysis_frame(msg, model):
	conf_p = {'bootstrap.servers': '172.18.0.3:29092'}
	producer = confluent_kafka.Producer(conf_p)
	redis = Redis(host="172.18.0.6", port=6379)

	url = msg.value.decode('utf-8')
	frame = model(m.unpackb(redis.get(url)))

	redis.set(f"{url}_redy", m.packb(frame.render()))
	redis.expire(f"{url}_redy", 10)

	producer.produce("processed_images", value=f"{msg}_redy".encode('utf-8'))
	producer.poll(1)

async def start_server_yolo_analys():
	create_log_file()
	logger.info("Start server_yolo_analys")
	signal(SIGINT, end_work)
	signal(SIGTERM, end_work)

	spark = SparkSession.builder \
		.appName("KafkaStream") \
		.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
		.getOrCreate()

	model = yolov5.load('yolov5s.pt')
	kafka_stream = init_spark_cons(spark)
	query_cons = kafka_stream.writeStream.foreach(lambda msg: analysis_frame(msg, model)).start()
	query_cons.awaitTermination()

if __name__ == "__main__":
    run(start_server_yolo_analys())
else:
    logger.error("The server_yolo_analys.py module cannot be run by module")