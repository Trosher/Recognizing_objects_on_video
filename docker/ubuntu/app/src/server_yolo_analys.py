from loguru import logger
from asyncio import run
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from os import getcwd
import yolov5
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from kafka_init import get_producer

class server_yolo_analys(object):
	@logger.catch(level='INFO')
	def __init__(self):
		self.__spark = self.__get_spark()
		self.__spark_stream = self.__get_stream_kafka()

		self.__producer = get_producer('172.18.0.3:29092')
		self.__redis = Redis(host="172.18.0.6", port=6379)
		self.__model = yolov5.load('yolov5s.pt')

	@logger.catch(level='INFO')
	def __enter__(self):
		return self

	@logger.catch(level='INFO')
	def __exit__(self, exc_type, exc_val, exc_tb):
		logger.info("End work server_yolo_analys")
		self.__redis.close()
		self.__spark.stop()
		exit(0)

	@logger.catch(level='INFO')
	def __get_time(self):
		time = datetime.now()
		return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

	@logger.catch(level='INFO')
	def __create_log_file(self):
		path_to_log = f"{getcwd()}/logs/server_yolo_analys_log/runtime_server_yolo_analys_{self.__get_time()}.log"
		logger.add(path_to_log, retention="1 days")
		logger.info(path_to_log)

	@logger.catch(level='INFO')
	def __get_df_spark(self):
		schema = StructType(
			[
				StructField("url", StringType())
			]
		)
		queue = self.__spark_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
		return queue

	@logger.catch(level='INFO')
	def __get_spark(self):
		try:
			spark = SparkSession.builder \
				.appName("ParserSession") \
				.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
				.getOrCreate()
			spark.sparkContext.setLogLevel("ERROR")
		except Exception as e:
			logger.error(ex)
		return spark

	@logger.catch(level='INFO')
	def __get_stream_kafka(self):
		try:
			kafka_topic_stream = self.__spark.readStream \
				.format("kafka") \
				.option("kafka.bootstrap.servers", "172.18.0.3:29092") \
				.option("subscribe", "frame_to_analyze") \
				.option("startingOffsets", "latest") \
				.option("failOnDataLoss", "false") \
				.load()
		except Exception as e:
			logger.error(e)
		return kafka_topic_stream

	@logger.catch(level='INFO')
	def __picture_recognition(self, url, frame):
		frame = self.__model(frame).render()[0]
		self.__redis.set(f"{url}_redy", m.packb(frame))
		self.__redis.expire(f"{url}_redy", 10)
		self.__producer.produce("processed_images", value=f"{url}_redy".encode("utf-8"))
		self.__producer.poll(0)

	@logger.catch(level='INFO')
	def __analys_frame(self, batch_df, batch_id):
		data_collect = batch_df.collect()
		for data_row in data_collect:
			url = data_row["value"].decode("utf-8")
			try:
				self.__picture_recognition(url, m.unpackb(self.__redis.get(url)))
			except Exception as e:
				logger.error(e)

	@logger.catch(level='INFO')
	async def start(self):
		self.__create_log_file()
		logger.info("Start server_yolo_analys")
		query = self.__get_stream_kafka().writeStream \
			.foreachBatch(self.__analys_frame) \
			.outputMode("append") \
			.start()
		query.awaitTermination()

if __name__ == "__main__":
	with server_yolo_analys() as sya:
		run(sya.start())
else:
	logger.error("The server_yolo_analys.py module cannot be run by module")