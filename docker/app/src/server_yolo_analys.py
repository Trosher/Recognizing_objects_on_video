from asyncio import run

import msgpack_numpy as m
import yolov5

import time
from datetime import datetime
from os import getcwd
from loguru import logger

from redis import Redis
from pyspark.sql import SparkSession
from kafka_init import get_producer
import time

class server_yolo_analys(object):
	"""The yolo analys server class."""
	@logger.catch(level='INFO')
	def __init__(self):
		"""Initializes the server_yolo_analys.

		Initializes Spark, Kafka stream, producer, Redis, and YOLOv5 model.

		Returns:
			None
			
		"""
		self.__spark = self.__get_spark()
		self.__spark_stream = self.__get_stream_kafka()

		self.__producer = get_producer('172.16.0.3:29092')
		self.__redis = Redis(host="172.16.0.6", port=6379)
		self.__model = yolov5.load('yolov5s.pt')

	@logger.catch(level='INFO')
	def __enter__(self):
		"""Enters the context of the server_yolo_analys.

		Returns:
			self: The instance of the class.
			
		"""
		return self

	@logger.catch(level='INFO')
	def __exit__(self, exc_type, exc_val, exc_tb):
		"""Handles the exit of the server_yolo_analys.

		Args:
			exc_type: The type of exception.
			exc_val: The exception value.
			exc_tb: The exception traceback.

		Returns:
			None
			
		"""
		logger.info("End work server_yolo_analys")
		self.__producer.flush()
		self.__redis.close()
		self.__spark.stop()
		exit(0)

	@logger.catch(level='INFO')
	def __get_time(self):
		"""Gets the current time and returns it in a specific format.

        Returns:
            str: The current time formatted as 'year-month-day-hour-minute-second'.

        """
		time = datetime.now()
		return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

	@logger.catch(level='INFO')
	def __create_log_file(self):
		"""Creates a log file for the yolo analys server.
        
        Returns:
            None
        
        """
		path_to_log = f"{getcwd()}/logs/server_yolo_analys_log/runtime_server_yolo_analys_{self.__get_time()}.log"
		logger.add(path_to_log, retention="1 days")
		logger.info(path_to_log)

	@logger.catch(level='INFO')
	def __get_df_spark(self):
		"""Gets the DataFrame from the Spark stream.

		Returns:
			DataFrame: The DataFrame from the Spark stream.
		"""
		queue = self.__spark_stream.selectExpr("CAST(value AS STRING)")
		return queue

	@logger.catch(level='INFO')
	def __get_spark(self):
		"""Gets the Spark session for data processing.

		Returns:
			SparkSession: The Spark session for data processing.
		"""
		try:
			spark = SparkSession.builder \
				.appName("ParserSession") \
				.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
				.getOrCreate()
			spark.sparkContext.setLogLevel("ERROR")
		except Exception as e:
			logger.error(e)
		return spark

	@logger.catch(level='INFO')
	def __get_stream_kafka(self):
		"""Gets the Kafka stream for frame_to_analyze topic.

		Returns:
			DataFrame: The Kafka stream for frame_to_analyze topic.
		"""
		try:
			kafka_topic_stream = self.__spark.readStream \
				.format("kafka") \
				.option("kafka.bootstrap.servers", "172.16.0.3:29092") \
				.option("subscribe", "frame_to_analyze") \
				.option("startingOffsets", "latest") \
				.option("failOnDataLoss", "false") \
				.load()
		except Exception as e:
			logger.error(e)
		return kafka_topic_stream

	@logger.catch(level='INFO')
	def __picture_recognition(self, url: str, frame):
		"""Performs picture recognition on the given URL and frame, then sends the processed image to a Kafka topic.

		Args:
			url (str): The URL of the image.
			frame: The image frame to be processed.

		Returns:
			None
			
		"""
		frame = self.__model(frame).render()[0]
		self.__redis.set(f"{url}_redy", m.packb(frame))
		self.__redis.expire(f"{url}_redy", 10)
		self.__producer.produce("processed_images", value=f"{url}_redy".encode("utf-8"))
		self.__producer.poll(0)

	@logger.catch(level='INFO')
	def __analys_frame(self, batch_df, batch_id):
		"""Analyzes each batch of data from the streaming DataFrame.

		Args:
			batch_df: The batch DataFrame to be analyzed.
			batch_id: The ID of the batch.

		Returns:
			None
			
		"""
		data_collect = batch_df.collect()
		for data_row in data_collect:
			url = data_row["value"]
			try:
				self.__picture_recognition(url, m.unpackb(self.__redis.get(url)))
			except Exception as e:
				logger.error(e)

	@logger.catch(level='INFO')
	async def start(self):
		"""Starts the server and uses Apache Spark for streaming data analysis.

		This method creates a log file, starts the server, and uses Spark's writeStream to process incoming data with a custom function __analys_frame.
    
		Returns:
			None
			
		"""
		self.__create_log_file()
		logger.info("Start server_yolo_analys")
		query = self.__get_df_spark().writeStream \
			.foreachBatch(self.__analys_frame) \
			.outputMode("append") \
			.start()
		query.awaitTermination()

if __name__ == "__main__":
	time.sleep(20)
	with server_yolo_analys() as sya:
		run(sya.start())
else:
	logger.error("The server_yolo_analys.py module cannot be run by module")