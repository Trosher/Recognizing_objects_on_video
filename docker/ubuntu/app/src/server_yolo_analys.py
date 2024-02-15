from confluent_kafka import Consumer, Producer, KafkaError
from loguru import logger
from asyncio import run, create_task
from signal import signal, SIGINT, SIGTERM
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from sys import platform
from os import getcwd
import yolov5

class server_yolo_analys():
	def __init__(self):             
		self.model = yolov5.load('yolov5s.pt')
		conf_prod = {'bootstrap.servers': 'localhost:29092'}
		conf_cons = {'bootstrap.servers': 'localhost:29092',
            'group.id': 'analysis',
            'auto.offset.reset': 'earliest'
            }
		self.producer = Producer(conf_prod)
		self.consumer = Consumer(conf_cons)
		self.consumer.subscribe(["frame_to_analyze"])
		self.redis = Redis(host="127.0.0.1",port=10001)
	
	def __check_system_win(self):
		return True if platform == "win32" or platform == "win64" else False

	def __get_time(self):
		time = datetime.now()
		return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

	@logger.catch(level='INFO')
	async def __create_log_file(self):
		slash = "\\" if self.__check_system_win() else "/"
		path_to_log = f"{getcwd()}{slash}logs{slash}server_yolo_analys_log{slash}runtime_server_yolo_analys_{self.__get_time()}.log"
		logger.add(path_to_log, retention="1 days")
		logger.info(path_to_log)

	@logger.catch
	def __end_work(self, a, b):
		self.consumer.close()
		logger.info("End work get_vidio_server")
		exit(0)
	
	@logger.catch
	def __message_verification(self, msg):
		status = True
		if msg.error():
			if msg.error().code() == KafkaError._PARTITION_EOF:
				logger.warning(f'Reached end of topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
			else:
				logger.error(f'Error occured: {msg.error()}')
			status = False
		return status

	@logger.catch
	async def __frame_sending(self, url):
		frame = self.model(m.unpackb(self.redis.get(url)))
		while frame:
			self.redis.set(f"{url}_redy", m.packb(frame.render()))
			self.redis.expire(f"{url}_redy", 10)
			self.producer.poll(0)
			frame = self.model(m.unpackb(self.redis.get(url)))

	@logger.catch
	async def __analyzing_and_sending_frame(self, msg):
		self.producer.produce("processed_images", value=f"{msg}_redy".encode('utf-8'))
		self.producer.poll(0)
		task = create_task(self.__frame_sending(msg))
		await task
		
	@logger.catch
	async def start(self):
		await self.__create_log_file()
		logger.info("Start server_yolo_analys")
		signal(SIGINT, self.__end_work)
		signal(SIGTERM, self.__end_work)
		while True:
			msg = self.consumer.poll(1.0)
			if msg and self.__message_verification(msg):
				task = create_task(self.__analyzing_and_sending_frame(msg.value().decode('utf-8')))
				await task

def start_server_yolo_analys():
    server = server_yolo_analys()
    run(server.start())

if __name__ == "__main__":
    start_server_yolo_analys()
    #logger.error("The get_vidio_server.py module cannot be run by itself, use the start_program.py")
else:
    start_server_yolo_analys()