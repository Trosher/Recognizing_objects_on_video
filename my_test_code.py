import sys
from loguru import logger
from confluent_kafka import Producer

conf = {'bootstrap.servers':'172.18.0.3:29092'}
producer = Producer(conf)

def delivery_callback(err, msg):
    if err:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

message = '{"user": 1, "url":"http://211.132.61.124:80/mjpg/video.mjpg"}'
producer.produce("url_video", value=message.encode('utf-8'), callback=delivery_callback)
producer.poll(0)

from asyncio import run, create_task
from signal import signal, SIGINT, SIGTERM
from loguru import logger
import cv2
from confluent_kafka import Consumer, Producer, KafkaError
from json import loads
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from sys import platform
from os import getcwd
from pyspark import SparkContext, SparkConf

# dobavit funkcional otklucenia stream

class server_get_vidio(object):
    def __init__(self):             
        self.urls = {}
        self.last_url = None
        conf_prod = {'bootstrap.servers': '172.18.0.3:29092'}
        conf_cons = {'bootstrap.servers': '172.18.0.3:29092',
            'group.id': 'geters',
            'auto.offset.reset': 'earliest'
            }
        self.producer = Producer(conf_prod)
        self.consumer = Consumer(conf_cons)
        self.consumer.subscribe(["url_video"])
        self.redis = Redis(host="172.18.0.6", port=6379)

    def __get_time(self):
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    async def __create_log_file(self):
        path_to_log = f"{getcwd()}/logs/server_get_vidio_log/runtime_server_get_vidio_{self.__get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)

    @logger.catch(level='INFO')
    def __end_work(self, a, b):
        for url in self.urls:
            self.urls[url].release()
        cv2.destroyAllWindows()
        self.consumer.close()
        logger.info("End work get_vidio_server")
        exit(0)
    
    @logger.catch(level='INFO')
    async def __del_user(self, url):
        logger.info(f"User deletion {url}")
        self.urls[url].release()
        del self.urls[url]
    
    @logger.catch(level='INFO')
    def __retrieving_dir_from_message(self, msg):
        if msg['url']:
            logger.info(f"Adding a new url {msg['url']}")
            self.urls[msg['url']] = cv2.VideoCapture(msg['url'])
            self.last_url = msg['url']
        #else:
        #    task = create_task(self.__del_user(msg['url']))
        #    await task
        #    self.last_url = None

    @logger.catch(level='INFO')
    def __working_with_message(self, msg):
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.warning(f'Reached end of topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
            else:
                logger.error(f'Error occured: {msg.error()}')
        else:
            logger.info(f"New message ({msg.value()}) received from the url_video topic")
            self.__retrieving_dir_from_message(loads(msg.value()))
    
    @logger.catch(level='INFO')
    async def __frame_sending(self, frame, url):
        if not frame.any() == None:
            self.redis.set(url, m.packb(frame))
            self.redis.expire(url, 10)
            self.producer.produce("frame_to_analyze", value=f"{url}".encode('utf-8'))
            self.producer.poll(0)

    @logger.catch(level='INFO')
    async def __get_frame(self, url):
        while True:
            ret, frame = self.urls[url].read()
            if ret:
                task_frame_sending = create_task(self.__frame_sending(frame, url))
                await task_frame_sending
            else:
                logger.error("Error: Could not read frame")
                break
        task_del_user = create_task(self.__del_user(url))
        await task_del_user

    @logger.catch(level='INFO')
    async def __create_stream(self):
        if not self.urls[self.last_url].isOpened():
            logger.error("Error: Could not open video stream")
            # add message about this situation
            del self.urls[self.last_url]
        else:
            task_get_frame = create_task(self.__get_frame(self.last_url))
            await task_get_frame
            
    @logger.catch(level='INFO')
    async def start(self):
        await self.__create_log_file()
        logger.info("Start get_video_server")
        signal(SIGINT, self.__end_work)
        signal(SIGTERM, self.__end_work)
        task_create_stream = create_task(self.__create_stream())
        while True:
            msg = self.consumer.poll(1.0)
            if msg:
                self.__working_with_message(msg.decode('utf-8'))
                if self.last_url:
                    await task_create_stream

def start_server_get_vidio():
    server = server_get_vidio()
    run(server.start())
    
if __name__ == "__main__":
    start_server_get_vidio()
    #logger.error("The get_vidio_server.py module cannot be run by itself, use the start_program.py")
else:
    start_server_get_vidio()
    