from asyncio import run, create_task
import asyncio
from signal import signal, SIGINT, SIGTERM
from loguru import logger
import cv2
from confluent_kafka import Producer, Consumer
from json import loads
from redis import Redis
import msgpack_numpy as m
from datetime import datetime
from os import getcwd
from pyspark.sql import SparkSession

class server_get_vidio(object):   
    @logger.catch(level='INFO')
    def __init__(self):
        self.__urls = set()
        self.__streams = []
        conf_prod = {'bootstrap.servers': '172.18.0.3:29092'}
        conf_cons = {'bootstrap.servers': '172.18.0.3:29092',
            'group.id': 'server_get_vidio',
            'auto.offset.reset': 'earliest',
			'max.poll.interval.ms': '86400000'
			}
        self.__producer = Producer(conf_prod)
        self.__consumer = Consumer(conf_cons)
        self.__consumer.subscribe(["url_video"])
        self.__redis = Redis(host="172.18.0.6", port=6379)

    @logger.catch(level='INFO')
    def __enter__(self):
        return self

    @logger.catch(level='INFO')
    def __get_time(self):
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    def __create_log_file(self):
        path_to_log = f"{getcwd()}/logs/server_get_vidio_log/runtime_server_get_vidio_{get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)


    @logger.catch(level='INFO')
    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("End work get_vidio_server")
        for stream in self.__streams:
            stream.release()
        self.__consumer.close()
        self.__redis.close()

    @logger.catch(level='INFO')
    async def __frame_sending(self, frame, url):
        await asyncio.sleep(0.01)
        self.__redis.set(url, m.packb(frame))
        self.__redis.expire(url, 10)
        self.__producer.produce("frame_to_analyze", value=f"{url}".encode('utf-8'))
        self.__producer.poll(0)

    @logger.catch(level='INFO')
    async def __create_stream(self, url):
        logger.info(f"Start __create_stream for {url}")
        stream = cv2.VideoCapture(url)
        self.__streams.append(stream)
        while url in self.__urls:
            ret, frame = stream.read()
            if ret:
                task = create_task(self.__frame_sending(frame, url))
                await task
            else:
                logger.error("Error: Could not read frame")
                try:
                    logger.info(f"Delite url {url}")
                    self.__urls.remove(url)
                except Exception as e:
                    logger.error(e)
                break
            await asyncio.sleep(0.03)
        stream.release()

    @logger.catch(level='INFO')
    async def __add_and_del_url(self):
        logger.info("Start __add_and_del_url")
        while True:
            msg = self.__consumer.poll(1.0)
            if msg:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.warning(f'Reached end of topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
                    else:
                        logger.error(f'Error occured: {msg.error()}')
                else:
                    msg = loads(msg.value())
                    url = msg['url']
                    status = msg['status']
                    if url and status == "start":
                        logger.info(f"Add new url {url}")
                        self.__urls.add(url)
                        task = create_task(self.__create_stream(url))
                        await task
                    elif url and status == "del":
                        try:
                            logger.info(f"Delite url {url}")
                            self.__urls.remove(url)
                        except Exception as e:
                            logger.error(e)

    @logger.catch(level='INFO')
    async def start(self):
        logger.info("Start get_video_server")
        task = create_task(self.__add_and_del_url())
        await task

if __name__ == "__main__":
    with server_get_vidio() as ssgv:
        run(ssgv.start())
else:
    logger.error("The get_vidio_server.py module cannot be run by module")
    