from asyncio import run, create_task
from asyncio import sleep as sleep_a
from loguru import logger
import cv2
from kafka_init import get_producer, get_consumer
from json import loads
import msgpack_numpy as m
from redis import Redis
from datetime import datetime
from os import getcwd

class server_get_vidio(object):   
    @logger.catch(level='INFO')
    def __init__(self):
        self.__url = None
        self.__stream = None
        self.__producer = get_producer('172.18.0.3:29092')
        self.__consumer = get_consumer('172.18.0.3:29092', 'url_video', 'geters')
        self.__redis = Redis(host="172.18.0.6", port=6379)

    @logger.catch(level='INFO')
    def __enter__(self):
        return self

    @logger.catch(level='INFO')
    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("End work get_vidio_server")
        try:
            self.__stream.release()
        except:
            pass
        self.__producer.flush()
        self.__consumer.close()
        self.__redis.close()
        exit(0)

    @logger.catch(level='INFO')
    def __get_time(self):
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    def __create_log_file(self):
        path_to_log = f"{getcwd()}/logs/server_get_vidio_log/runtime_server_get_vidio_{self.__get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)

    @logger.catch(level='INFO')
    def __frame_sending(self, frame, url):
        self.__redis.set(url, m.packb(frame))
        self.__redis.expire(url, 10)
        self.__producer.produce("frame_to_analyze", value=f"{url}".encode('utf-8'))
        self.__producer.poll(0)

    def __get_frame(self, cap):
        while True:
            ret, frame = cap.read()
            if ret:
                yield ret, frame
            else:
                yield None, None
    
    @logger.catch(level='INFO')
    async def __creater_stream(self, url):
        logger.info(f"Start thred for pars url: {url}")
        try:
            cap = cv2.VideoCapture(url)
            self.__stream = cap
            for ret, frame in self.__get_frame(cap):
                if not url == self.__url:
                    break
                if ret:
                    self.__frame_sending(frame, url)
                else:
                    logger.error("Unknown problems with frame intake")
                    self.__stream = None
                    self.__url = None
                    cap.release()
                    break
                await sleep_a(0.03)
        except Exception as e:
            logger.error(e)

    @logger.catch(level='INFO')
    def __listener_kafka_topic_url_video(self):
        while True:
            msg = self.__consumer.poll(timeout=1)
            if msg:
                yield loads(msg.value().decode("utf-8"))
            else:
                yield None

    @logger.catch(level='INFO')
    async def __check_urls(self):
        logger.info("Start __check_urls")
        for msg in self.__listener_kafka_topic_url_video():
            if msg and msg['url'] != self.__url:
                self.__url = msg['url']
            await sleep_a(0.3)

    async def __stream_controler(self):
        logger.info("Start __stream_controler")
        while True:
            if self.__url:
                logger.info(self.__url)
                task = create_task(self.__creater_stream(self.__url))
                await task
            await sleep_a(0.3)

    @logger.catch(level='INFO')
    async def start(self):
        self.__create_log_file()
        logger.info("Start get_video_server")
        tasks = [create_task(self.__check_urls()), create_task(self.__stream_controler())]
        for task in tasks:
            await task
        while True:
            pass

if __name__ == "__main__":
    with server_get_vidio() as ssgv:
        run(ssgv.start())
else:
    logger.error("The get_vidio_server.py module cannot be run by module")
    