from confluent_kafka import Consumer, Producer, KafkaError
from redis import Redis
from loguru import logger
from asyncio import run
import msgpack_numpy as m
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from typing import Any
from uvicorn import run
from starlette.responses import HTMLResponse
from os.path import realpath, dirname
from sys import platform
from PIL import Image
from datetime import datetime
from os import getcwd

class ui_video_server(FastAPI):
    @logger.catch
    def __init__(self, **extra: Any):
        super().__init__(**extra)
        
        slash = "\\" if (True if platform == "win32" or platform == "win64" else False) else "/"
        self.ui_path = dirname(realpath(__file__)) + slash + "ui" + slash + "page.html"
        conf_prod = {'bootstrap.servers': '172.18.0.3:29092'}
        conf_cons = {'bootstrap.servers': '172.18.0.3:29092',
            'group.id': 'geters',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': '86400000'
            }
        
        self.producer = Producer(conf_prod)
        self.consumer = Consumer(conf_cons)
        self.consumer.subscribe(["processed_images"])
        self.redis = Redis(host="172.18.0.6",port=10001)
        
        self.add_api_route("/", self.__get_root, methods=["GET"], 
                           include_in_schema=False)
        self.add_api_route("/videoPlayer/", self.__get_stream, methods=["GET"], 
                           include_in_schema=False)
        self.__create_log_file()

    def __check_system_win(self):
        return True if platform == "win32" or platform == "win64" else False

    def __get_time(self):
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    async def __create_log_file(self):
        slash = "\\" if self.__check_system_win() else "/"
        path_to_log = f"{getcwd()}{slash}logs{slash}ui_video_server_log{slash}runtime_ui_video_server_{self.__get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)

    @logger.catch
    async def __start_stream(self, url_for_upload: str):
        message = f'{{"url":"{url_for_upload}", "status":"start"}}'
        self.producer.produce("url_video", value=message.encode('utf-8'))
        self.producer.poll(0)
        logger.info(f"A message ({message}) has been forwarded to topick url_video")
        while True:
            msg = self.consumer.poll(timeout=5)
            if msg:
                for key in msg:
                    frame = m.unpackb(self.redis.get(key.decode('utf-8')))
                    try:
                        yield Image.fromarray(frame)
                    except Exception as e:
                        logger.error(e)
                        message = '{{"url":"{}, "status":"del"}}'.format(url_for_upload)
                        self.producer.produce("url_video", value=message.encode('utf-8'))
                        self.producer.poll(0)
                        break
            else:
                message = f'{{"url":"{url_for_upload}", "status":"del"}}'
                self.producer.produce("url_video", value=message.encode('utf-8'))
                self.producer.poll(0)
                break
            
    @logger.catch
    def __get_stream(self, url: str):
        logger.info(f"Adding a new url {url}")
        return StreamingResponse(self.__start_stream(url), status_code=200)
            
    @logger.catch
    def __get_root(self):
        request = ""
        try:
            with open(self.ui_path) as content:
                request = HTMLResponse(content=content.read(), status_code=200)
                logger.info(f"Given to the root page")
        except Exception as e:
            logger.error(e)
            request = JSONResponse(content={"message": "Page ui not found"}, status_code=501)
        return request

if __name__ == "__main__":
    app = ui_video_server(
        title="Ui for a server that displays objects on video",
        description="Description: You send a link to a frame stream and receive it with recognized objects in return.",
    )
    run(app, host="127.0.0.1", port=9997)
else:
    logger.error("The ui_video_server.py module cannot be run by module")