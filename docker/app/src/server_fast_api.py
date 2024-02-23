from kafka_init import get_producer, get_consumer
from redis import Redis

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from uvicorn import run as u_run

import time
from os import getcwd
from asyncio import sleep

import msgpack_numpy as m
from typing import Any
from PIL import Image
from datetime import datetime
import io

from loguru import logger

# 192.168.0.217
# http://211.132.61.124:80/mjpg/video.mjpg
class ui_video_server(FastAPI):
    """The UI video server class."""
    @logger.catch
    def __init__(self, **extra: Any):
        """Initializes the UI video server.

        Args:
            **extra: Additional keyword arguments.
            
        Returns:
            None

        """
        logger.info("Start work ui_video_server")
        super().__init__(**extra)
        self.templates = Jinja2Templates(directory=f"{getcwd()}/ui/")
        self.render_url = None

        self.__producer = get_producer('172.16.0.3:29092')
        self.__consumer = get_consumer('172.16.0.3:29092', 'processed_images', 'ui')
        self.__redis = Redis(host="172.16.0.6", port=6379)
        
        self.add_api_route(path="/", endpoint=self.__get_root, methods=["GET"], include_in_schema=False)
        self.add_api_route(path="/videoPlayer", endpoint=self.__get_stream, methods=["GET"], include_in_schema=False)
        self.add_api_route(path="/uploadUrl/", endpoint=self.__upload_url, methods=["POST"], include_in_schema=False)
        self.__create_log_file()

    @logger.catch(level='INFO')
    def __enter__(self):
        """Enters the context of the UI video server.

        Returns:
            self: The UI video server object.

        """
        return self

    @logger.catch(level='INFO')
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Handles the exit of the UI video server.

        Args:
            exc_type: The type of the exception.
            exc_val: The value of the exception.
            exc_tb: The traceback of the exception.
            
        Returns:
            None

        """
        logger.info("End work ui_video_server")
        self.__redis.close()
        self.__consumer.close()
        self.__producer.flush()
        exit(0)

    def __get_time(self):
        """Gets the current time and returns it in a specific format.

        Returns:
            str: The current time formatted as 'year-month-day-hour-minute-second'.

        """
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    def __create_log_file(self):
        """Creates a log file for the UI video server.
        
        Returns:
            None
        
        """
        path_to_log = f"{getcwd()}/logs/ui_video_server_log/runtime_ui_video_server_{self.__get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)

    @logger.catch(level='INFO')
    async def __listener_kafka_topic_processed_images(self):
        """Listens for messages on the Kafka topic for processed images.

        Yields:
            str: The message received from the Kafka topic, decoded as UTF-8.

        """
        while True:
            msg = self.__consumer.poll(timeout=1)
            if msg:
                yield msg.value().decode("utf-8")
            else:
                yield None
            await sleep(0.01)

    @logger.catch
    async def __start_stream(self):
        """Starts the video stream.

        Yields:
            bytes: A frame of the video stream as bytes.

        """
        async for msg in self.__listener_kafka_topic_processed_images():
            try:
                if msg:
                    frame = self.__redis.get(msg)
                    if frame:
                        image = Image.fromarray(m.unpackb(frame))
                        with io.BytesIO() as output:
                            image.save(output, format='JPEG')
                            image_bytes = output.getvalue()
                        yield (b'--frame\r\n'
                               b'Content-Type: image/jpeg\r\n\r\n' + image_bytes + b'\r\n')
            except Exception as e:
                logger.error(e)
                break
            await sleep(0.01)

    @logger.catch
    async def __get_stream(self):
        """Returns a streaming response for the video stream.

        Returns:
            StreamingResponse: A streaming response for the video stream.

        """
        return StreamingResponse(self.__start_stream(), media_type='multipart/x-mixed-replace; boundary=frame')

    @logger.catch
    async def __upload_url(self, url: str):
        """Handles the upload of a new URL.

        Args:
            url (str): The URL to be uploaded.
            
        Returns:
            None

        """
        self.render_url = url
        message = f'{{"url":"{url}"}}'
        self.__producer.produce("url_video", value=message.encode('utf-8'))
        self.__producer.poll(0)
        logger.info(f"A message ({message}) has been forwarded to topick url_video")
            
    @logger.catch
    async def __get_root(self, request: Request):
        """Handler for the root endpoint.

        Args:
            request (Request): The incoming request object.

        Returns:
            TemplateResponse: The HTML template response for the root page.

        """
        logger.info(f"Given to the root page")
        return self.templates.TemplateResponse('page.html', {'request': request})

if __name__ == "__main__":
    time.sleep(20)
    with ui_video_server(
        title="Ui for a server that displays objects on video", 
        description="Description: You send a link to a frame stream and receive it with recognized objects in return.",
    ) as app:
        u_run(app, host="0.0.0.0", port=9999)
else:
    logger.error("The ui_video_server.py module cannot be run by module")