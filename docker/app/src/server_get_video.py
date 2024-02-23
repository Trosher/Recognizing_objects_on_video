from redis import Redis
from kafka_init import get_producer, get_consumer
import cv2

from asyncio import run, create_task
from asyncio import sleep as sleep_a

from json import loads
from os import getcwd
import msgpack_numpy as m

from datetime import datetime
import time

from loguru import logger

class server_get_vidio(object):
    """The get video server class."""
    @logger.catch(level='INFO')
    def __init__(self):
        """Initializes the video server.

        This method initializes the video server by setting initial values for the URL and stream, and establishing connections to the Kafka producer, Kafka consumer, and Redis.

        Returns:
            None
            
        """
        self.__url = None
        self.__stream = None
        self.__producer = get_producer('172.18.0.3:29092')
        self.__consumer = get_consumer('172.18.0.3:29092', 'url_video', 'geters')
        self.__redis = Redis(host="172.18.0.6", port=6379)

    @logger.catch(level='INFO')
    def __enter__(self):
        """Enters the context of the get video server.

        Returns:
            self: The UI video server object.

        """
        return self

    @logger.catch(level='INFO')
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Performs cleanup and exits the video server.

        This method releases the video stream, flushes the Kafka producer, closes the Kafka consumer and Redis connections, and exits the program.

        Args:
            exc_type: The type of the exception.
            exc_val: The exception value.
            exc_tb: The exception traceback.

        Returns:
            None
            
        """
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
        """Gets the current time and returns it in a specific format.

        Returns:
            str: The current time formatted as 'year-month-day-hour-minute-second'.

        """
        time = datetime.now()
        return f'{time.year}-{time.month}-{time.day}-{time.hour}-{time.minute}-{time.second}'

    @logger.catch(level='INFO')
    def __create_log_file(self):
        """Creates a log file for the get video server.
        
        Returns:
            None
        
        """
        path_to_log = f"{getcwd()}/logs/server_get_vidio_log/runtime_server_get_vidio_{self.__get_time()}.log"
        logger.add(path_to_log, retention="1 days")
        logger.info(path_to_log)

    @logger.catch(level='INFO')
    def __frame_sending(self, frame, url: str):
        """Sends the video frame to be analyzed and stores it in Redis.

        This method stores the video frame in Redis and sends a message to a Kafka topic for analysis.

        Args:
            frame (np.ndarray): The video frame to be sent for analysis.
            url (str): The URL of the video stream.

        Returns:
            None
        """
        self.__redis.set(url, m.packb(frame))
        self.__redis.expire(url, 10)
        self.__producer.produce("frame_to_analyze", value=f"{url}".encode('utf-8'))
        self.__producer.poll(0)

    def __get_frame(self, cap):
        """Generates frames from the video stream.

        This method continuously reads frames from the video stream and yields them.

        Args:
            cap: The video capture object.

        Yields:
            A tuple containing the return value and the frame from the video stream.
        """
        while True:
            ret, frame = cap.read()
            if ret:
                yield ret, frame
            else:
                yield None, None
    
    @logger.catch(level='INFO')
    async def __creater_stream(self, url: str):
        """Creates and processes a video stream from the given URL.

        This method opens a video capture object using the provided URL and continuously processes frames from the stream.

        Args:
            url (str): The URL of the video stream.

        Returns:
            None
            
        """
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
        """Listens to a Kafka topic for video URL messages.

        This method continuously polls the Kafka consumer for new messages containing video URLs.

        Yields:
            The video URL message received from the Kafka topic.
        """
        while True:
            msg = self.__consumer.poll(timeout=1)
            if msg:
                yield loads(msg.value().decode("utf-8"))
            else:
                yield None

    @logger.catch(level='INFO')
    async def __check_urls(self):
        """Checks for new video URLs and updates the current URL if necessary.

        This method continuously listens for messages from a Kafka topic containing video URLs. If a new URL is received, it updates the current URL.

        Returns:
            None
            
        """
        logger.info("Start __check_urls")
        for msg in self.__listener_kafka_topic_url_video():
            if msg and msg['url'] != self.__url:
                self.__url = msg['url']
            await sleep_a(0.3)

    async def __stream_controler(self):
        """Controls the video stream creation and processing.

        This method continuously checks for the availability of a URL and creates a task to create a stream from the URL.

        Returns:
            None
        
        """
        logger.info("Start __stream_controler")
        while True:
            if self.__url:
                logger.info(self.__url)
                task = create_task(self.__creater_stream(self.__url))
                await task
            await sleep_a(0.3)

    @logger.catch(level='INFO')
    async def start(self):
        """Starts the video server and its associated tasks.

        This method creates a log file, starts the video server, and runs tasks for checking URLs and controlling the video stream.

        Returns:
            None
            
        """
        self.__create_log_file()
        logger.info("Start get_video_server")
        tasks = [create_task(self.__check_urls()), create_task(self.__stream_controler())]
        for task in tasks:
            await task
        while True:
            pass

if __name__ == "__main__":
    time.sleep(20)
    with server_get_vidio() as ssgv:
        run(ssgv.start())
else:
    logger.error("The get_vidio_server.py module cannot be run by module")
    