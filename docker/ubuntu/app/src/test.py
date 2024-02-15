import sys
from loguru import logger
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:29092'}
producer = Producer(conf)

def delivery_callback(err, msg):
    if err:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

message = '{"user": 1, "url":"http://211.132.61.124:80/mjpg/video.mjpg"}'
producer.produce("url_video", value=message.encode('utf-8'), callback=delivery_callback)
producer.poll(0)