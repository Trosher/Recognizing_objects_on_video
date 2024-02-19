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

message = '{"url":"http://77.73.54.134:88/webcapture.jpg?command=snap&channel=8?0.3060441690920612/"}'
#message = '{"url":"http://211.132.61.124:80/mjpg/video.mjpg"}'
producer.produce("url_video", value=message.encode('utf-8'), callback=delivery_callback)
producer.poll(0)