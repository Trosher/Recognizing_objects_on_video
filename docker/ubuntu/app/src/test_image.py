import sys
from loguru import logger
from confluent_kafka import Consumer, Producer
from redis import Redis
import msgpack_numpy as m
from time import sleep

conf_prod = {'bootstrap.servers': '172.18.0.3:29092'}
conf_cons = {'bootstrap.servers': '172.18.0.3:29092',
    'group.id': 'ui',
    'auto.offset.reset': 'earliest'
    }

producer = Producer(conf_prod)
consumer = Consumer(conf_cons)
consumer.subscribe(["processed_images"])
redis = Redis(host="172.18.0.6",port=10001)

conf = {'bootstrap.servers':'172.18.0.3:29092'}
producer = Producer(conf)

def delivery_callback(err, msg):
    if err:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

#message = '{"url":"http://77.73.54.134:88/webcapture.jpg?command=snap&channel=8?0.3060441690920612/", "status":"start"}'
message = 'http://211.132.61.124:80/mjpg/video.mjpg'
producer.produce("frame_to_analyze", value=message.encode('utf-8'), callback=delivery_callback)
producer.poll(0)