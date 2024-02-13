from confluent_kafka import Consumer, Producer, KafkaError
from re import match
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

conf_cons = {'bootstrap.servers': 'localhost:29092',
    'group.id': 'ui',
    'auto.offset.reset': 'earliest'
    }
consumer = Consumer(conf_cons)
consumer.subscribe(["processed_images"])

redis = Redis(host="127.0.0.1",port=10001)
msg = "1"
while msg:
    msg = consumer.poll(1.0)
    print(msg)
    frame = m.unpackb(redis.get(msg))
    print(frame)