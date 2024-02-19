from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
import redis
import json
import confluent_kafka
import msgpack_numpy as m

redis_client = redis.Redis(host="172.18.0.6", port=6379)

schema = StructType([
    StructField("url", StringType(), True)
])

def write_to_redis(row):
    conf_p = {'bootstrap.servers': '172.18.0.3:29092'}
    producer = confluent_kafka.Producer(conf_p)
    redis_client = redis.Redis(host="172.18.0.6", port=6379)

    stream = cv2.VideoCapture(json.loads(row.value.decode('utf-8'))["url"])
    ret, frame = stream.read()
    
    producer.produce("frame_to_analyze", value="aga".encode('utf-8'))
    producer.poll(1)
    redis_client.set(json.loads(row.value.decode('utf-8'))["url"], m.packb(frame))

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

kafka_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "172.18.0.3:29092") \
  .option("subscribe", "url_video") \
  .option("startingOffsets", "latest") \
  .load()

query = kafka_stream.writeStream.foreach(write_to_redis).start()

query.awaitTermination()