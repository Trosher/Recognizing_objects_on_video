from confluent_kafka import Producer, Consumer

def get_producer(host):
    return Producer({'bootstrap.servers': host})

def get_consumer(host, topic, group_id):
    consumer = Consumer({'bootstrap.servers': host, 
                     'group.id': group_id, 
                     'auto.offset.reset': 'earliest'})
    consumer.subscribe([topic])
    return consumer