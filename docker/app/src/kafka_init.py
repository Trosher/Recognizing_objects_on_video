from confluent_kafka import Producer, Consumer

def get_producer(host):
    """Creates and returns a Kafka producer.

    Args:
        host (str): The host address for the Kafka bootstrap servers.

    Returns:
        Producer: A Kafka producer instance.

    """
    return Producer({'bootstrap.servers': host})

def get_consumer(host, topic, group_id):
    """Creates and returns a Kafka consumer.

    Args:
        host (str): The host address for the Kafka bootstrap servers.
        topic (str): The Kafka topic to subscribe to.
        group_id (str): The group ID for the consumer group.

    Returns:
        ñonsumer: A Kafka consumer instance subscribed to the specified topic.

    """
    consumer = Consumer({'bootstrap.servers': host, 
                     'group.id': group_id, 
                     'auto.offset.reset': 'earliest'})
    consumer.subscribe([topic])
    return consumer