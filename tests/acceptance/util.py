import subprocess
import time
import uuid

from kafka import KafkaClient
from kafka import KafkaConsumer
from kafka import SimpleProducer

ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def create_topic(topic_name, replication_factor, partitions):
    cmd = ['/usr/bin/kafka-topics', '--create',
           '--zookeeper', ZOOKEEPER_URL,
           '--replication-factor', str(replication_factor),
           '--partitions', str(partitions),
           '--topic', topic_name]
    subprocess.check_call(cmd)

    # It may take a little moment for the topic to be ready for writing.
    time.sleep(1)


def create_random_topic(replication_factor, partitions):
    topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def produce_example_msg(topic):
    kafka = KafkaClient(KAFKA_URL)
    producer = SimpleProducer(kafka)
    producer.send_messages(topic, b'some message')


def create_consumer_group(topic, group_name):
    consumer = KafkaConsumer(
        topic,
        group_id=group_name,
        auto_commit_enable=False,
        bootstrap_servers=[KAFKA_URL],
        auto_offset_reset='smallest')
    for message in consumer:
        consumer.task_done(message)
        break
    consumer.commit()
