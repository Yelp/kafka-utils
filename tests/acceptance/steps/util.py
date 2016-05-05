import subprocess
import time
import uuid

from kafka import KafkaClient
from kafka import KafkaConsumer
from kafka import SimpleProducer

from yelp_kafka_tool.util import config


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def get_cluster_config():
    return config.get_cluster_config(
        'test',
        'test_cluster',
        'tests/acceptance/config',
    )


def create_topic(topic_name, replication_factor, partitions):
    cmd = ['kafka-topics.sh', '--create',
           '--zookeeper', ZOOKEEPER_URL,
           '--replication-factor', str(replication_factor),
           '--partitions', str(partitions),
           '--topic', topic_name]
    subprocess.check_call(cmd)

    # It may take a little moment for the topic to be ready for writing.
    time.sleep(1)


def call_cmd(cmd):
    output = ''
    try:
        p = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate('y')
        if out:
            output += out
        if err:
            output += err
    except subprocess.CalledProcessError as e:
        output += e.output
    return output


def create_random_topic(replication_factor, partitions, topic_name=None):
    if not topic_name:
        topic_name = str(uuid.uuid1())
    create_topic(topic_name, replication_factor, partitions)
    return topic_name


def produce_example_msg(topic, num_messages=1):
    kafka = KafkaClient(KAFKA_URL)
    producer = SimpleProducer(kafka)
    for i in xrange(num_messages):
        producer.send_messages(topic, b'some message')


def create_consumer_group(topic, group_name, num_messages=1):
    consumer = KafkaConsumer(
        topic,
        group_id=group_name,
        auto_commit_enable=False,
        bootstrap_servers=[KAFKA_URL],
        auto_offset_reset='smallest')
    for i in xrange(num_messages):
        message = consumer.next()
        consumer.task_done(message)
    consumer.commit()
    return consumer
