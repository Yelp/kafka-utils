# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
import time
import uuid

from kafka import KafkaConsumer
from kafka import SimpleProducer
from kafka.common import LeaderNotAvailableError

from kafka_utils.util import config
from kafka_utils.util.client import KafkaToolClient


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
    kafka = KafkaToolClient(KAFKA_URL)
    producer = SimpleProducer(kafka)
    for i in xrange(num_messages):
        try:
            producer.send_messages(topic, b'some message')
        except LeaderNotAvailableError:
            # Sometimes kafka takes a bit longer to assign a leader to a new
            # topic
            time.sleep(10)
            producer.send_messages(topic, b'some message')


def create_random_group_id():
    return str(uuid.uuid1())


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


def call_offset_get(group, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


def initialize_kafka_offsets_topic():
    topic = create_random_topic(1, 1)
    produce_example_msg(topic, num_messages=1)
    create_consumer_group(topic, 'foo')
    call_offset_get('foo', storage='kafka')
    time.sleep(10)
