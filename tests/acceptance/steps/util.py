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

from kafka import SimpleProducer
from kafka.common import LeaderNotAvailableError

from kafka_utils.util import config
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import set_consumer_offsets


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


def list_topics():
    cmd = ['kafka-topics.sh', '--list',
           '--zookeeper', ZOOKEEPER_URL]

    return call_cmd(cmd)


def delete_topic(topic_name):
    cmd = ['kafka-topics.sh', '--delete',
           '--zookeeper', ZOOKEEPER_URL,
           '--topic', topic_name]

    return call_cmd(cmd)


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


def create_random_group_id():
    return str(uuid.uuid1())


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


def create_consumer_group(topic, group_name, num_messages=1):
    client = KafkaToolClient(KAFKA_URL)
    set_consumer_offsets(
        client,
        group_name,
        {topic: {0: num_messages}},
        offset_storage='zookeeper',
        raise_on_error=True,
    )
    return client


def create_consumer_group_with_kafka_storage(topic, group_name):
    client = KafkaToolClient(KAFKA_URL)
    set_consumer_offsets(
        client,
        group_name,
        {topic: {0: 1}},
        offset_storage='kafka',
        raise_on_error=True,
    )
    return client


def get_consumer_offset(topics, group, storage='zookeeper'):
    client = KafkaToolClient(KAFKA_URL)
    return get_current_consumer_offsets(
        client,
        group,
        topics,
        storage
    )


def set_consumer_group_offset(topic, group, offset, storage='kafka'):
    client = KafkaToolClient(KAFKA_URL)
    set_consumer_offsets(
        client,
        group,
        {topic: {0: offset}},
        offset_storage=storage,
        raise_on_error=True,
    )


def call_watermark_get(topic_name, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--discovery-base-path', 'tests/acceptance/config',
           'get_topic_watermark', topic_name]
    return call_cmd(cmd)


def call_offset_get(group, storage=None, json=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
    if storage:
        cmd.extend(['--storage', storage])
    if json:
        cmd.extend(['--json'])
    return call_cmd(cmd)


def initialize_kafka_offsets_topic():
    if '__consumer_offsets' in list_topics():
        return
    topic = create_random_topic(1, 1)
    produce_example_msg(topic, num_messages=1)
    kafka = KafkaToolClient(KAFKA_URL)
    set_consumer_offsets(
        kafka,
        create_random_group_id(),
        {topic: {0: 1}},
        offset_storage='kafka',
        raise_on_error=True,
    )
    time.sleep(20)
