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
import json
import subprocess
import time
import uuid

import six
from kafka import SimpleProducer
from kafka.common import LeaderNotAvailableError

from kafka_utils.util import config
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import set_consumer_offsets


ZOOKEEPER_URL = 'zookeeper:2181'
KAFKA_URL = 'kafka:9092'


def load_json(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        data = bytes_or_str.decode()
    else:
        data = bytes_or_str

    return json.loads(data)


def get_cluster_config():
    return config.get_cluster_config(
        'test',
        'test_cluster',
        'tests/acceptance/config',
    )


def update_topic_config(topic_name, config):
    cmd = ['kafka-topics.sh', '--alter',
           '--zookeeper', ZOOKEEPER_URL,
           '--topic', topic_name,
           '--config', config]
    subprocess.check_call(cmd)


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

    communicate_input = b'y'

    try:
        p = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate(communicate_input)
    except subprocess.CalledProcessError as e:
        output += e.output

    if six.PY3:
        if out:
            output += out.decode()
        if err:
            output += err.decode()
    else:
        if out:
            output += out
        if err:
            output += err

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
    for i in range(num_messages):
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
        raise_on_error=True,
    )
    return client


def set_consumer_group_offset(topic, group, offset):
    client = KafkaToolClient(KAFKA_URL)
    set_consumer_offsets(
        client,
        group,
        {topic: {0: offset}},
        raise_on_error=True,
    )


def call_watermark_get(topic_name, regex=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--discovery-base-path', 'tests/acceptance/config',
           'get_topic_watermark', topic_name]
    if regex:
        cmd.append('-r')
    return call_cmd(cmd)


def call_offset_get(group, json=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
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
        raise_on_error=True,
    )
    time.sleep(20)
