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
from __future__ import absolute_import

import argparse
import json

import requests

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import get_broker_id
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd


DEFAULT_JOLOKIA_PORT = 8778


class UnderReplicatedCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'under_replicated',
            description='Check under replicated partitions for all '
                        'brokers in cluster.',
            help='This command will sum all under replicated partitions '
                 'for each broker if any. It will query jolokia port for '
                 'receive this data.',
        )
        subparser.add_argument(
            '--first-broker-only',
            action='store_true',
            help='If this parameter is specified, it will do nothing and succeed '
                 'on not first brokers from Kafka cluster. Set --broker-id to -1 '
                 'to read broker-id from --data-path. Default: %(default)s',
        )
        subparser.add_argument(
            '--jolokia-port',
            type=convert_to_port,
            default=DEFAULT_JOLOKIA_PORT,
            help='Port to access to jolokia on each broker. Default: %(default)s',
        )

        return subparser

    def run_command(self):
        """Under_replicated command, checks number of under replicated partitions for
        all brokers in the Kafka cluster."""
        broker_list = self.zk.get_brokers()

        if self.args.first_broker_only:
            if self.args.broker_id is None:
                return status_code.WARNING, 'Broker id is not specified'

            if not check_run_on_first_broker(broker_list, self.args.broker_id, self.args.data_path):
                return status_code.OK, 'Provided broker is not the first in broker-list'

        under_replicated = get_under_replicated(broker_list, self.args.jolokia_port)

        if len(under_replicated) == 0:
            return status_code.OK, 'No under replicated partitions'
        else:
            count = 0
            for broker, stats in under_replicated.items():
                print('broker {broker} has {count}:'.format(broker=broker, count=len(stats)))

                for topic_partition in stats:
                    topic, partition = topic_partition
                    print('{broker} {topic}:{partition}'.format(
                        broker=broker, topic=topic, partition=partition))

                count += len(stats)

            msg = "{under_replicated} under replicated partitions".format(
                under_replicated=count
            )
            return status_code.CRITICAL, msg


def convert_to_port(string):
    """Convert string to valid port."""
    MIN_PORT_NUMBER = 0
    MAX_PORT_NUMBER = 65535
    error_msg = 'Unsigned short integer required, {string} given.'.format(string=string)
    try:
        value = int(string)
    except ValueError:
        raise argparse.ArgumentTypeError(error_msg)
    if value < MIN_PORT_NUMBER or value > MAX_PORT_NUMBER:
        raise argparse.ArgumentTypeError(error_msg)
    return value


def check_run_on_first_broker(broker_list, broker_id, data_path):
    """Returns true if the first broker in broker_list the same as in args."""
    broker_id = broker_id if broker_id != -1 else get_broker_id(data_path)

    brokers = sorted(broker_list.keys())

    return broker_id == brokers[0]


def parse_topic_partition(json_key):
    topic = 'UNKNOWN'
    partition = 'UNKNOWN'

    entries = json_key.split(',')
    for entry in entries:
        key, val = entry.split('=')
        if key == 'partition':
            partition = int(val)
        elif key == 'topic':
            topic = val

    return topic, partition


def get_under_replicated_from_broker(host, port):
    """Query one broker for info about under replicated partitions.

    :param string host: hostname of broker
    :param int port: jolokia port for query under replicated partitions number
    :returns list: tuples of topic, partition of under replicated partitions for the broker

        * list: [(topic, partition), ...]
    """
    UNDER_REPL_KEY = 'kafka.cluster:type=Partition,name=UnderReplicated,topic=*,partition=*'
    JOLOKIA_PREFIX = 'jolokia'

    url = 'http://{host}:{port}/{prefix}/read/{key}'.format(
        host=host,
        port=port,
        prefix=JOLOKIA_PREFIX,
        key=UNDER_REPL_KEY,
    )

    response = requests.get(url, timeout=10)

    response.raise_for_status()

    under_replicated = json.loads(response.content)['value']
    return [
        parse_topic_partition(key)
        for key, val in under_replicated.items()
        if int(val['Value']) == 1
    ]


def get_under_replicated(broker_list, port):
    """Go across each broker in broker_list and query under replicated partitions number.
    Then summarize them and return.

    :param dictionary broker_list: dictionary with brokers information, broker_id is key
    :param int port: jolokia port for query under replicated partitions number
    :returns dict: with under replicated topic-partition for each broker

        * dict: { broker: [(topic, partition), ...], ... }
    """

    under_replicated = {}
    for broker_info in broker_list.values():
        broker = broker_info['host']
        broker_stats = get_under_replicated_from_broker(broker, port)
        if len(broker_stats) == 0:
            continue
        under_replicated[broker] = broker_stats

    return under_replicated
