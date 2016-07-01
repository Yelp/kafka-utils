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

from collections import defaultdict

from kafka import KafkaClient

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import get_broker_id
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd


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
            '--minimum-replication',
            type=int,
            default=2,
            help='Minimum number of in-sync replicas for under replicated partition. '
                 'Default: %(default)s',
        )

        return subparser

    def run_command(self):
        """Under_replicated command, checks number of under replicated partitions for
        all brokers in the Kafka cluster."""
        broker_list = self.zk.get_brokers()

        if self.args.first_broker_only:
            if self.args.broker_id is None:
                return status_code.WARNING, 'Broker id is not specified'

            if not _check_run_on_first_broker(broker_list, self.args.broker_id, self.args.data_path):
                return status_code.OK, 'Provided broker is not the first in broker-list.'

        under_replicated = _get_under_replicated(
            broker_list,
            self.args.minimum_replication,
        )

        if not under_replicated:
            return status_code.OK, 'No under replicated partitions.'
        else:
            count = 0
            for broker_id, stats in under_replicated.items():
                broker = broker_list[broker_id]['host']
                print(
                    'broker {broker} has {count} under-replicated partitions'.format(
                        broker=broker,
                        count=len(stats),
                    )
                )

                for topic_partition in stats:
                    topic, partition = topic_partition
                    if self.args.verbose:
                        print('{broker} {topic}:{partition}'.format(
                            broker=broker,
                            topic=topic,
                            partition=partition,
                        ))

                count += len(stats)

            msg = "{under_replicated} under replicated partitions.".format(
                under_replicated=count
            )
            return status_code.CRITICAL, msg


def _check_run_on_first_broker(broker_list, broker_id, data_path):
    """Returns true if the first broker in broker_list the same as in args."""
    broker_id = broker_id if broker_id != -1 else get_broker_id(data_path)

    first_broker_id, _ = min(broker_list.items())

    return broker_id == first_broker_id


def _get_topic_partition_metadata(hosts):
    """Returns topic-partition metadata from Kafka broker."""
    kafka_client = KafkaClient(hosts, timeout=10)
    return kafka_client.topic_partitions


def _prepare_host_list(broker_list):
    """Returns string with broker_list hosts, compatible format with KafkaClient ctor.
    String format:

        * string: '{host}:{port}, ...'
    """
    return ','.join(
        [
            '{host}:{port}'.format(host=broker_info['host'], port=broker_info['port'])
            for broker_info in broker_list.values()
        ]
    )


def _process_topic_partition_metadata(topic_partitions_metadata, min_replication):
    """Return dict with under replicated topic-partition for each broker"""
    under_replicated = defaultdict(list)
    for partitions in topic_partitions_metadata.values():
        for metadata in partitions.values():
            not_in_sync = set(metadata.replicas) - set(metadata.isr)
            if len(not_in_sync) > 0 and len(metadata.isr) < min_replication:
                for broker in not_in_sync:
                    under_replicated[broker].append((metadata.topic, metadata.partition))

    return under_replicated


def _get_under_replicated(broker_list, min_replication):
    """Requests kafka-broker for metadata info for topics.
    Then checks if topic-partition is under replicated and there are not enough
    replicas in sync. Returns dict of under replicated partitions grouped by brokers.

    :param dictionary broker_list: dictionary with brokers information, broker_id is key
    :param int default_replication: minimun in sync replicas
    :returns dict: with under replicated topic-partition for each broker

        * dict: { broker: [(topic, partition), ...], ... }
    """
    metadata = _get_topic_partition_metadata(
        _prepare_host_list(broker_list)
    )

    return _process_topic_partition_metadata(metadata, min_replication)
