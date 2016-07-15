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

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import get_broker_id
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_metadata


# This check will look on lines with that error-code in error field
# from kafka metadata response.
REPLICA_NOT_AVAILABLE_ERROR = 9


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
            self.cluster_config.broker_list
        )

        if not under_replicated:
            return status_code.OK, 'No under replicated partitions.'
        else:
            if self.args.verbose:
                for (topic, partition) in under_replicated:
                    print('{topic}:{partition}'.format(
                        topic=topic,
                        partition=partition,
                    ))

            msg = "{under_replicated} under replicated partitions.".format(
                under_replicated=len(under_replicated),
            )
            return status_code.CRITICAL, msg


def _check_run_on_first_broker(broker_list, broker_id, data_path):
    """Returns true if the first broker in broker_list the same as in args."""
    broker_id = broker_id if broker_id != -1 else get_broker_id(data_path)

    first_broker_id, _ = min(broker_list.items())

    return broker_id == first_broker_id


def _process_topic_partition_metadata(topic_partitions_metadata):
    """Return set with under replicated partitions."""
    under_replicated = set()
    for partitions in topic_partitions_metadata.values():
        for metadata in partitions.values():
            if int(metadata.error) == REPLICA_NOT_AVAILABLE_ERROR:
                under_replicated.add((metadata.topic, metadata.partition))

    return under_replicated


def _get_under_replicated(broker_list):
    """Requests kafka-broker for metadata info for topics.
    Then checks if topic-partition is under replicated and there are not enough
    replicas in sync. Returns set of under replicated partitions.

    :param dictionary broker_list: dictionary with brokers information, broker_id is key
    :returns set: with under replicated partitions

        * set: { (topic, partition), ... }
    """
    metadata = get_topic_partition_metadata(broker_list)

    return _process_topic_partition_metadata(metadata)
