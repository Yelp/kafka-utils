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
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_metadata


class OfflineCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'offline',
            description='Check offline partitions on the specified broker',
            help='This command will sum all under replicated partitions '
                 'for each broker if any. It will query jolokia port for '
                 'receive this data.',
        )

        return subparser

    def run_command(self):
        """Checks the number of offline partitions"""
        offline = _get_offline_partitions(self.cluster_config)

        if not offline:
            return status_code.OK, 'No offline partitions.'
        else:
            if self.args.verbose:
                for (topic, partition) in offline:
                    print('{topic}:{partition}'.format(
                        topic=topic,
                        partition=partition,
                    )
                    )

            msg = "{offline_n} offline partitions.".format(
                offline_n=len(offline),
            )
            return status_code.CRITICAL, msg


def _get_offline_partitions(cluster_config):
    """Return set with under replicated partitions."""

    metadata = get_topic_partition_metadata(cluster_config.broker_list)
    offline = set()
    for partitions in metadata.values():
        for partition_metadata in partitions.values():
            if int(partition_metadata.error) == 5:
                offline.add((partition_metadata.topic, partition_metadata.partition))

    return offline
