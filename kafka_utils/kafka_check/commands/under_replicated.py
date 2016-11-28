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
from kafka_utils.util.metadata import get_topic_partition_with_error
from kafka_utils.util.metadata import REPLICA_NOT_AVAILABLE_ERROR


class UnderReplicatedCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'under_replicated',
            description='Check under replicated partitions for all '
                        'brokers in cluster.',
            help='This command will fail if there are any under replicated '
                 'partitions in the cluster.',
        )

        return subparser

    def run_command(self):
        """Under_replicated command, checks number of under replicated partitions for
        all brokers in the Kafka cluster."""
        under_replicated = get_topic_partition_with_error(
            self.cluster_config,
            REPLICA_NOT_AVAILABLE_ERROR,
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
