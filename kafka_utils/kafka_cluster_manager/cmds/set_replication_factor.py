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
import logging
import sys

from .command import ClusterManagerCmd


class SetReplicationFactorCmd(ClusterManagerCmd):

    def __init__(self):
        super(SetReplicationFactorCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'set_replication_factor',
            description='Increase/decrease the replication factor of a topic.',
            help='This command is used to increase or decrease the replication'
            ' factor of a topic. The brokers that the replicas are added to or'
            ' removed from are chosen to maintain or increase the balance of'
            ' the cluster.',
        )
        subparser.add_argument(
            '--topic',
            help='Kafka topic whose replication factor will be modified.',
            required=True,
        )
        subparser.add_argument(
            'replication_factor',
            help='The new replication factor for the topic.',
            type=self.positive_nonzero_int,
        )
        return subparser

    def run_command(self, ct):
        """Get executable proposed plan(if any) for display or execution."""
        if self.args.topic in ct.topics:
            topic = ct.topics[self.args.topic]
        else:
            self.log.error(
                "Topic {topic} not found. Exiting."
                .format(topic=self.args.topic),
            )
            sys.exit(1)

        if topic.replication_factor == self.args.replication_factor:
            self.log.info(
                "Topic {topic} already has replication factor {rf}. "
                "No action to perform."
                .format(topic=topic.id, rf=self.args.replication_factor),
            )
            return

        if self.args.replication_factor > len(ct.brokers):
            self.log.error(
                "Replication factor {rf} is greater than the total number of "
                "brokers {brokers}. Exiting."
                .format(
                    rf=self.args.replication_factor,
                    brokers=len(ct.brokers)
                ),
            )
            sys.exit(1)

        base_assignment = ct.assignment

        if topic.replication_factor < self.args.replication_factor:
            self.log.info(
                "Increasing topic {topic} replication factor from {old_rf} to "
                "{new_rf}."
                .format(
                    topic=topic.id,
                    old_rf=topic.replication_factor,
                    new_rf=self.args.replication_factor,
                ),
            )
            for partition in topic.partitions:
                ct.add_replica(
                    partition,
                    self.args.replication_factor - partition.replication_factor,
                )
        else:
            self.log.info(
                "Decreasing topic {topic} replication factor from {old_rf} to "
                "{new_rf}."
                .format(
                    topic=topic.id,
                    old_rf=topic.replication_factor,
                    new_rf=self.args.replication_factor,
                ),
            )
            for partition in topic.partitions:
                ct.remove_replica(
                    partition,
                    partition.replication_factor - self.args.replication_factor,
                )

        assignment = ct.assignment

        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            assignment,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )
        self.process_assignment(reduced_assignment, allow_rf_change=True)
