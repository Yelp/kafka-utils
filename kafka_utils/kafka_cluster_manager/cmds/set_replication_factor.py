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
from kafka_utils.util import positive_nonzero_int


class SetReplicationFactorCmd(ClusterManagerCmd):

    def __init__(self):
        super().__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'set_replication_factor',
            description='Increase/decrease the replication factor of a topic.',
            help='This command is used to increase or decrease the replication'
            ' factor of a topic. The brokers that the replicas are added to or'
            ' removed from are chosen to maintain or increase the balance of'
            ' the cluster. The only exception is that out-of-sync replicas are'
            ' always removed before in-sync replicas.',
        )
        subparser.add_argument(
            '--topic',
            help='Kafka topic whose replication factor will be modified.',
            required=True,
        )
        subparser.add_argument(
            '--rf-mismatch',
            action='store_true',
            default=False,
            help='This will allow the command to run even if the cluster topology contains mismatches'
                 ' i.e. if there are partitions for the same topic that have different replication_factors',
        )
        subparser.add_argument(
            'replication_factor',
            help='The new replication factor for the topic.',
            type=positive_nonzero_int,
        )
        return subparser

    def run_command(self, ct, cluster_balancer):
        """Get executable proposed plan(if any) for display or execution."""
        if self.args.topic in ct.topics:
            topic = ct.topics[self.args.topic]
        else:
            self.log.error(
                "Topic {topic} not found. Exiting."
                .format(topic=self.args.topic),
            )
            sys.exit(1)

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

        # Fetch topic_data once
        topic_data = self.zk.get_topics(topic.id)[topic.id]
        partition_movement_count = 0

        for partition in topic.partitions:
            # Get the replication factor of that partition
            partition_rf = len(partition.replicas)
            changes_for_partition = abs(
                self.args.replication_factor - partition_rf
            )

            if partition_rf == self.args.replication_factor:
                continue

            if partition_rf < self.args.replication_factor:
                self.log.info(
                    "Increasing topic partition {topic}:{partition} replication factor from {old_rf} to "
                    "{new_rf}."
                    .format(
                        topic=topic.id,
                        partition=partition.partition_id,
                        old_rf=partition_rf,
                        new_rf=self.args.replication_factor,
                    ),
                )
                cluster_balancer.add_replica(
                    partition.name,
                    changes_for_partition,
                )
            else:
                self.log.info(
                    "Decreasing topic partition {topic}:{partition} replication factor from {old_rf} to "
                    "{new_rf}."
                    .format(
                        topic=topic.id,
                        partition=partition.partition_id,
                        old_rf=partition_rf,
                        new_rf=self.args.replication_factor,
                    ),
                )
                partition_data = topic_data['partitions'][str(partition.partition_id)]
                isr = partition_data['isr']
                osr_broker_ids = [b.id for b in partition.replicas if b.id not in isr]
                if osr_broker_ids:
                    self.log.info(
                        "The out of sync replica(s) {osr_broker_ids} will be "
                        "prioritized for removal."
                        .format(osr_broker_ids=osr_broker_ids)
                    )
                cluster_balancer.remove_replica(
                    partition.name,
                    osr_broker_ids,
                    changes_for_partition,
                )
            # Each replica addition/removal for each partition counts for one
            # partition movement
            partition_movement_count += changes_for_partition

        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            ct,
            max_partition_movements=partition_movement_count,
            max_leader_only_changes=0,
        )
        self.process_assignment(reduced_assignment, allow_rf_change=True, allow_rf_mismatch=self.args.rf_mismatch)
