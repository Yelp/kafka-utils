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
from __future__ import print_function

import logging
import sys

import six

from .command import ClusterManagerCmd
from kafka_utils.kafka_cluster_manager.cluster_info.display \
    import display_cluster_topology_stats
from kafka_utils.kafka_cluster_manager.cluster_info.stats \
    import get_replication_group_imbalance_stats
from kafka_utils.util import positive_float
from kafka_utils.util import positive_int
from kafka_utils.util.validation import assignment_to_plan
from kafka_utils.util.validation import validate_plan


DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


class RebalanceCmd(ClusterManagerCmd):

    def __init__(self):
        super(RebalanceCmd, self).__init__()
        self.log = logging.getLogger('ClusterRebalance')

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'rebalance',
            description='Rebalance cluster by moving partitions across brokers '
            'and changing the preferred replica.',
            help='This command is used to rebalance a Kafka cluster. Based on '
            'the given flags this tool will generate and submit a reassinment '
            'plan that will evenly distribute partitions and leaders '
            'across the brokers of the cluster. The replication groups option '
            'moves the replicas of the same partition to separate replication '
            'making the cluster resilient to the failure of one of more zones.'
        )
        subparser.add_argument(
            '--replication-groups',
            action='store_true',
            help='Evenly distributes replicas over replication-groups.',
        )
        subparser.add_argument(
            '--brokers',
            action='store_true',
            help='Evenly distributes partitions optimally over brokers'
            ' with minimal movements for each replication-group.',
        )
        subparser.add_argument(
            '--leaders',
            action='store_true',
            help='Evenly distributes leaders optimally over brokers.',
        )
        subparser.add_argument(
            '--max-partition-movements',
            type=positive_int,
            default=DEFAULT_MAX_PARTITION_MOVEMENTS,
            help='Maximum number of partition-movements in final set of actions.'
                 ' DEFAULT: %(default)s. RECOMMENDATION: Should be at least max '
                 'replication-factor across the cluster.',
        )
        subparser.add_argument(
            '--max-leader-changes',
            type=positive_int,
            default=DEFAULT_MAX_LEADER_CHANGES,
            help='Maximum number of actions with leader-only changes.'
                 ' DEFAULT: %(default)s',
        )
        subparser.add_argument(
            '--max-movement-size',
            type=positive_float,
            default=None,
            help='Maximum total size of the partitions moved in the final set'
                 ' of actions. Since each PartitionMeasurer implementation'
                 ' defines its own notion of size, the size unit to use will'
                 ' depend on  the selected PartitionMeasurer implementation.'
                 ' DEFAULT: No limit.'
                 ' RECOMMENDATION: Should be at least the maximum partition-size'
                 ' on the cluster.',
        )
        subparser.add_argument(
            '--auto-max-movement-size',
            action='store_true',
            help='Set max-movement-size to the size of the largest partition'
                 ' in the cluster.',
        )
        subparser.add_argument(
            '--show-stats',
            action='store_true',
            help='Output post-rebalance cluster topology stats.',
        )
        subparser.add_argument(
            '--score-improvement-threshold',
            type=positive_float,
            default=None,
            help='The minimum required improvement in cluster topology score'
            ' for an assignment to be applied. Default: None',
        )
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        """Get executable proposed plan(if any) for display or execution."""

        # The ideal weight of each broker is total_weight / broker_count.
        # It should be possible to remove partitions from each broker until
        # the weight of the broker is less than this ideal value, otherwise it
        # is impossible to balance the cluster. If --max-movement-size is too
        # small, exit with an error.
        if self.args.max_movement_size:
            total_weight = sum(
                partition.weight
                for partition in six.itervalues(cluster_topology.partitions)
            )
            broker_count = len(cluster_topology.brokers)
            optimal_weight = total_weight / broker_count

            broker, max_unmovable_on_one_broker = max((
                (broker, sum(
                    partition.weight
                    for partition in broker.partitions
                    if partition.size > self.args.max_movement_size
                ))
                for broker in cluster_topology.brokers.values()),
                key=lambda t: t[1],
            )

            if max_unmovable_on_one_broker >= optimal_weight:
                sorted_partitions = sorted(
                    [
                        partition
                        for partition in broker.partitions
                        if partition.size > self.args.max_movement_size
                    ],
                    reverse=True,
                    key=lambda partition: partition.size,
                )

                for partition in sorted_partitions:
                    max_unmovable_on_one_broker -= partition.weight
                    if max_unmovable_on_one_broker <= optimal_weight:
                        required_max_movement_size = partition.size
                        break

                self.log.error(
                    'Max movement size {max_movement_size} is too small, it is'
                    ' not be possible to balance the cluster. A max movement'
                    ' size of {required} or higher is required.'.format(
                        max_movement_size=self.args.max_movement_size,
                        required=required_max_movement_size,
                    )
                )
                sys.exit(1)
        elif self.args.auto_max_movement_size:
            self.args.max_movement_size = max(
                partition.size
                for partition in six.itervalues(cluster_topology.partitions)
            )
            self.log.info(
                'Auto-max-movement-size: using {max_movement_size} as'
                ' max-movement-size.'.format(
                    max_movement_size=self.args.max_movement_size,
                )
            )

        base_assignment = cluster_topology.assignment
        base_score = cluster_balancer.score()
        rg_imbalance, _ = get_replication_group_imbalance_stats(
            list(cluster_topology.rgs.values()),
            list(cluster_topology.partitions.values())
        )

        cluster_balancer.rebalance()

        assignment = cluster_topology.assignment
        score = cluster_balancer.score()
        new_rg_imbalance, _ = get_replication_group_imbalance_stats(
            list(cluster_topology.rgs.values()),
            list(cluster_topology.partitions.values())
        )

        if self.args.show_stats:
            display_cluster_topology_stats(cluster_topology, base_assignment)
            if base_score is not None and score is not None:
                print('\nScore before: %f' % base_score)
                print('Score after:  %f' % score)
                print('Score improvement: %f' % (score - base_score))

        if not validate_plan(
            assignment_to_plan(assignment),
            assignment_to_plan(base_assignment),
        ):
            self.log.error('Invalid latest-cluster assignment. Exiting.')
            sys.exit(1)

        if self.args.score_improvement_threshold:
            if base_score is None or score is None:
                self.log.error(
                    '%s cannot assign scores so --score-improvement-threshold'
                    ' cannot be used.',
                    cluster_balancer.__class__.__name__,
                )
                return
            else:
                score_improvement = score - base_score
                if score_improvement >= self.args.score_improvement_threshold:
                    self.log.info(
                        'Score improvement %f is greater than the threshold %f.'
                        ' Continuing to apply the assignment.',
                        score_improvement,
                        self.args.score_improvement_threshold,
                    )
                elif new_rg_imbalance < rg_imbalance:
                    self.log.info(
                        'Score improvement %f is less than the threshold %f,'
                        ' but replica balance has improved. Continuing to'
                        ' apply the assignment.',
                        score_improvement,
                        self.args.score_improvement_threshold,
                    )
                else:
                    self.log.info(
                        'Score improvement %f is less than the threshold %f.'
                        ' Assignment will not be applied.',
                        score_improvement,
                        self.args.score_improvement_threshold,
                    )
                    return

        # Reduce the proposed assignment based on max_partition_movements
        # and max_leader_changes
        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            assignment,
            self.args.max_partition_movements,
            self.args.max_leader_changes,
        )
        if reduced_assignment:
            self.process_assignment(reduced_assignment)
        else:
            self.log.info("Cluster already balanced. No actions to perform.")
