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
            help='Maximum total size of the partitions moved in the final set of'
                 ' actions.'
                 ' DEFAULT: No limit. RECOMMENDATION: Should be at least max '
                 'partition-size across the cluster.',
        )
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        """Get executable proposed plan(if any) for display or execution."""
        base_assignment = cluster_topology.assignment
        cluster_balancer.rebalance()
        assignment = cluster_topology.assignment

        if not validate_plan(
            assignment_to_plan(assignment),
            assignment_to_plan(base_assignment),
        ):
            self.log.error('Invalid latest-cluster assignment. Exiting.')
            sys.exit(1)

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
