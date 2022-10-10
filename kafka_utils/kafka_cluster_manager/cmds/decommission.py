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

import humanfriendly

from .command import ClusterManagerCmd
from .command import DEFAULT_MAX_MOVEMENT_SIZE
from kafka_utils.util import positive_float
from kafka_utils.util import positive_int
from kafka_utils.util.validation import assignment_to_plan
from kafka_utils.util.validation import validate_plan


DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


class DecommissionCmd(ClusterManagerCmd):

    def __init__(self):
        super().__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'decommission',
            description='Decommission one or more brokers of the cluster.',
            help='This command is used to move all the replicas assigned to given '
            'brokers and redistribute them across all the other brokers while '
            'trying to keep the cluster balanced.',
        )
        subparser.add_argument(
            'broker_ids',
            nargs='+',
            type=int,
            help='Broker ids of the brokers to decommission.',
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

        movement_size_group = subparser.add_mutually_exclusive_group()
        movement_size_group.add_argument(
            '--max-movement-size',
            type=positive_float,
            default=DEFAULT_MAX_MOVEMENT_SIZE,
            help='Maximum total size of the partitions moved in the final set'
                 ' of actions. Since each PartitionMeasurer implementation'
                 ' defines its own notion of size, the size unit to use will'
                 ' depend on  the selected PartitionMeasurer implementation.'
                 ' DEFAULT: No limit.'
                 ' RECOMMENDATION: Should be at least the maximum partition-size'
                 ' on the brokers to decommission, ideally a little larger.',
        )
        movement_size_group.add_argument(
            '--auto-max-movement-size',
            action='store_true',
            help='Set max-movement-size to the size of the largest partition'
                 ' in the set of brokers to be decommissioned.',
        )
        subparser.add_argument(
            '--force-progress',
            action='store_true',
            help='If set, ensures that at minimum one partition is moved regardless'
                 ' of --max-movement-size set.',
        )
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        # If the max_movement_size is still default, then the user did not input a value for it
        if self.args.force_progress and self.args.max_movement_size == DEFAULT_MAX_MOVEMENT_SIZE:
            self.log.error(
                '--force-progress must be used with --max-movement-size',
            )
            sys.exit(1)
        # Obtain the largest partition in the set of partitions we will move
        partitions_to_move = set()
        for broker in self.args.broker_ids:
            partitions_to_move.update(cluster_topology.brokers[broker].partitions)

        largest_size = max(
            [partition.size for partition in partitions_to_move] or (1, )
        )

        smallest_size = min(
            [partition.size for partition in partitions_to_move] or (0, )
        )

        if self.args.auto_max_movement_size:
            self.args.max_movement_size = largest_size
            self.log.info(
                'Auto-max-movement-size: using {human_max_movement_size} ({max_movement_size}) as'
                ' max-movement-size.'.format(
                    human_max_movement_size=humanfriendly.format_size(self.args.max_movement_size),
                    max_movement_size=self.args.max_movement_size,
                )
            )

        if self.args.max_movement_size and self.args.max_movement_size < largest_size:
            if not self.args.force_progress:
                self.log.error(
                    'Max partition movement size is only {max_movement_size},'
                    ' but remaining partitions to move range from {smallest_size} to'
                    ' {largest_size}. The decommission will not make progress'.format(
                        max_movement_size=humanfriendly.format_size(self.args.max_movement_size),
                        smallest_size=humanfriendly.format_size(smallest_size),
                        largest_size=humanfriendly.format_size(largest_size),
                    )
                )
                sys.exit(1)
            else:
                self.log.warning(
                    'Max partition movement size is only {max_movement_size},'
                    ' but remaining partitions to move range from {smallest_size} to'
                    ' {largest_size}. The decommission may be slower than expected'.format(
                        max_movement_size=humanfriendly.format_size(self.args.max_movement_size),
                        smallest_size=humanfriendly.format_size(smallest_size),
                        largest_size=humanfriendly.format_size(largest_size),
                    )
                )

        base_assignment = cluster_topology.assignment

        cluster_balancer.decommission_brokers(self.args.broker_ids)

        if not validate_plan(
            assignment_to_plan(cluster_topology.assignment),
            assignment_to_plan(base_assignment),
        ):
            self.log.error('Invalid assignment %s.', cluster_topology.assignment)
            print(
                f'Invalid assignment: {cluster_topology.assignment}',
                file=sys.stderr,
            )
            sys.exit(1)

        # Reduce the proposed assignment based on max_partition_movements
        # and max_leader_changes
        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            cluster_topology,
            self.args.max_partition_movements,
            self.args.max_leader_changes,
            max_movement_size=self.args.max_movement_size,
            force_progress=self.args.force_progress,
        )
        if reduced_assignment:
            self.process_assignment(reduced_assignment)
        else:
            msg_str = "Cluster already balanced. No more replicas in decommissioned brokers."
            self.log.info(msg_str)
            print(msg_str)
