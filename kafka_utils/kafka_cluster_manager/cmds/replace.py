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

from .command import ClusterManagerCmd
from kafka_utils.util import positive_int
from kafka_utils.util.validation import assignment_to_plan
from kafka_utils.util.validation import validate_plan


DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


class ReplaceBrokerCmd(ClusterManagerCmd):

    def __init__(self):
        super(ReplaceBrokerCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'replace-broker',
            description='Replace the given broker with new broker by moving all partitions.',
            help='This command is used to move all the replicas assigned to a given '
            'broker to destination broker. No change in cluster-imbalance state',
        )
        subparser.add_argument(
            '--source-broker',
            type=int,
            required=True,
            help='Broker id of source broker.',
        )
        subparser.add_argument(
            '--dest-broker',
            type=int,
            required=True,
            help='Broker id of destination broker.',
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
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        if self.args.source_broker == self.args.dest_broker:
            print("Error: Destination broker is same as source broker.")
            sys.exit()

        base_assignment = cluster_topology.assignment
        cluster_topology.replace_broker(self.args.source_broker, self.args.dest_broker)

        if not validate_plan(
            assignment_to_plan(cluster_topology.assignment),
            assignment_to_plan(base_assignment),
        ):
            self.log.error('Invalid assignment %s.', cluster_topology.assignment)
            print(
                'Invalid assignment: {0}'.format(cluster_topology.assignment),
                file=sys.stderr,
            )
            sys.exit(1)

        # Reduce the proposed assignment based on max_partition_movements
        # and max_leader_changes
        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            cluster_topology.assignment,
            self.args.max_partition_movements,
            self.args.max_leader_changes,
        )
        if reduced_assignment:
            self.process_assignment(reduced_assignment)
        else:
            self.log.info("Broker already replaced. No more replicas in source broker.")
            print("Broker already replaced. No more replicas in source broker.")
