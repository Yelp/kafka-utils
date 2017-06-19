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


DEFAULT_MAX_LEADER_CHANGES = 5


class RevokeLeadershipCmd(ClusterManagerCmd):

    def __init__(self):
        super(RevokeLeadershipCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'revoke-leadership',
            description='Re-assign leadership for all partitions on given brokers to other brokers',
            help='This command is used to move leadership for all partitions '
            'on given brokers to other brokers in balanced order. The generated plan include'
            ' only leadership changes.'
        )
        subparser.add_argument(
            'broker_ids',
            nargs='+',
            type=int,
            help='Broker ids of the brokers to revoke leadership for.',
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
        base_assignment = cluster_topology.assignment

        cluster_balancer.revoke_leadership(self.args.broker_ids)

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

        # Reduce the proposed assignment based on max_leader_changes
        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            cluster_topology.assignment,
            0,  # Number of partition movements
            self.args.max_leader_changes,
        )
        if reduced_assignment:
            self.process_assignment(reduced_assignment)
        else:
            msg = "Cluster already balanced. No more partitions as leaders in " \
                "revoked-leadership brokers."
            self.log.info(msg)
            print(msg)
