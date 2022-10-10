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
from kafka_utils.util import positive_int
from kafka_utils.util.validation import assignment_to_plan
from kafka_utils.util.validation import validate_plan


DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


class ReplaceBrokerCmd(ClusterManagerCmd):

    def __init__(self):
        super().__init__()
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
            default=None,
            help='Broker id of destination broker. If empty, will shrink the replica set of topics',
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
            '--rf-change',
            action='store_true',
            default=False,
            help='This will allow the replication factor to change (if removing a broker)'
        )
        subparser.add_argument(
            '--rf-mismatch',
            action='store_true',
            default=False,
            help='This will allow the replication factor to mismatch for partitions of a same topic (if removing a broker)'
        )
        subparser.add_argument(
            '--topic-partition-filter',
            type=str,
            help='Path to file containing a colon separated list of topic partitions to work on.'
                 ' This is useful if you only want to operate on a limited set of topic partitions'
                 ' Observe that kafka-check outputs colon separated lists, after ommitting the first $n'
                 ' lines e.g. kafka-check offline | tail -n+$N',
        )
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        if self.args.source_broker == self.args.dest_broker:
            print("Error: Destination broker is same as source broker.")
            sys.exit()
        if self.args.dest_broker is None:
            self.log.warning('This will shrink the replica set of topics.')

        base_assignment = cluster_topology.assignment
        cluster_topology.replace_broker(self.args.source_broker, self.args.dest_broker)

        if not validate_plan(
            assignment_to_plan(cluster_topology.assignment),
            assignment_to_plan(base_assignment),
            allow_rf_change=self.args.rf_change,
            allow_rf_mismatch=self.args.rf_mismatch,
        ):
            self.log.error('Invalid assignment %s.', cluster_topology.assignment)
            print(
                f'Invalid assignment: {cluster_topology.assignment}',
                file=sys.stderr,
            )
            sys.exit(1)

        # Reduce the proposed assignment based on the topic_partition_filter, if provided
        if self.args.topic_partition_filter:
            self.log.info("Using provided filter list")
            filter_set = self.get_topic_filter()
            filtered_assignment = {}
            for t_p, replica in base_assignment.items():
                if t_p in filter_set:
                    filtered_assignment[t_p] = replica
            base_assignment = filtered_assignment

        # Reduce the proposed assignment based on max_partition_movements
        # and max_leader_changes
        reduced_assignment = self.get_reduced_assignment(
            base_assignment,
            cluster_topology,
            self.args.max_partition_movements,
            self.args.max_leader_changes,
        )
        if reduced_assignment:
            self.process_assignment(reduced_assignment, allow_rf_change=self.args.rf_change, allow_rf_mismatch=self.args.rf_mismatch)
        else:
            self.log.info("Broker already replaced. No more replicas in source broker.")
            print("Broker already replaced. No more replicas in source broker.")
