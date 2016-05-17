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
import json
import logging

from .command import ClusterManagerCmd
from kafka_utils.kafka_cluster_manager.cluster_info.stats import imbalance_value_all
from kafka_utils.util.validation import plan_to_assignment


class StatsCmd(ClusterManagerCmd):

    def __init__(self):
        super(StatsCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'stats',
            description='Show imbalance statistics of cluster topology',
            help='This command is used to display imbalance statistics of current '
            'cluster-topology or cluster-topology after given assignment is '
            'applied.',
        )
        subparser.add_argument(
            '--read-from-file',
            dest='plan_file_path',
            metavar='<reassignment-plan-file-path>',
            type=str,
            help='Read the partition assignment from json file. Example format:'
            ' {"version": 1, "partitions": [{"topic": "foo", "partition": 1, '
            '"replicas": [1,2,3]}]}',
        )
        return subparser

    def run_command(self, cluster_topology):
        if self.args.plan_file_path:
            base_assignment = cluster_topology.assignment

            print('Integrating given assignment-plan in current cluster-topology.')
            cluster_topology.update_cluster_topology(self.get_assignment())
            self.imbalance_stats(cluster_topology, base_assignment)
        else:
            self.imbalance_stats(cluster_topology)

    def log_imbalance_stats(self, imbal):
        net_imbalance = (
            imbal['replica_cnt'] +
            imbal['net_part_cnt_per_rg'] +
            imbal['topic_partition_cnt']
        )
        print(
            'Replication-group imbalance (replica-count): {imbal_repl}\n'
            'Net Partition-count imbalance/replication-group: '
            '{imbal_part_rg}\nNet Partition-count imbalance: {imbal_part}\n'
            'Topic-partition-count imbalance: {imbal_tp}\n'
            'Net-cluster imbalance (excluding leader-imbalance): '
            '{imbal_net}'.format(
                imbal_part_rg=imbal['net_part_cnt_per_rg'],
                imbal_repl=imbal['replica_cnt'],
                imbal_part=imbal['partition_cnt'],
                imbal_tp=imbal['topic_partition_cnt'],
                imbal_net=net_imbalance,
            )
        )
        net_imbalance_with_leaders = net_imbalance + imbal['leader_cnt']
        print(
            'Leader-count imbalance: {imbal_leader}\n'
            'Net-cluster imbalance (including leader-imbalance): '
            '{imbal}'.format(
                imbal=net_imbalance_with_leaders,
                imbal_leader=imbal['leader_cnt'],
            )
        )

        print(
            'Total partition-movements: {total_movements}'.format(
                total_movements=imbal['total_movements'],
            ),
        )

    def imbalance_stats(self, ct, base_assignment=None):
        print('Calculating cluster imbalance statistics...')
        initial_imbal = imbalance_value_all(ct, base_assignment)
        self.log_imbalance_stats(initial_imbal)
        total_imbal = (
            initial_imbal['replica_cnt'] +
            initial_imbal['net_part_cnt_per_rg'] +
            initial_imbal['topic_partition_cnt'] +
            initial_imbal['partition_cnt'] +
            initial_imbal['leader_cnt']
        )
        if total_imbal == 0:
            print('Cluster is currently balanced!')

    def get_assignment(self):
        """Parse the given json plan in dict format."""
        try:
            plan = json.loads(open(self.args.plan_file_path).read())
            return plan_to_assignment(plan)
        except IOError:
            self.log.exception(
                'Given json file {file} not found.'
                .format(file=self.args.plan_file_path),
            )
            raise
        except ValueError:
            self.log.exception(
                'Given json file {file} could not be decoded.'
                .format(file=self.args.plan_file_path),
            )
            raise
        except KeyError:
            self.log.exception(
                'Given json file {file} could not be parsed in desired format.'
                .format(file=self.args.plan_file_path),
            )
            raise
