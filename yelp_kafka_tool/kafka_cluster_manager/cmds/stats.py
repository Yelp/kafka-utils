from __future__ import print_function

import json
import logging

from .command import ClusterManagerCmd
from yelp_kafka_tool.kafka_cluster_manager. \
    cluster_info.cluster_topology import ClusterTopology
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import imbalance_value_all
from yelp_kafka_tool.kafka_cluster_manager. \
    replication_group.yelp_group import extract_yelp_replication_group
from yelp_kafka_tool.util.zookeeper import ZK


class StatsCmd(ClusterManagerCmd):

    def __init__(self):
        super(StatsCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'stats',
            description='Show imbalance statistics of cluster topology',
            help='This command is used to display imbalance stats of current '
            'cluster-topology or cluster-topology after given assignment is '
            'applied',
        )
        subparser.add_argument(
            '--assignment-json',
            type=str,
            help='json file path of assignment to be used over current cluster-'
            'topology for calculating stats in the format {"version": 1, '
            '"partitions": [{"topic": "foo", "partition": 1, "replicas": [1,2,3]'
            '}]',
        )
        return subparser

    def run_command(self, cluster_topology):
        self.initial_imbalance_stats(cluster_topology)

    def log_imbalance_stats(self, imbal, leaders=True):
        net_imbalance = (
            imbal['replica_cnt'] +
            imbal['net_part_cnt_per_rg'] +
            imbal['topic_partition_cnt']
        )
        self.log.info(
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
        if leaders:
            net_imbalance_with_leaders = net_imbalance + imbal['leader_cnt']
            self.log.info(
                'Leader-count imbalance: {imbal_leader}\n'
                'Net-cluster imbalance (including leader-imbalance): '
                '{imbal}'.format(
                    imbal=net_imbalance_with_leaders,
                    imbal_leader=imbal['leader_cnt'],
                )
            )

    def initial_imbalance_stats(self, ct):
        self.log.info('Calculating rebalance imbalance statistics...')
        initial_imbal = imbalance_value_all(ct)
        self.log_imbalance_stats(initial_imbal)
        total_imbal = (
            initial_imbal['replica_cnt'] +
            initial_imbal['net_part_cnt_per_rg'] +
            initial_imbal['topic_partition_cnt'] +
            initial_imbal['partition_cnt'] +
            initial_imbal['leader_cnt']
        )
        if total_imbal == 0:
            self.log.info('Cluster is currently balanced!')

    def run(self, cluster_config, args):
        self.cluster_config = cluster_config
        self.args = args

        with ZK(self.cluster_config) as self.zk:
            self.log.info(
                'Starting %s for cluster: %s and zookeeper: %s',
                self.__class__.__name__,
                self.cluster_config.name,
                self.cluster_config.zookeeper,
            )
            brokers = self.zk.get_brokers()
            assignment = self.zk.get_cluster_assignment()
            if args.assignment_json:
                try:
                    proposed_plan = json.loads(open(args.assignment_json).read())
                except IOError:
                    self.log.error(
                        'Given json file {file} not found.'
                        .format(file=args.assignment_json),
                    )
                    raise
                except ValueError:
                    self.log.error(
                        'Given json file {file} could not be decoded.'
                        .format(file=args.assignment_json),
                    )
                    raise

                for ele in proposed_plan['partitions']:
                    t_p = (ele['topic'], ele['partition'])
                    if t_p in assignment.keys():
                        assignment[t_p] = ele['replicas']
                    else:
                        self.log.error(
                            'Invalid topic partition {t_p} in given assignment'
                            .format(t_p=t_p),
                        )
            ct = ClusterTopology(assignment, brokers, extract_yelp_replication_group)
            self.run_command(ct)
