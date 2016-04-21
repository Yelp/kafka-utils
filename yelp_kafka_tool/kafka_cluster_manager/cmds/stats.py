import json
import logging

from .command import ClusterManagerCmd
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import imbalance_value_all


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
        if self.args.assignment_json:
            plan = self.get_plan()
            base_assignment = cluster_topology.assignment
            cluster_topology.update_cluster_topology(plan)
            self.imbalance_stats(cluster_topology, base_assignment)
        else:
            self.imbalance_stats(cluster_topology)

    def log_imbalance_stats(self, imbal):
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
        net_imbalance_with_leaders = net_imbalance + imbal['leader_cnt']
        self.log.info(
            'Leader-count imbalance: {imbal_leader}\n'
            'Net-cluster imbalance (including leader-imbalance): '
            '{imbal}'.format(
                imbal=net_imbalance_with_leaders,
                imbal_leader=imbal['leader_cnt'],
            )
        )

        self.log.info(
            'Total partition-movements: {movement_cnt}'
            .format(movement_cnt=imbal['total_movements']),
        )

    def imbalance_stats(self, ct, base_assignment=None):
        self.log.info('Calculating rebalance imbalance statistics...')
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
            self.log.info('Cluster is currently balanced!')

    def get_plan(self):
        try:
            return json.loads(open(self.args.assignment_json).read())
        except IOError:
            self.log.exception(
                'Given json file {file} not found.'
                .format(file=self.args.assignment_json),
            )
            raise
        except ValueError:
            self.log.exception(
                'Given json file {file} could not be decoded.'
                .format(file=self.args.assignment_json),
            )
            raise
