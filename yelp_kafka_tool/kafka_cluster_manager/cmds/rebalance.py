import logging
import sys

from .command import ClusterManagerCmd
from yelp_kafka_tool.kafka_cluster_manager. \
    cluster_info.stats import imbalance_value_all
from yelp_kafka_tool.util.validation import assignment_to_plan
from yelp_kafka_tool.util.validation import validate_plan


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
            'plan that will evently distributed partitions and leaders '
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
            type=self.positive_int,
            default=DEFAULT_MAX_PARTITION_MOVEMENTS,
            help='Maximum number of partition-movements in final set of actions.'
                 ' DEFAULT: %(default)s. RECOMMENDATION: Should be at least max '
                 'replication-factor across the cluster.',
        )
        subparser.add_argument(
            '--max-leader-changes',
            type=self.positive_int,
            default=DEFAULT_MAX_LEADER_CHANGES,
            help='Maximum number of actions with leader-only changes.'
                 ' DEFAULT: %(default)s',
        )
        return subparser

    def run_command(self, ct):
        """Get executable proposed plan(if any) for display or execution."""
        base_assignment = ct.assignment
        assignment = self.build_balanced_assignment(base_assignment, ct)

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

    def build_balanced_assignment(self, base_assignment, ct):
        # Get initial imbalance statistics
        initial_imbal = self.pre_balancing_imbalance_stats(base_assignment, ct)

        # Balancing to be done in the given order only
        # Rebalance replication-groups
        if self.args.replication_groups:
            self.log.info(
                'Re-balancing replica-count over replication groups: %s',
                ', '.join(ct.rgs.keys()),
            )
            ct.rebalance_replication_groups()
            self.replication_group_rebalance_stats(ct)

        # Rebalance broker-partition count per replication-groups
        if self.args.brokers:
            self.log.info(
                'Re-balancing partition-count across brokers: %s',
                ', '.join(str(e) for e in ct.brokers.keys()),
            )
            ct.rebalance_brokers()
            self.broker_rebalance_stats(ct, initial_imbal)

        # Rebalance broker as leader count per broker
        if self.args.leaders:
            self.log.info(
                'Re-balancing leader-count across brokers: %s',
                ', '.join(str(e) for e in ct.brokers.keys()),
            )
            ct.rebalance_leaders()
            self.final_rebalance_stats(ct, initial_imbal, self.args.leaders)

        return ct.assignment

    # Imbalance statistics evaluation and reporting
    def pre_balancing_imbalance_stats(self, base_assignment, ct):
        self.log.info('Calculating initial rebalance imbalance statistics...')
        initial_imbal = imbalance_value_all(base_assignment, ct)
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
        return initial_imbal

    def replication_group_rebalance_stats(self, ct):
        self.log.info(
            'Calculating rebalance imbalance-stats after rebalancing '
            'replica-count over replication-groups...',
        )
        curr_imbal = imbalance_value_all(ct, leaders=False)
        self.log.info(
            'Imbalance statistics after rebalancing replica-count over '
            'replication-groups'
        )
        self.log_imbalance_stats(curr_imbal, leaders=False)
        # Assert that replication-groups balanced
        assert(curr_imbal['replica_cnt'] == 0), (
            'Replication-group imbalance count is non-zero: {imbal}'
            .format(imbal=curr_imbal['replica_cnt']),
        )

    def broker_rebalance_stats(self, ct, initial_imbal):
        self.log.info(
            'Calculating rebalance imbalance-stats after rebalancing brokers...',
        )
        curr_imbal = imbalance_value_all(ct, leaders=False)
        self.log_imbalance_stats(curr_imbal, leaders=False)
        if curr_imbal['net_part_cnt_per_rg'] > 0:
            # Report as warning if replication-groups didn't rebalance
            self.log.error(
                'Partition-count over brokers imbalance count is non-zero: '
                '{imbal}'.format(imbal=curr_imbal['net_part_cnt_per_rg']),
            )
        # Assert that replication-group imbalance should not increase
        assert(
            curr_imbal['net_part_cnt_per_rg'] <=
            initial_imbal['net_part_cnt_per_rg']), (
            'Partition-count imbalance count increased from '
            '{initial_imbal} to {curr_imbal}'.format(
                initial_imbal=initial_imbal['net_part_cnt_per_rg'],
                curr_imbal=curr_imbal['net_part_cnt_per_rg'],
            )
        )

    def final_rebalance_stats(self, ct, initial_imbal, leaders_balanced=False):
        self.log.info('Calculating final rebalance imbalance-stats... ')
        curr_imbal = imbalance_value_all(ct)
        self.log_imbalance_stats(curr_imbal)
        # Verify leader-imbalance only if balanced
        if leaders_balanced:
            if curr_imbal['leader_cnt'] > 0:
                # Report as warning if replication-groups didn't rebalance
                ct.log.warning(
                    'Leader-count over brokers imbalance count is non-zero: '
                    '{imbal}'.format(imbal=curr_imbal['leader_cnt']),
                )
            # Assert that leader-imbalance should not increase
            assert(curr_imbal['leader_cnt'] <= initial_imbal['leader_cnt']), (
                'Leader-count imbalance count increased from '
                '{initial_imbal} to {curr_imbal}'.format(
                    initial_imbal=initial_imbal['leader_cnt'],
                    curr_imbal=curr_imbal['leader_cnt'],
                )
            )

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
        self.log.info(
            'Total partition-movements: {movement_cnt}'
            .format(movement_cnt=imbal['total_movements']),
        )
