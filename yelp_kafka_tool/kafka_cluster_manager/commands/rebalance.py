import logging
import sys

from .commands.command import ClusterManagerCmd


DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


class RebalanceCmd(ClusterManagerCmd):

    def __init__(self):
        super(RebalanceCmd, self).__init__()
        self.log = logging.getLogger('ClusterRebalance')

    def add_subparser(self, subparser):
        subparser = subparsers.add_parser(
            'rebalance',
            description='Re-assign partitions over brokers.',
        )
        subparser.add_argument(
            '--replication-groups',
            action='store_true',
            help='Evenly distributes replicas over replication-groups.',
        )
        subparser.add_argument(
            '--leaders',
            action='store_true',
            help='Evenly distributes leaders optimally over brokers.',
        )
        subparser.add_argument(
            '--brokers',
            action='store_true',
            help='Evenly distributes partitions optimally over brokers'
            ' with minimal movements for each replication-group.',
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
        subparser.add_argument(
            '--apply',
            action='store_true',
            help='Proposed-plan will be executed on confirmation.',
        )
        subparser.add_argument(
            '--no-confirm',
            action='store_true',
            help='Proposed-plan will be executed without confirmation.'
                 ' --apply flag also required.',
        )
        subparser.add_argument(
            '--proposed-plan-json',
            dest='proposed_plan_file',
            metavar='<reassignment-plan-file-path>',
            type=str,
            help='Export candidate partition reassignment configuration '
                 'to given json file.',
        )
        subparser.add_argument(
            '--skip-'
        subparser.set_defaults(command=self.reassign_partitions)

    def reassign_partitions(self, cluster_config, args):
        """Get executable proposed plan(if any) for display or execution."""
        with ZK(cluster_config) as zk:
            self.log.info(
                'Starting re-assignment tool for cluster: {c_name} and zookeeper: '
                '{zookeeper}'.format(
                    c_name=cluster_config.name,
                    zookeeper=cluster_config.zookeeper,
                )
            )
            if is_reassignment_pending(zk):
                self.log.error('Previous reassignment pending. Exiting...')
                sys.exit(1)

            ct = ClusterTopology(zk=zk)
            rebalance_layers(
                ct,
                rebalance_replication_groups=args.replication_groups,
                rebalance_brokers=args.brokers,
                rebalance_leaders=args.leaders,
            )
            curr_plan = get_plan(ct.assignment)
            base_plan = get_plan(ct.initial_assignment)
            _log.info('Validating current cluster-topology against initial cluster-topology...')
            if not validate_plan(curr_plan, base_plan):
                _log.error('Invalid latest-cluster assignment. Exiting...')
                sys.exit(1)

            # Evaluate proposed-plan and execute/display the same
            # Get final-proposed-plan details
            result = get_reduced_proposed_plan(
                ct.initial_assignment,
                ct.assignment,
                args.max_partition_movements,
                args.max_leader_changes,
            )
            if result:
                # Display or store plan
                display_assignment_changes(result, args.no_confirm)
                # Export proposed-plan to json file
                red_original_assignment = dict((ele[0], ele[1]) for ele in result[0])
                red_proposed_assignment = dict((ele[0], ele[1]) for ele in result[1])
                proposed_plan = get_plan(red_proposed_assignment)
                if args.proposed_plan_file:
                    _log.info(
                        'Storing proposed-plan in json file, {file}'
                        .format(file=args.proposed_plan_file),
                    )
                    proposed_plan_json(proposed_plan, args.proposed_plan_file)
                # Validate and execute plan
                base_plan = get_plan(ct.initial_assignment)
                _log.info(
                    'Original plan before assignment {plan}'
                    .format(plan=get_plan(red_original_assignment)),
                )
                _log.info(
                    'Proposed plan assignment {plan}'
                    .format(plan=get_plan(red_proposed_assignment)),
                )
                _log.info('Validating complete proposed-plan...')
                if validate_plan(proposed_plan, base_plan):
                    # Actual movement of partitions in new-plan
                    net_partition_movements = sum([
                        len(set(replicas) - set(red_proposed_assignment[p_name]))
                        for p_name, replicas in red_original_assignment.iteritems()
                    ])
                    # Net leader changes only
                    net_leader_only_changes = sum([
                        1
                        for p_name, replicas in red_original_assignment.iteritems()
                        if set(replicas) == set(red_proposed_assignment[p_name]) and
                        replicas[0] != red_proposed_assignment[p_name][0]
                    ])
                    _log.info(
                        'Proposed-plan description: Actions: {actions}, '
                        'Partition-movements: {movements}, Leader-only '
                        'changes: {leader_changes}'.format(
                            actions=len(proposed_plan['partitions']),
                            movements=net_partition_movements,
                            leader_changes=net_leader_only_changes,
                        ),
                    )
                    execute_plan(ct, zk, proposed_plan, args.apply, args.no_confirm, script_path)
                else:
                    _log.error('Invalid proposed-plan. Execution Unsuccessful. Exiting...')
                    sys.exit(1)
            else:
                # No new-plan
                msg_str = 'No topic-partition layout changes proposed.'
                if args.no_confirm:
                    _log.info(msg_str)
                else:
                    print(msg_str)
            _log.info('Kafka-cluster-manager tool execution completed.')


def rebalance_layers(
    ct,
    rebalance_replication_groups=False,
    rebalance_brokers=False,
    rebalance_leaders=False,
    display=True,
):
    """Rebalance current cluster-state to get updated state based on
    rebalancing options for different rebalance layers.

    NOTE: Ordering of rebalancing layers should be :-
    a) Replication-groups: (Replica-count imbalance)
    b) Brokers: (partition-count imbalance)
    c) Leaders: (Broker as leader-count imbalance)
    """
    # Get initial imbalance statistics
    initial_imbal = pre_balancing_imbalance_stats(ct, display)

    # Balancing to be done in the given order only
    # Rebalance replication-groups
    if rebalance_replication_groups:
        _log.info(
            'Re-balancing replica-count over replication groups: {groups}...'
            .format(groups=', '.join(ct.rgs.keys())),
        )
        ct.rebalance_replication_groups()
        replication_group_rebalance_stats(ct, display)

    # Rebalance broker-partition count per replication-groups
    if rebalance_brokers:
        _log.info(
            'Re-balancing partition-count across brokers: {brokers}...'
            .format(brokers=', '.join(str(e) for e in ct.brokers.keys())),
        )
        ct.rebalance_brokers()
        broker_rebalance_stats(ct, initial_imbal, display)

    # Rebalance broker as leader count per broker
    if rebalance_leaders:
        _log.info(
            'Re-balancing leader-count across brokers: {brokers}...'
            .format(brokers=', '.join(str(e) for e in ct.brokers.keys())),
        )
        ct.rebalance_leaders()
    final_rebalance_stats(ct, initial_imbal, display, rebalance_leaders)


# Imbalance statistics evaluation and reporting
def pre_balancing_imbalance_stats(ct, display):
    _log.info('Calculating initial rebalance imbalance statistics...')
    initial_imbal = imbalance_value_all(ct, display=display)
    log_imbalance_stats(initial_imbal)
    total_imbal = (
        initial_imbal['replica_cnt'] +
        initial_imbal['net_part_cnt_per_rg'] +
        initial_imbal['topic_partition_cnt'] +
        initial_imbal['partition_cnt'] +
        initial_imbal['leader_cnt']
    )
    if total_imbal == 0:
        _log.info('Cluster is currently balanced!')
    return initial_imbal


def replication_group_rebalance_stats(ct, display):
    _log.info(
        'Calculating rebalance imbalance-stats after rebalancing '
        'replica-count over replication-groups...',
    )
    curr_imbal = imbalance_value_all(ct, leaders=False, display=display)
    _log.info(
        'Imbalance statistics after rebalancing replica-count over '
        'replication-groups'
    )
    log_imbalance_stats(curr_imbal, leaders=False)
    # Assert that replication-groups balanced
    assert(curr_imbal['replica_cnt'] == 0), (
        'Replication-group imbalance count is non-zero: {imbal}'
        .format(imbal=curr_imbal['replica_cnt']),
    )


def broker_rebalance_stats(ct, initial_imbal, display):
    _log.info(
        'Calculating rebalance imbalance-stats after rebalancing brokers...',
    )
    curr_imbal = imbalance_value_all(ct, leaders=False, display=display)
    log_imbalance_stats(curr_imbal, leaders=False)
    if curr_imbal['net_part_cnt_per_rg'] > 0:
        # Report as warning if replication-groups didn't rebalance
        _log.error(
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


def final_rebalance_stats(ct, initial_imbal, display, leaders_balanced=False):
    _log.info('Calculating final rebalance imbalance-stats... ')
    curr_imbal = imbalance_value_all(ct, display)
    log_imbalance_stats(curr_imbal)
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


def log_imbalance_stats(imbal, leaders=True):
    net_imbalance = (
        imbal['replica_cnt'] +
        imbal['net_part_cnt_per_rg'] +
        imbal['topic_partition_cnt']
    )
    _log.info(
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
        _log.info(
            'Leader-count imbalance: {imbal_leader}\n'
            'Net-cluster imbalance (including leader-imbalance): '
            '{imbal}'.format(
                imbal=net_imbalance_with_leaders,
                imbal_leader=imbal['leader_cnt'],
            )
        )
    _log.info(
        'Total partition-movements: {movement_cnt}'
        .format(movement_cnt=imbal['total_movements']),
    )
