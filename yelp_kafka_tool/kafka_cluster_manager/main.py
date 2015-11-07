"""
Generate and/or execute the reassignment plan with minimal
movements having optimally balanced replication-groups.

Example:
    kafka-cluster-manager --cluster-type scribe rebalance --replication-groups

    The above command first applies the re-balancing algorithm
    over given broker-id's '0,1,2' over default cluster
    uswest1-devc-scribe for given type cluster-type 'scribe'
    to generate a new plan in the format.
    {"version": 1, "partitions": [
        {"topic": "T3", "partition": 1, "replicas": [2]},
        {"topic": "T1", "partition": 2, "replicas": [1]},
        {"topic": "T1", "partition": 3, "replicas": [2, 0]}
    ]}
    The above implies that on execution for partition '1' of topic 'T3'
    will be moved to new broker-id '2' and similarly for others.

Attributes:
    --cluster-name:     Cluster name over which the reassignment will be done
    --zookeeper:        Zookeeper hostname
    rebalance:          Indicates that given request is for partition
                        reassignment
    --cluster-type:     Type of cluster for example 'scribe', 'spam'
    --max-changes:      Maximum number of actions as part of single execution
                        of the tool
    --apply:            On True execute proposed assignment after execution,
                        display proposed-plan otherwise
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging
import sys

from yelp_kafka.config import ClusterConfig
from yelp_kafka_tool.util import config
from yelp_kafka_tool.util.zookeeper import ZK
from .cluster_info.cluster_topology import ClusterTopology
from .cluster_info.display import display_assignment_changes
from .cluster_info.util import (
    get_reduced_proposed_plan,
    confirm_execution,
    proposed_plan_json,
    validate_plan,
    get_plan,
)
from .cluster_info.stats import imbalance_value_all
from .util import KafkaInterface


DEFAULT_MAX_CHANGES = 5
KAFKA_SCRIPT_PATH = '/usr/bin/kafka-reassign-partitions.sh'

_log = logging.getLogger('kafka-cluster-manager')


def execute_plan(ct, zk, proposed_plan, to_apply, no_confirm, script_path):
    """Save proposed-plan and execute the same if requested."""
    # Execute proposed-plan
    if to_execute(to_apply, no_confirm):
        _log.info('Executing Proposed Plan')
        status = KafkaInterface(script_path).execute_plan(
            zk,
            proposed_plan,
            ct.brokers.values(),
            ct.topics.values(),
        )
        if not status:
            _log.error('Plan execution unsuccessful. Exiting...')
            sys.exit(1)
        else:
            _log.info('Plan sent to zookeeper for reassignment successfully.')

    else:
        _log.info('Proposed Plan won\'t be executed.')


def to_execute(to_apply, no_confirm):
    """Confirm if proposed-plan should be executed."""
    if to_apply and (no_confirm or confirm_execution()):
        return True
    return False


def is_cluster_stable(zk):
    """Return True if cluster state is stable.

    :key-term: stable
    Currently cluster being stable means non existence of 'reassign_partitions'
    node in zookeeper. The existence of above node implies previous reassignment
    in progress, implying that cluster has incorrect replicas temporarily.
    TODO: Under-replicated-partition check
    """
    if zk.reassignment_in_progress():
        in_progress_plan = zk.get_in_progress_plan()
        if in_progress_plan:
            in_progress_partitions = in_progress_plan['partitions']
            _log.info(
                'Previous re-assignment in progress for {count} partitions.'
                ' Current partitions in re-assignment queue: {partitions}'
                .format(
                    count=len(in_progress_partitions),
                    partitions=in_progress_partitions,
                )
            )
        else:
            _log.warning(
                'Previous re-assignment in progress. In progress partitions'
                ' could not be fetched',
            )
        return False
    return True


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


def reassign_partitions(cluster_config, args):
    """Get executable proposed plan(if any) for display or execution."""
    with ZK(cluster_config) as zk:
        _log.info(
            'Starting re-assignment tool for cluster: {c_name} and zookeeper: '
            '{zookeeper}'.format(
                c_name=cluster_config.name,
                zookeeper=cluster_config.zookeeper,
            )
        )
        if not is_cluster_stable(zk):
            _log.error('Cluster-state is not stable. Exiting...')
            sys.exit(1)
        script_path = None
        # Use kafka-scripts
        if args.use_kafka_script:
            script_path = args.script_path
        ct = ClusterTopology(zk=zk, script_path=script_path)
        _log.info('Re-assigning partitions across cluster topology...')
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
            args.max_changes,
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
                    'Proposed-plan description: Action(s): {actions}, '
                    'Partition-movement(s): {movements}, Leader-only '
                    'change(s): {leader_changes}'.format(
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


def parse_args():
    """Parse the arguments."""
    parser = argparse.ArgumentParser(
        description='Alter topic-partition layout over brokers.',
    )
    parser.add_argument(
        '--cluster-type',
        dest='cluster_type',
        help='Type of cluster',
        type=str,
        default=None
    )
    parser.add_argument(
        '--zookeeper',
        dest='zookeeper',
        type=str,
        help='Zookeeper hostname',
        default=None,
    )
    parser.add_argument(
        '--cluster-name',
        dest='cluster_name',
        help='Name of the cluster (example: uswest1-devc;'
        ' Default to local cluster)',
        default=None
    )
    subparsers = parser.add_subparsers()

    # re-assign partitions
    parser_rebalance = subparsers.add_parser(
        'rebalance',
        description='Re-assign partitions over brokers.',
    )
    parser_rebalance.add_argument(
        '--replication-groups',
        dest='replication_groups',
        action='store_true',
        help='Evenly distributes replicas over replication-groups.',
    )
    parser_rebalance.add_argument(
        '--use-kafka-script',
        dest='use_kafka_script',
        action='store_true',
        help='Use kafka-cli scripts to access zookeeper.'
        ' Use --script-path to provide path for script.',
    )
    parser_rebalance.add_argument(
        '--script-path',
        dest='script_path',
        type=str,
        default=KAFKA_SCRIPT_PATH,
        help='Path of kafka-cli scripts to be used to access zookeeper.'
        ' DEFAULT: %(default)s',
    )
    parser_rebalance.add_argument(
        '--leaders',
        dest='leaders',
        action='store_true',
        help='Evenly distributes leaders optimally over brokers.',
    )
    parser_rebalance.add_argument(
        '--brokers',
        dest='brokers',
        action='store_true',
        help='Evenly distributes partitions optimally over brokers'
        ' with minimal movements for each replication-group.',
    )
    parser_rebalance.add_argument(
        '--max-changes',
        dest='max_changes',
        type=int,
        default=DEFAULT_MAX_CHANGES,
        help='Maximum number of actions executed from proposed assignment'
             ' DEFAULT: %(default)s'
    )
    parser_rebalance.add_argument(
        '--apply',
        dest='apply',
        action='store_true',
        help='Proposed-plan will be executed on confirmation.'
    )
    parser_rebalance.add_argument(
        '--no-confirm',
        dest='no_confirm',
        action='store_true',
        help='Proposed-plan will be executed without confirmation.'
             ' --apply flag also required.'
    )
    parser_rebalance.add_argument(
        '--json',
        dest='proposed_plan_file',
        metavar='<reassignment-plan-file-path>',
        type=str,
        help='Export candidate partition reassignment configuration '
             'to given json file.',
    )
    parser_rebalance.add_argument(
        '--log-file',
        type=str,
        help='Write logs to specified file',
    )
    parser_rebalance.add_argument(
        '--skip-imbalance-stats',
        dest='skip_imbalance_stats',
        action='store_true',
        help='Skip any imbalance calculations to speed-up rebalancing time',
    )
    parser_rebalance.add_argument(
        '--debug',
        dest='debug',
        action='store_true',
        help='Get debug level logs.',
    )
    parser_rebalance.set_defaults(command=reassign_partitions)
    return parser.parse_args()


def validate_args(args):
    """Validate relevant arguments. Exit on failure."""
    result = True
    params = [args.zookeeper, args.cluster_type]
    if all(params) or not any(params):
        _log.error(
            'Command must include exactly one '
            'of zookeeper or cluster-type argument',
        )
        result = False
    if args.max_changes <= 0:
        _log.error(
            'max-changes should be greater than 0: '
            '{max_changes} found. Aborting...'
            .format(max_changes=args.max_changes)
        )
        result = False
    rebalance_options = [args.replication_groups, args.leaders, args.brokers]
    if not any(rebalance_options):
        _log.error(
            'At least one of --replication-groups, --leaders, --brokers flag required.',
        )
        result = False

    if args.no_confirm and not args.apply:
        _log.error('--apply required with --no-confirm flag.')
        result = False
    return result


def run():
    """Verify command-line arguments and run reassignment functionalities."""
    args = parse_args()
    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    # Re-direct logs to file or stdout
    logging.basicConfig(
        level=level,
        filename=args.log_file,
        format='[%(asctime)s] [%(levelname)s:%(module)s] %(message)s',
    )
    if not validate_args(args):
        sys.exit(1)
    if args.zookeeper:
        cluster_config = ClusterConfig(
            type=None,
            name=args.cluster_name,
            broker_list=[],
            zookeeper=args.zookeeper,
        )
    else:
        cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
        )
    args.command(cluster_config, args)
