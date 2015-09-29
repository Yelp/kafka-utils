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
from .execute_assignment import execute_plan
from .util import KAFKA_SCRIPT_PATH


DEFAULT_MAX_CHANGES = 5
_log = logging.getLogger('kafka-cluster-manager')


def reassign_partitions(cluster_config, args):
    """Get executable proposed plan(if any) for display or execution."""
    with ZK(cluster_config) as zk:
        script_path = args.use_kafka_script
        if script_path == 'default':
            script_path = KAFKA_SCRIPT_PATH
        ct = ClusterTopology(zk=zk, script_path=script_path)

        # Re-balance replication-groups
        if args.replication_groups:
            ct.reassign_partitions(replication_groups=True)

        # Execute or display plan
        params = {
            'zk': zk,
            'initial_plan': ct.initial_assignment,
            'curr_plan': ct.assignment,
            'max_actions': args.max_changes,
            'apply': args.apply,
            'no_confirm': args.no_confirm,
            'proposed_plan_file': args.proposed_plan_file,
            'brokers': ct.brokers.keys(),
            'topics': ct.topics.keys(),
            'script_path': script_path,
        }
        execute_plan(params)


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
        '--use-kafka-script-path',
        dest='use_kafka_script',
        type=str,
        default=None,
        help='Path of kafka-cli scripts to be used to access zookeeper.'
        ' Use \'default\' to use default path: {default}'
        .format(default=KAFKA_SCRIPT_PATH),
    )
    parser_rebalance.add_argument(
        '--replication-groups',
        dest='replication_groups',
        action='store_true',
        help='Evenly distributes replicas over replication-groups.',
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
    rebalance_options = [args.replication_groups]

    # At-least one of rebalancing options required
    if not any(rebalance_options):
        _log.error('\'--replication-groups\' flag required.')
        result = False

    if args.no_confirm and not args.apply:
        _log.error('--apply required with --no-confirm flag.')
        result = False
    return result


def run():
    """Verify command-line arguments and run reassignment functionalities."""
    logging.basicConfig(level=logging.ERROR)
    args = parse_args()
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
