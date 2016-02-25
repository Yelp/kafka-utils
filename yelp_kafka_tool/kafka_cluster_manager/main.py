from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import ConfigParser
import sys
from logging import getLogger
from logging.config import fileConfig

from yelp_kafka.config import ClusterConfig

from .cluster_info.util import confirm_execution
from .util import KafkaInterface
from yelp_kafka_tool.util import config
from yelp_kafka_tool.kafka_cluster_manager.rebalance import RebalanceCmd


_log = getLogger()


def execute_plan(ct, zk, proposed_plan, to_apply, no_confirm, script_path):
    """Save proposed-plan and execute the same if requested."""
    # Execute proposed-plan
    if to_execute(to_apply, no_confirm):
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
        _log.info('Proposed plan won\'t be executed.')


def to_execute(to_apply, no_confirm):
    """Confirm if proposed-plan should be executed."""
    if to_apply and (no_confirm or confirm_execution()):
        return True
    return False


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
        required=True,
    )
    parser.add_argument(
        '--cluster-name',
        dest='cluster_name',
        help='Name of the cluster (example: uswest1-devc;'
        ' Default to local cluster)',
    )
    parser.add_argument(
        '--topology-base-path',
        dest='topology_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        '--logconf',
        type=str,
        help='Path to logging configuration file. Default: log to console.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Proposed-plan will be executed on confirmation.',
    )
    parser.add_argument(
        '--no-confirm',
        action='store_true',
        help='Proposed-plan will be executed without confirmation.'
             ' --apply flag also required.',
    )
    parser.add_argument(
        '--dump-to-file',
        dest='proposed_plan_file',
        metavar='<reassignment-plan-file-path>',
        type=str,
        help='Dump the partition reassignment plan '
             'to a json file.',
    )

    subparsers = parser.add_subparsers()
    RebalanceCmd().add_subparser(subparsers)

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
    # Load logging configuration from file
    try:
        fileConfig(args.logconf)
    except ConfigParser.NoSectionError:
        _log.error(
            'Failed to load {logconf} file. Exiting...'
            .format(logconf=args.logconf),
        )

    if not validate_args(args):
        sys.exit(1)
    if args.zookeeper:
        if args.cluster_name is None:
            cluster_name = 'Unknown'
        else:
            cluster_name = args.cluster_name
        cluster_config = ClusterConfig(
            type=None,
            name=cluster_name,
            broker_list=[],
            zookeeper=args.zookeeper,
        )
    else:
        cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
        )
    args.command(cluster_config, args)
