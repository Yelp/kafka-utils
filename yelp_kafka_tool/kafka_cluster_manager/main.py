from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import ConfigParser
import logging
import sys
from logging.config import fileConfig

from yelp_kafka.config import ClusterConfig

from yelp_kafka_tool.util import config
from yelp_kafka_tool.kafka_cluster_manager.commands.rebalance import RebalanceCmd


_log = logging.getLogger()


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
        '--discovery-base-path',
        dest='discovery_base_path',
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
    rebalance_options = [args.replication_groups, args.leaders, args.brokers]
    if not any(rebalance_options):
        _log.error(
            'At least one of --replication-groups, --leaders, --brokers flag required.',
        )
        sys.exit(1)

    if args.no_confirm and not args.apply:
        _log.error('--apply required with --no-confirm flag.')
        sys.exit(1)


def configure_logging(log_conf=None):
    if log_conf:
        try:
            fileConfig(args.logconf)
        except ConfigParser.NoSectionError:
            logging.basicConfig(level=logging.WARNING)
            _log.error(
                'Failed to load {logconf} file.'
                .format(logconf=args.logconf),
            )
    else:
        logging.basicConfig(level=logging.WARNING)


def run():
    """Verify command-line arguments and run reassignment functionalities."""
    args = parse_args()

    configure_logging(args.logconf)
    validate_args(args)

    cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
            args.discovery_base_path,
    )
    args.command(cluster_config, args)
