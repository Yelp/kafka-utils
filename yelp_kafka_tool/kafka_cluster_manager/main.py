"""
Generate and/or execute the reassignment plan with minimal
movements having optimally balanced replication-groups and brokers.

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
    --cluster-type:             Type of cluster for example 'scribe', 'spam'
    --cluster-name:             Cluster name over which the reassignment will be done
    --zookeeper:                Zookeeper hostname
    rebalance:                  Indicates that given request is for partition
                                reassignment
    --leader:                   Re-balance broker as leader count
    --brokers:                  Re-balance partition-count per broker
    --replication-groups:       Re-balance replica and partition-count per replication-group
    --max-partition-movements:  Maximum number of partition-movements as part of final actions
    --max-leader-only-changes:  Maximum number of actions with leader only changes
    --apply:                    On True execute proposed assignment after execution,
                                display proposed-plan otherwise
    --no-confirm:               Execute the plan without asking for confirmation.
    --logconf:                  Provide logging configuration file path.
    --proposed-plan-json:       Export proposed-plan to .json format
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import ConfigParser
import sys
from logging import getLogger
from logging.config import fileConfig

from yelp_kafka.config import ClusterConfig

from .cluster_info.cluster_topology import ClusterTopology
from .cluster_info.display import display_assignment_changes
from .cluster_info.stats import imbalance_value_all
from .cluster_info.util import confirm_execution
from .cluster_info.util import get_plan
from .cluster_info.util import get_reduced_proposed_plan
from .cluster_info.util import proposed_plan_json
from .cluster_info.util import validate_plan
from .util import KafkaInterface
from yelp_kafka_tool.util import config
from yelp_kafka_tool.util.zookeeper import ZK
from yelp_kafka_tool.kafka_cluster_manager.rebalance import RebalanceCmd
from yelp_kafka_tool.kafka_cluster_manager.decommission import DecommissionCmd
from yelp_kafka_tool.kafka_cluster_manager.replace import ReplaceCmd


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
        default=None,
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
    parser.add_argument(
        '--logconf',
        type=str,
        help='Path to logging configuration file. Default: log to console.',
    )

    subparsers = parser.add_subparsers()
    RebalanceCmd().add_subparser(subparsers)
    DecommissionCmd().add_subparser(subparsers)

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
