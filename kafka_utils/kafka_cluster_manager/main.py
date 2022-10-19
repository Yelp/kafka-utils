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
import argparse
import configparser
import logging
import sys
from logging.config import fileConfig

from kafka_utils.kafka_cluster_manager.cluster_info.cluster_balancer \
    import ClusterBalancer
from kafka_utils.kafka_cluster_manager.cluster_info.partition_count_balancer \
    import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer \
    import PartitionMeasurer
from kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer \
    import UniformPartitionMeasurer
from kafka_utils.kafka_cluster_manager.cluster_info.replication_group_parser \
    import DefaultReplicationGroupParser
from kafka_utils.kafka_cluster_manager.cluster_info.replication_group_parser \
    import ReplicationGroupParser
from kafka_utils.kafka_cluster_manager.cmds.decommission import DecommissionCmd
from kafka_utils.kafka_cluster_manager.cmds.preferred_replica_election import PreferredReplicaElectionCmd
from kafka_utils.kafka_cluster_manager.cmds.rebalance import RebalanceCmd
from kafka_utils.kafka_cluster_manager.cmds.replace import ReplaceBrokerCmd
from kafka_utils.kafka_cluster_manager.cmds.revoke_leadership import RevokeLeadershipCmd
from kafka_utils.kafka_cluster_manager.cmds.set_replication_factor import SetReplicationFactorCmd
from kafka_utils.kafka_cluster_manager.cmds.stats import StatsCmd
from kafka_utils.kafka_cluster_manager.cmds.store_assignments \
    import StoreAssignmentsCmd
from kafka_utils.util import config
from kafka_utils.util.utils import dynamic_import

_log = logging.getLogger()

GENETIC_BALANCER_MODULE = \
    "kafka_utils.kafka_cluster_manager.cluster_info.genetic_balancer"
PARTITION_COUNT_BALANCER_MODULE = \
    "kafka_utils.kafka_cluster_manager.cluster_info.partition_count_balancer"


def parse_args():
    """Parse the arguments."""
    parser = argparse.ArgumentParser(
        description='Manage and describe partition layout over brokers of'
        ' a cluster.',
    )
    parser.add_argument(
        '--cluster-type',
        '-t',
        dest='cluster_type',
        help='Type of the cluster.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--cluster-name',
        '-c',
        dest='cluster_name',
        help='Name of the cluster (Default to local cluster).',
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
        '--write-to-file',
        dest='proposed_plan_file',
        metavar='<reassignment-plan-file-path>',
        type=str,
        help='Write the partition reassignment plan '
             'to a json file.',
    )
    parser.add_argument(
        '--group-parser',
        type=str,
        help='Module containing an implementation of ReplicationGroupParser. '
        'The module should be specified as path_to_include_to_py_path:module. '
        'Ex: "/module/path:module.parser". '
        'If not specified the default replication group parser will create '
        'only one group for all brokers.',
    )
    parser.add_argument(
        '--partition-measurer',
        type=str,
        help='Module containing an implementation of PartitionMeasurer. '
        'The module should be specified as path_to_include_to_py_path:module. '
        'Default: Assign each partition a weight and size of 1.'
    )
    parser.add_argument(
        '--measurer-args',
        type=str,
        action='append',
        default=[],
        help='Argument list that is passed to the chosen PartitionMeasurer. '
        'Ex: --measurer-args "--n 10" will pass ["--n", "10"] to the '
        'PartitionMeasurer\'s parse_args method.'
    )
    parser.add_argument(
        '--cluster-balancer',
        type=str,
        help='Module containing an implementation of ClusterBalancer. '
        'The module should be specified as path_to_include_to_py_path:module. '
        'Default: PartitionCountBalancer.',
    )
    parser.add_argument(
        '--balancer-args',
        type=str,
        action='append',
        default=[],
        help='Argument list that is passed to the chosen ClusterBalancer. '
        'Ex: --balancer-args "--n 10" will pass ["--n", "10"] to the '
        'ClusterBalancer\'s parse_args method.'
    )
    parser.add_argument(
        '--partition-count-balancer',
        action='store_const',
        const=PARTITION_COUNT_BALANCER_MODULE,
        dest='cluster_balancer',
        help='Use the number of partitions on each broker to balance the '
        'cluster.',
    )
    parser.add_argument(
        '--genetic-balancer',
        action='store_const',
        const=GENETIC_BALANCER_MODULE,
        dest='cluster_balancer',
        help='Use partition metrics and a genetic algorithm to balance the '
        'cluster.',
    )

    subparsers = parser.add_subparsers()
    RebalanceCmd().add_subparser(subparsers)
    DecommissionCmd().add_subparser(subparsers)
    RevokeLeadershipCmd().add_subparser(subparsers)
    StatsCmd().add_subparser(subparsers)
    StoreAssignmentsCmd().add_subparser(subparsers)
    ReplaceBrokerCmd().add_subparser(subparsers)
    SetReplicationFactorCmd().add_subparser(subparsers)
    PreferredReplicaElectionCmd().add_subparser(subparsers)

    return parser.parse_args()


def exception_logger(exc_type, exc_value, exc_traceback):
    """Log unhandled exceptions"""
    if not issubclass(exc_type, KeyboardInterrupt):  # do not log Ctrl-C
        _log.critical(
            "Uncaught exception:",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


def configure_logging(log_conf=None, log_unhandled_exceptions=True):
    if log_conf:
        try:
            fileConfig(log_conf, disable_existing_loggers=False)
        except configparser.NoSectionError:
            logging.basicConfig(level=logging.INFO)
            _log.error(
                'Failed to load {logconf} file.'
                .format(logconf=log_conf),
            )
    else:
        logging.basicConfig(level=logging.INFO)
    if log_unhandled_exceptions:
        sys.excepthook = exception_logger


def run():
    args = parse_args()

    configure_logging(args.logconf)

    cluster_config = config.get_cluster_config(
        args.cluster_type,
        args.cluster_name,
        args.discovery_base_path,
    )

    if args.group_parser:
        rg_parser = dynamic_import(args.group_parser, ReplicationGroupParser)()
    else:
        rg_parser = DefaultReplicationGroupParser()

    if args.partition_measurer:
        partition_measurer = dynamic_import(
            args.partition_measurer,
            PartitionMeasurer
        )
    else:
        partition_measurer = UniformPartitionMeasurer

    if args.cluster_balancer:
        cluster_balancer = dynamic_import(
            args.cluster_balancer,
            ClusterBalancer
        )
    else:
        cluster_balancer = PartitionCountBalancer

    args.command(
        cluster_config,
        rg_parser,
        partition_measurer,
        cluster_balancer,
        args,
    )
