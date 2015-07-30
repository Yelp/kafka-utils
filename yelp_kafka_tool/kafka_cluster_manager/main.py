"""
Generate and/or execute the reassignment plan with minimal
movements having optimally balanced partitions or leaders or both.

Example:
    kafka-cluster-manager --cluster-type scribe rebalance --broker-list '0,1,2'
        --partitions

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
    --broker-list:      Broker-list over which the assignment will be generated
    --max-changes:      Maximum number of actions as part of single execution
                        of the tool
    --apply:            On True execute proposed assignment after execution,
                        display proposed-plan otherwise
    --partitions:       Optimally balance partitions over brokers
    --leaders:          Optimally balance preferred-leaders over brokers
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import sys

from yelp_kafka.config import ClusterConfig
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology \
    import ClusterTopology
from yelp_kafka_tool.util import config
from yelp_kafka_tool.util.zookeeper import ZK
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    display_cluster_topology,
    display_same_replica_count_rg,
    display_same_topic_partition_count_broker,
    display_partition_count_per_broker,
    display_leader_count_per_broker,
)

DEFAULT_MAX_CHANGES = 5


def reassign_partitions(cluster_config, args):
    """Get executable proposed plan(if any) for display or execution."""
    with ZK(cluster_config) as zk:
        ct = ClusterTopology(zk)
        # Display cluster topology as fetched from zookeeper
        print('Displaying current cluster topology')
        display_cluster_topology(ct)

        # Display topology as built from objects
        ct.reassign_partitions(
            [args.partitions, args.leaders],
            args.max_changes,
            args.apply
        )

        print('Displaying cluster topology after reassignment')
        display_cluster_topology(ct)
        assert(ct.initial_assignment == ct.assignment)

        # Get imbalance stats
        # Partition-count imbalance
        stdev_imbalance, net_imbalance, partitions_per_broker = \
            ct.partition_imbalance()
        display_partition_count_per_broker(
            partitions_per_broker,
            stdev_imbalance,
            net_imbalance,
        )
        # Leader-count imbalance
        stdev_imbalance, net_imbalance, leaders_per_broker = \
            ct.leader_imbalance()
        display_leader_count_per_broker(
            leaders_per_broker,
            stdev_imbalance,
            net_imbalance,
        )

        # Duplicate-replica-count imbalance
        net_imbalance, duplicate_replica_count_per_rg = \
            ct.replication_group_imbalance()
        display_same_replica_count_rg(
            duplicate_replica_count_per_rg,
            net_imbalance,
        )

        # Same topic-partition count
        net_imbalance, same_topic_partition_count_per_broker = \
            ct.topic_imbalance()
        display_same_topic_partition_count_broker(
            same_topic_partition_count_per_broker,
            net_imbalance,
        )


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

    # re-balance partitions
    parser_rebalance = subparsers.add_parser(
        'rebalance',
        description='Re-balance the partitions or leaders '
        'or both optimally over brokers.'
        'At least one of \'--partitions\' or \'--leaders\' option required.'
    )
    parser_rebalance.add_argument(
        '--partitions',
        dest='partitions',
        action='store_true',
        help='Evenly distributes partitions optimally over given brokers'
    )
    parser_rebalance.add_argument(
        '--leaders',
        dest='leaders',
        action='store_true',
        help='Evenly distributes leaders optimally over brokers'
    )
    parser_rebalance.add_argument(
        '--max-changes',
        dest='max_changes',
        type=int,
        default=DEFAULT_MAX_CHANGES,
        help='Maximum number of actions executed from proposed assignment'
             '%(default)s'
    )
    parser_rebalance.add_argument(
        '--apply',
        dest='apply',
        action='store_true',
        help='Proposed-plan will be executed on confirmation'
    )
    parser_rebalance.set_defaults(command=reassign_partitions)
    return parser.parse_args()


def validate_args(args):
    """Validate relevant arguments. Exit on failure."""
    result = True
    params = [args.zookeeper, args.cluster_type]
    if all(params) or not any(params):
        print(
            'Command must include exactly one '
            'of zookeeper or cluster-type argument',
        )
        result = False
    if args.max_changes <= 0:
        print(
            'max-changes should be greater than 0: '
            '{max_changes} found. Aborting...'
            .format(max_changes=args.max_changes)
        )
        result = False
    rebalance_options = [args.leaders, args.partitions]
    if not any(rebalance_options):
        print(
            'At least one of \'--partitions\' or '
            '\'--leaders\' flag required.'
        )
        result = False
    return result


def run():
    """Verify command-line arguments and run reassignment functionalities."""
    args = parse_args()
    if not validate_args(args):
        sys.exit(1)
    if args.zookeeper:
        cluster_config = ClusterConfig(
            name=args.cluster_name,
            broker_list=[],
            zookeeper=args.zookeeper
        )
    else:
        cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
        )
    args.command(cluster_config, args)
