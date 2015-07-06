from __future__ import (
    absolute_import,
    print_function,
    unicode_literals,
)
import argparse
import sys
import logging

from yelp_kafka.discovery import (
    get_all_clusters,
    get_local_cluster,
)

from .commands.delete_topics import DeleteTopics
from .commands.list_topics import ListTopics
from .commands.offset_advance import OffsetAdvance
from .commands.offset_get import OffsetGet
from .commands.offset_rewind import OffsetRewind
from .commands.offset_set import OffsetSet
from .commands.rename_group import RenameGroup


def parse_args():
    parser = argparse.ArgumentParser(
        description="kakfa-consumer-manager provides to ability to view and "
        "manipulate consumer offsets for a specific consumer group."
    )
    parser.add_argument(
        '--cluster-type', dest='cluster_type', required=True,
        help='Type of Kafka cluster. This is a mandatory option',
        choices=['scribe', 'standard', 'spam'],
    )
    parser.add_argument(
        '--cluster-name', dest='cluster_name',
        help='Kafka Cluster Name. If not specified, this defaults to the '
        'local cluster.'
    )
    subparsers = parser.add_subparsers()

    OffsetGet.add_parser(subparsers)
    OffsetSet.add_parser(subparsers)
    OffsetAdvance.add_parser(subparsers)
    OffsetRewind.add_parser(subparsers)
    ListTopics.add_parser(subparsers)
    DeleteTopics.add_parser(subparsers)
    RenameGroup.add_parser(subparsers)
    args = parser.parse_args()
    return args


def run():
    logging.basicConfig(level=logging.ERROR)
    args = parse_args()

    conf = None
    if not args.cluster_name:
        conf = get_local_cluster(args.cluster_type)
    else:
        clusters = get_all_clusters(args.cluster_type)
        for cluster in clusters:
            if cluster.name == args.cluster_name:
                conf = cluster
                break

    if not conf:
        print(
            "Error: Kafka cluster: {cluster} not found.".format(
                cluster=args.cluster_name
            ),
            file=sys.stderr
        )
        sys.exit(1)

    args.command(args, conf)
