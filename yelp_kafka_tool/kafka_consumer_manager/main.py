from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import sys

from yelp_kafka.error import ConfigurationError

from .commands.copy_group import CopyGroup
from .commands.list_groups import ListGroups
from .commands.list_topics import ListTopics
from .commands.offset_advance import OffsetAdvance
from .commands.offset_get import OffsetGet
from .commands.offset_restore import OffsetRestore
from .commands.offset_rewind import OffsetRewind
from .commands.offset_save import OffsetSave
from .commands.offset_set import OffsetSet
from .commands.rename_group import RenameGroup
from .commands.unsubscribe_topics import UnsubscribeTopics
from yelp_kafka_tool.util.config import get_cluster_config


def parse_args():
    parser = argparse.ArgumentParser(
        description="kakfa-consumer-manager provides to ability to view and "
        "manipulate consumer offsets for a specific consumer group."
    )
    parser.add_argument(
        '--cluster-type', dest='cluster_type', required=True,
        help='Type of Kafka cluster. This is a mandatory option. Examples:'
        ' ["datapipe", "scribe", "standard", "spam"].',
    )
    parser.add_argument(
        '--cluster-name', dest='cluster_name',
        help='Kafka Cluster Name. If not specified, this defaults to the '
        'local cluster.'
    )
    subparsers = parser.add_subparsers()

    OffsetGet.add_parser(subparsers)
    OffsetSave.add_parser(subparsers)
    OffsetSet.add_parser(subparsers)
    OffsetAdvance.add_parser(subparsers)
    OffsetRewind.add_parser(subparsers)
    ListTopics.add_parser(subparsers)
    ListGroups.add_parser(subparsers)
    UnsubscribeTopics.add_parser(subparsers)
    CopyGroup.add_parser(subparsers)
    RenameGroup.add_parser(subparsers)
    OffsetRestore.add_parser(subparsers)
    return parser.parse_args()


def run():
    logging.basicConfig(level=logging.ERROR)
    args = parse_args()
    try:
        conf = get_cluster_config(args.cluster_type, args.cluster_name)
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    args.command(args, conf)
