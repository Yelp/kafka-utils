# -*- coding: utf-8 -*-
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
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import sys

from .commands.copy_group import CopyGroup
from .commands.delete_group import DeleteGroup
from .commands.list_groups import ListGroups
from .commands.list_topics import ListTopics
from .commands.offset_advance import OffsetAdvance
from .commands.offset_get import OffsetGet
from .commands.offset_restore import OffsetRestore
from .commands.offset_rewind import OffsetRewind
from .commands.offset_save import OffsetSave
from .commands.offset_set import OffsetSet
from .commands.offset_set_timestamp import OffsetSetTimestamp
from .commands.offsets_for_timestamp import OffsetsForTimestamp
from .commands.rename_group import RenameGroup
from .commands.unsubscribe_topics import UnsubscribeTopics
from .commands.watermark_get import WatermarkGet
from kafka_utils.util.config import get_cluster_config
from kafka_utils.util.error import ConfigurationError


def parse_args():
    parser = argparse.ArgumentParser(
        description="kafka-consumer-manager provides to ability to view and "
        "manipulate consumer offsets for a specific consumer group.",
    )
    parser.add_argument(
        '--cluster-type', '-t', dest='cluster_type', required=True,
        help='Type of Kafka cluster. This is a mandatory option.',
    )
    parser.add_argument(
        '--cluster-name', '-c', dest='cluster_name',
        help='Kafka Cluster Name. If not specified, this defaults to the '
        'local cluster.',
    )
    parser.add_argument(
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        '--verbose',
        '-v',
        dest='warn_verbose',
        action='store_true',
        help='Show WARNING level logging',
    )
    parser.add_argument(
        '--vverbose',
        '-vv',
        dest='info_verbose',
        action='store_true',
        help='Show INFO level logging',
    )
    parser.add_argument(
        '--vvverbose',
        '-vvv',
        dest='debug_verbose',
        action='store_true',
        help='Show DEBUG level logging',
    )
    subparsers = parser.add_subparsers()

    OffsetGet.add_parser(subparsers)
    OffsetSave.add_parser(subparsers)
    OffsetSet.add_parser(subparsers)
    OffsetAdvance.add_parser(subparsers)
    OffsetRewind.add_parser(subparsers)
    WatermarkGet.add_parser(subparsers)
    ListTopics.add_parser(subparsers)
    ListGroups.add_parser(subparsers)
    UnsubscribeTopics.add_parser(subparsers)
    CopyGroup.add_parser(subparsers)
    DeleteGroup.add_parser(subparsers)
    RenameGroup.add_parser(subparsers)
    OffsetRestore.add_parser(subparsers)
    OffsetsForTimestamp.add_parser(subparsers)
    OffsetSetTimestamp.add_parser(subparsers)
    return parser.parse_args()


def run():
    logging.basicConfig(level=logging.ERROR)
    args = parse_args()
    if args.warn_verbose:
        logging.getLogger().setLevel(logging.WARNING)
    if args.info_verbose:
        logging.getLogger().setLevel(logging.INFO)
    if args.debug_verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    try:
        conf = get_cluster_config(
            args.cluster_type,
            args.cluster_name,
            args.discovery_base_path,
        )
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    args.command(args, conf)
