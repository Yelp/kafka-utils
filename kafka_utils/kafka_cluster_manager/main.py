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
import ConfigParser
import importlib
import inspect
import logging
import os
import sys
from logging.config import fileConfig

from kafka_utils.kafka_cluster_manager.cluster_info.replication_group_parser \
    import DefaultReplicationGroupParser
from kafka_utils.kafka_cluster_manager.cluster_info.replication_group_parser \
    import ReplicationGroupParser
from kafka_utils.kafka_cluster_manager.cmds.decommission import DecommissionCmd
from kafka_utils.kafka_cluster_manager.cmds.rebalance import RebalanceCmd
from kafka_utils.kafka_cluster_manager.cmds.replace import ReplaceBrokerCmd
from kafka_utils.kafka_cluster_manager.cmds.stats import StatsCmd
from kafka_utils.kafka_cluster_manager.cmds.store_assignments \
    import StoreAssignmentsCmd
from kafka_utils.util import config


_log = logging.getLogger()


def dynamic_import_group_parser(module_full_name):
    try:
        path, module_name = module_full_name.rsplit(':', 1)
    except ValueError:
        print(
            "{0} is not a valid parser module".format(module_full_name),
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.isdir(path):
        print("{0} is not a valid directory".format(path), file=sys.stderr)
        sys.exit(1)

    sys.path.append(path)
    module = importlib.import_module(module_name)
    for class_name, class_type in inspect.getmembers(module, inspect.isclass):
        if (issubclass(class_type, ReplicationGroupParser) and
                class_type is not ReplicationGroupParser):
            return class_type()


def parse_args():
    """Parse the arguments."""
    parser = argparse.ArgumentParser(
        description='Manage and describe partition layout over brokers of'
        ' a cluster.',
    )
    parser.add_argument(
        '--cluster-type',
        dest='cluster_type',
        help='Type of the cluster.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--cluster-name',
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
        help='Module containing an implementation of ReplicationGroupParser.'
        'The module should be specified as path_to_include_to_py_path:module.'
        'Ex: "/module/path:module.parser".'
        'If not specified the default replication group parser will create '
        'only one group for all brokers.',
    )

    subparsers = parser.add_subparsers()
    RebalanceCmd().add_subparser(subparsers)
    DecommissionCmd().add_subparser(subparsers)
    StatsCmd().add_subparser(subparsers)
    StoreAssignmentsCmd().add_subparser(subparsers)
    ReplaceBrokerCmd().add_subparser(subparsers)

    return parser.parse_args()


def configure_logging(log_conf=None):
    if log_conf:
        try:
            fileConfig(log_conf)
        except ConfigParser.NoSectionError:
            logging.basicConfig(level=logging.INFO)
            _log.error(
                'Failed to load {logconf} file.'
                .format(logconf=log_conf),
            )
    else:
        logging.basicConfig(level=logging.INFO)


def run():
    args = parse_args()

    configure_logging(args.logconf)

    cluster_config = config.get_cluster_config(
        args.cluster_type,
        args.cluster_name,
        args.discovery_base_path,
    )
    if args.group_parser:
        rg_parser = dynamic_import_group_parser(args.group_parser)
    else:
        rg_parser = DefaultReplicationGroupParser()

    args.command(cluster_config, rg_parser, args)
