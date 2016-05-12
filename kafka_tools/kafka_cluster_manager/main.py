# -*- coding: utf-8 -*-
# Copyright 2015 Yelp Inc.
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
from __future__ import unicode_literals

import argparse
import ConfigParser
import logging
from logging.config import fileConfig

from kafka_tools.kafka_cluster_manager.cmds.decommission import DecommissionCmd
from kafka_tools.kafka_cluster_manager.cmds.rebalance import RebalanceCmd
from kafka_tools.kafka_cluster_manager.cmds.replace import ReplaceBrokerCmd
from kafka_tools.kafka_cluster_manager.cmds.stats import StatsCmd
from kafka_tools.kafka_cluster_manager.cmds.store_assignments import StoreAssignmentsCmd
from kafka_tools.util import config


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
        '--write-to-file',
        dest='proposed_plan_file',
        metavar='<reassignment-plan-file-path>',
        type=str,
        help='Write the partition reassignment plan '
             'to a json file.',
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
    """Verify command-line arguments and run reassignment functionalities."""
    args = parse_args()

    configure_logging(args.logconf)

    cluster_config = config.get_cluster_config(
        args.cluster_type,
        args.cluster_name,
        args.discovery_base_path,
    )
    args.command(cluster_config, args)
