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
"""
Kafka checks module.
Each check is separated subcommand for kafka-check.
"""
from __future__ import absolute_import
from __future__ import print_function

import argparse
import logging

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.min_isr import MinIsrCmd
from kafka_utils.kafka_check.commands.offline import OfflineCmd
from kafka_utils.kafka_check.commands.replica_unavailability import ReplicaUnavailabilityCmd
from kafka_utils.kafka_check.metadata_file import get_broker_id
from kafka_utils.kafka_check.status_code import prepare_terminate_message
from kafka_utils.kafka_check.status_code import terminate
from kafka_utils.util import config
from kafka_utils.util.error import ConfigurationError


def convert_to_broker_id(string):
    """Convert string to kafka broker_id."""
    error_msg = 'Positive integer or -1 required, {string} given.'.format(string=string)
    try:
        value = int(string)
    except ValueError:
        raise argparse.ArgumentTypeError(error_msg)
    if value <= 0 and value != -1:
        raise argparse.ArgumentTypeError(error_msg)
    return value


def parse_args():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser(
        description='Check kafka current status',
    )
    parser.add_argument(
        "--cluster-type",
        "-t",
        dest='cluster_type',
        required=True,
        help='Type of cluster',
        default=None,
    )
    parser.add_argument(
        "--cluster-name",
        "-c",
        dest='cluster_name',
        help='Name of the cluster',
    )
    parser.add_argument(
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        "--broker-id",
        help='The broker id where the check is running. Set to -1 if you use automatic '
        'broker ids, and it will read the id from data-path instead. This parameter is '
        'required only in case controller-only or first-broker-only are used.',
        type=convert_to_broker_id,
    )
    parser.add_argument(
        "--data-path",
        help='Path to the Kafka data folder.',
    )
    parser.add_argument(
        '--controller-only',
        action="store_true",
        help='If this parameter is specified, it will do nothing and succeed on '
        'non-controller brokers. Default: %(default)s',
    )
    parser.add_argument(
        '--first-broker-only',
        action='store_true',
        help='If specified, the command will only perform the check if '
        'broker_id is the lowest broker id in the cluster. If it is not the lowest, '
        'it will not perform any check and succeed immediately. '
        'Default: %(default)s',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
        default=False,
    )
    parser.add_argument(
        '-j',
        '--json',
        help='Print output in json format. Default: %(default)s',
        action="store_true",
        default=False,
    )

    subparsers = parser.add_subparsers()
    MinIsrCmd().add_subparser(subparsers)
    ReplicaUnavailabilityCmd().add_subparser(subparsers)
    OfflineCmd().add_subparser(subparsers)

    return parser.parse_args()


def run():
    """Verify command-line arguments and run commands"""
    args = parse_args()
    logging.basicConfig(level=logging.WARN)

    # to prevent flooding for sensu-check.
    logging.getLogger('kafka').setLevel(logging.CRITICAL)

    if args.controller_only and args.first_broker_only:
        terminate(
            status_code.WARNING,
            prepare_terminate_message(
                "Only one of controller_only and first_broker_only should be used",
            ),
            args.json,
        )

    if args.controller_only or args.first_broker_only:
        if args.broker_id is None:
            terminate(
                status_code.WARNING,
                prepare_terminate_message("broker_id is not specified"),
                args.json,
            )
        elif args.broker_id == -1:
            try:
                args.broker_id = get_broker_id(args.data_path)
            except Exception as e:
                terminate(
                    status_code.WARNING,
                    prepare_terminate_message("{}".format(e)),
                    args.json,
                )

    try:
        cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
            args.discovery_base_path,
        )
        code, msg = args.command(cluster_config, args)
    except ConfigurationError as e:
        terminate(
            status_code.CRITICAL,
            prepare_terminate_message("ConfigurationError {0}".format(e)),
            args.json,
        )

    terminate(code, msg, args.json)
