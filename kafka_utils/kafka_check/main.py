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

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.min_isr import MinIsrCmd
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
        dest='cluster_type',
        required=True,
        help='Type of cluster',
        default=None,
    )
    parser.add_argument(
        "--cluster-name",
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
        help='Kafka current broker id.',
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
        'non-controller brokers. Set --broker-id to -1 to read broker-id from '
        '--data-path.',
    )

    subparsers = parser.add_subparsers()
    MinIsrCmd().add_subparser(subparsers)

    return parser.parse_args()


def run():
    """Verify command-line arguments and run commands"""
    args = parse_args()

    try:
        cluster_config = config.get_cluster_config(
            args.cluster_type,
            args.cluster_name,
            args.discovery_base_path,
        )
        code, msg = args.command(cluster_config, args)
    except ConfigurationError as e:
        terminate(status_code.CRITICAL, "ConfigurationError {0}".format(e))
    except Exception as e:
        terminate(status_code.CRITICAL, "Got Exception: {0}".format(e))

    terminate(code, msg)
