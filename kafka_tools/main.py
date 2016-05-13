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
import glob
import logging
import os
import sys

from kafka_tools import __version__
from kafka_tools.util.config import TopologyConfiguration
from kafka_tools.util.error import ConfigurationError


logging.getLogger().addHandler(logging.NullHandler())


def parse_args():
    """Parse the arguments."""
    parser = argparse.ArgumentParser(
        description='Show available clusters.'
    )
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version="%(prog)s {0}".format(__version__),
    )
    parser.add_argument(
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        default='/nail/etc/kafka_discovery',
        help='Path of the directory containing the <cluster_type>.yaml config.'
        ' Default: %(default)s',
    )

    return parser.parse_args()


def run():
    """Verify command-line arguments and run reassignment functionalities."""
    args = parse_args()

    types = map(
        lambda x: os.path.basename(x)[:-5],
        glob.glob('{0}/*.yaml'.format(args.discovery_base_path)),
    )
    configs = []
    for cluster_type in types:
        try:
            configs.append(
                TopologyConfiguration(cluster_type, args.discovery_base_path),
            )
        except ConfigurationError:
            continue

    if not configs:
        print(
            "No valid cluster types found in {0}".format(
                args.discovery_base_path,
                file=sys.stderr,
            )
        )
        sys.exit(1)

    for config in configs:
        print("Cluster type {type}:".format(type=cluster_type))
        for cluster in config.get_all_clusters():
            print(
                "\tCluster name: {name}\n"
                "\tbroker list: {b_list}\n"
                "\tzookeeper: {zk}".format(
                    name=cluster.name,
                    b_list=cluster.broker_list,
                    zk=cluster.zookeeper,
                )
            )
