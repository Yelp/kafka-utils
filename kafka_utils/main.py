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

from kafka_utils import __version__
from kafka_utils.util.config import iter_configurations


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
        help='Path of the directory containing the <cluster_type>.yaml config.'
        ' Default try: '
        '$KAFKA_DISCOVERY_DIR, $HOME/.kafka_discovery, /etc/kafka_discovery',
    )

    return parser.parse_args()


def run():
    args = parse_args()

    for config in iter_configurations(args.discovery_base_path):
        print("cluster-type {type}:".format(type=config.cluster_type))
        for cluster in config.get_all_clusters():
            print(
                "\tcluster-name: {name}\n"
                "\tbroker-list: {b_list}\n"
                "\tzookeeper: {zk}".format(
                    name=cluster.name,
                    b_list=", ".join(cluster.broker_list),
                    zk=cluster.zookeeper,
                )
            )
