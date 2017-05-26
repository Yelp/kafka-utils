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

import sys

from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from kafka_utils.kafka_consumer_manager.util import KafkaGroupReader
from kafka_utils.util.zookeeper import ZK


class ListGroups(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_groups = subparsers.add_parser(
            "list_groups",
            description="List consumer groups.",
            add_help=False,
        )
        parser_list_groups.add_argument(
            '--storage',
            choices=['zookeeper', 'kafka', 'dual'],
            help="String describing where to fetch the committed offsets.",
            default='kafka'
        )
        parser_list_groups.set_defaults(command=cls.run)

    @classmethod
    def get_zookeeper_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Zookeeper.'''
        with ZK(cluster_config) as zk:
            try:
                return zk.get_children("/consumers")
            except NoNodeError:
                print(
                    "Error: No consumers node found in zookeeper",
                    file=sys.stderr,
                )

    @classmethod
    def get_kafka_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Kafka.'''
        kafka_group_reader = KafkaGroupReader(cluster_config)
        return list(kafka_group_reader.read_groups().keys())

    @classmethod
    def print_groups(cls, groups, cluster_config):
        print("Consumer Groups:")
        for groupid in groups:
            print("\t{groupid}".format(groupid=groupid))
        print(
            "{num_groups} groups found for cluster {cluster_name} "
            "of type {cluster_type}".format(
                num_groups=len(groups),
                cluster_name=cluster_config.name,
                cluster_type=cluster_config.type,
            ),
        )

    @classmethod
    def run(cls, args, cluster_config):
        groups = set()

        if args.storage in ('dual', 'zookeeper'):
            zk_groups = cls.get_zookeeper_groups(cluster_config)
            if zk_groups:
                groups.update(zk_groups)

        if args.storage in ('dual', 'kafka'):
            kafka_groups = cls.get_kafka_groups(cluster_config)
            if kafka_groups:
                groups.update(kafka_groups)

        cls.print_groups(groups, cluster_config)
