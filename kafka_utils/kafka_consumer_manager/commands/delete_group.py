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

from .offset_manager import OffsetWriter
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import nullify_offsets
from kafka_utils.util.offsets import set_consumer_offsets
from kafka_utils.util.zookeeper import ZK


class DeleteGroup(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_delete_group = subparsers.add_parser(
            "delete_group",
            description="Delete a consumer group by groupid. This "
            "tool shall delete all group offset metadata from Zookeeper.",
            add_help=False
        )
        parser_delete_group.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_delete_group.add_argument(
            'groupid',
            help="Consumer Group IDs whose metadata shall be deleted."
        )
        parser_delete_group.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing where to store the committed offsets.",
        )
        parser_delete_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid, None, None, cluster_config, client
        )
        if not args.storage or args.storage == 'zookeeper':
            cls.delete_group_zk(cluster_config, args.groupid)
        elif args.storage == 'kafka':
            cls.delete_group_kafka(client, args.groupid, topics_dict)
        else:
            print(
                "Error: Invalid offset storage option: "
                "{}.".format(args.storage),
                file=sys.stderr,
            )
            sys.exit(1)

    @classmethod
    def delete_group_zk(cls, cluster_config, group):
        with ZK(cluster_config) as zk:
            zk.delete_group(group)

    @classmethod
    def delete_group_kafka(cls, client, group, topics):
        new_offsets = nullify_offsets(topics)
        set_consumer_offsets(
            client,
            group,
            new_offsets,
            offset_storage='kafka',
        )
