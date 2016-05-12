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

import sys

from kafka import KafkaClient
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from kafka_tools.kafka_consumer_manager.util import create_offsets
from kafka_tools.kafka_consumer_manager.util import fetch_offsets
from kafka_tools.kafka_consumer_manager.util import preprocess_topics
from kafka_tools.util.zookeeper import ZK


class RenameGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_rename_group = subparsers.add_parser(
            "rename_group",
            description="Rename specified consumer group ID to a new name. "
            "This tool shall migrate all offset metadata in Zookeeper.",
            add_help=False
        )
        parser_rename_group.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_rename_group.add_argument(
            'old_groupid',
            help="Consumer Group ID to be renamed."
        )
        parser_rename_group.add_argument(
            'new_groupid',
            help="New name for the consumer group ID."
        )
        parser_rename_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        if args.old_groupid == args.new_groupid:
            print(
                "Error: Old group ID and new group ID are the same.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.old_groupid, None, None, cluster_config, client
        )
        with ZK(cluster_config) as zk:
            try:
                topics = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=args.new_groupid
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                preprocess_topics(
                    args.old_groupid,
                    topics_dict.keys(),
                    args.new_groupid,
                    topics,
                )

            old_offsets = fetch_offsets(zk, args.old_groupid, topics_dict)
            create_offsets(zk, args.new_groupid, old_offsets)
            try:
                old_base_path = "/consumers/{groupid}".format(
                    groupid=args.old_groupid,
                )
                zk.delete(old_base_path, recursive=True)
            except:
                print(
                    "Error: Unable to migrate all metadata in Zookeeper. "
                    "Please re-run the command.",
                    file=sys.stderr
                )
                raise
