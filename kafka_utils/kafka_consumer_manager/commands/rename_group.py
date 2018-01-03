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

from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from kafka_utils.kafka_consumer_manager.util import create_offsets
from kafka_utils.kafka_consumer_manager.util import fetch_offsets
from kafka_utils.kafka_consumer_manager.util import preprocess_topics
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import nullify_offsets
from kafka_utils.util.offsets import set_consumer_offsets
from kafka_utils.util.zookeeper import ZK


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
        parser_rename_group.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing the storage type",
            default='kafka',
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
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            groupid=args.old_groupid,
            topic=None,
            partitions=None,
            cluster_config=cluster_config,
            client=client,
            storage=args.storage,
        )
        if args.storage == 'kafka':
            cls.rename_group_with_storage_kafka(
                client,
                args.old_groupid,
                args.new_groupid,
                topics_dict,
            )
        else:
            cls.rename_group_with_storage_zookeeper(
                args.old_groupid,
                args.new_groupid,
                topics_dict,
                cluster_config,
            )

    @classmethod
    def rename_group_with_storage_kafka(
        cls,
        client,
        old_groupid,
        new_groupid,
        topics,
    ):
        copied_offsets = get_current_consumer_offsets(
            client,
            old_groupid,
            topics,
            offset_storage='kafka',
        )
        set_consumer_offsets(
            client,
            new_groupid,
            copied_offsets,
            offset_storage='kafka',
        )
        set_consumer_offsets(
            client,
            old_groupid,
            nullify_offsets(topics),
            offset_storage='kafka',
        )

    @classmethod
    def rename_group_with_storage_zookeeper(
        cls,
        old_groupid,
        new_groupid,
        topics_dict,
        cluster_config
    ):
        with ZK(cluster_config) as zk:
            try:
                topics = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=new_groupid
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                preprocess_topics(
                    old_groupid,
                    list(topics_dict.keys()),
                    new_groupid,
                    topics,
                )

            old_offsets = fetch_offsets(zk, old_groupid, topics_dict)
            create_offsets(zk, new_groupid, old_offsets)
            try:
                old_base_path = "/consumers/{groupid}".format(
                    groupid=old_groupid,
                )
                zk.delete(old_base_path, recursive=True)
            except Exception:
                print(
                    "Error: Unable to migrate all metadata in Zookeeper. "
                    "Please re-run the command.",
                    file=sys.stderr
                )
                raise
