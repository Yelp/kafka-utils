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
from kafka_utils.util.zookeeper import ZK


class CopyGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_copy_group = subparsers.add_parser(
            "copy_group",
            description="Copy specified consumer group details to a new group.",
        )
        parser_copy_group.add_argument(
            'source_groupid',
            help="Consumer Group to be copied.",
        )
        parser_copy_group.add_argument(
            'dest_groupid',
            help="New name for the consumer group being copied to.",
        )
        parser_copy_group.add_argument(
            "--topic",
            help="Kafka topic whose offsets will be copied into destination group"
            " If no topic is specificed all topic offsets will be copied.",
        )
        parser_copy_group.add_argument(
            "--partitions",
            nargs='+',
            type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be copied.",
        )
        parser_copy_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        if args.source_groupid == args.dest_groupid:
            print(
                "Error: Source group ID and destination group ID are same.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()
        source_topics = cls.preprocess_args(
            args.source_groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
        )
        with ZK(cluster_config) as zk:
            try:
                topics_dest_group = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=args.dest_groupid,
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                preprocess_topics(
                    args.source_groupid,
                    source_topics.keys(),
                    args.dest_groupid,
                    topics_dest_group,
                )

            # Fetch offsets
            source_offsets = fetch_offsets(zk, args.source_groupid, source_topics)
            create_offsets(zk, args.dest_groupid, source_offsets)
