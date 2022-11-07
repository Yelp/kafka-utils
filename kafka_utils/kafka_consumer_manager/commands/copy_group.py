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
from __future__ import annotations

import argparse
import sys
from typing import Any

from .offset_manager import OffsetManagerBase
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import set_consumer_offsets


class CopyGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers: Any) -> None:
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
    def run(cls, args: argparse.Namespace, cluster_config: ClusterConfig) -> None:
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
            use_admin_client=args.use_admin_client,
        )

        cls.copy_group_kafka(
            client,
            source_topics,
            args.source_groupid,
            args.dest_groupid,
        )

    @classmethod
    def copy_group_kafka(cls, client: KafkaToolClient, topics: dict[str, list[int]], source_group: str, destination_group: str) -> None:
        copied_offsets = get_current_consumer_offsets(client, source_group, topics)
        set_consumer_offsets(client, destination_group, copied_offsets)
