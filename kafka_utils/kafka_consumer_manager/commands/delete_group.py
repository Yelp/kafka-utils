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
from typing import Any

from .offset_manager import OffsetWriter
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.offsets import nullify_offsets
from kafka_utils.util.offsets import set_consumer_offsets


class DeleteGroup(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers: Any):
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
        parser_delete_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args: argparse.Namespace, cluster_config: ClusterConfig) -> None:
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid,
            None,
            None,
            cluster_config,
            client,
            use_admin_client=args.use_admin_client,
        )
        cls.delete_group_kafka(client, args.groupid, topics_dict)

    @classmethod
    def delete_group_kafka(cls, client: KafkaToolClient, group: str, topics: dict[str, dict[int, int]]) -> None:
        new_offsets = nullify_offsets(topics)
        set_consumer_offsets(client, group, new_offsets)
