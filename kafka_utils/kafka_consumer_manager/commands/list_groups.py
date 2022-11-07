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
from collections.abc import Collection
from typing import Any

from .offset_manager import OffsetManagerBase
from kafka_utils.kafka_consumer_manager.util import get_kafka_group_reader
from kafka_utils.util.config import ClusterConfig


class ListGroups(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers: Any) -> None:
        parser_list_groups = subparsers.add_parser(
            "list_groups",
            description="List consumer groups.",
            add_help=False,
        )
        parser_list_groups.set_defaults(command=cls.run)

    @classmethod
    def get_kafka_groups(cls, cluster_config: ClusterConfig, use_admin_client: bool = False) -> list[int]:
        '''Get the group_id of groups committed into Kafka.'''
        kafka_group_reader = get_kafka_group_reader(cluster_config, use_admin_client)
        groups_and_topics = kafka_group_reader.read_groups(list_only=True)
        return list(groups_and_topics.keys())

    @classmethod
    def print_groups(cls, groups: Collection[int], cluster_config: ClusterConfig) -> None:
        print("Consumer Groups:")
        for groupid in groups:
            print(f"\t{groupid}")
        print(
            "{num_groups} groups found for cluster {cluster_name} "
            "of type {cluster_type}".format(
                num_groups=len(groups),
                cluster_name=cluster_config.name,
                cluster_type=cluster_config.type,
            ),
        )

    @classmethod
    def run(cls, args: argparse.Namespace, cluster_config: ClusterConfig) -> None:
        groups = set()
        kafka_groups = cls.get_kafka_groups(
            cluster_config,
            args.use_admin_client,
        )
        if kafka_groups:
            groups.update(kafka_groups)

        cls.print_groups(groups, cluster_config)
