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
from collections import defaultdict

from .offset_manager import OffsetWriter
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import set_consumer_offsets


class OffsetSet(OffsetWriter):
    new_offsets_dict = defaultdict(dict)

    @classmethod
    def topics_dict(cls, string):
        try:
            topic, partition_offset = string.rsplit(".", 1)
            partition, offset = partition_offset.split("=", 1)
            cls.new_offsets_dict[topic][int(partition)] = int(offset)
        except ValueError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.",
                file=sys.stderr,
            )
            sys.exit(1)

    @classmethod
    def add_parser(cls, subparsers):
        parser_offset_set = subparsers.add_parser(
            "offset_set",
            description="Modify consumer offsets for the specified consumer "
            "group to the specified offset.",
            add_help=False,
        )
        parser_offset_set.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit.",
        )
        parser_offset_set.add_argument(
            'groupid',
            help="Consumer Group ID whose consumer offsets shall be modified.",
        )

        parser_offset_set.add_argument(
            "newoffsets", nargs='+', metavar=('<topic>.<partition>=<offset>'),
            type=cls.topics_dict,
            help="Tuple containing the Kafka topic, partition and "
            "the the intended "
            "new offset.",
        )
        parser_offset_set.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing where to store the committed offsets.",
            default='kafka',
        )
        parser_offset_set.add_argument(
            '--force',
            action='store_true',
            help="Force the offset of the group to be committed even if "
            "it does not already exist.",
        )

        parser_offset_set.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        # Let's verify that the consumer does exist in Zookeeper
        if not args.force:
            cls.get_topics_from_consumer_group_id(
                cluster_config,
                args.groupid,
                storage=args.storage,
            )

        try:
            results = set_consumer_offsets(
                client,
                args.groupid,
                cls.new_offsets_dict,
                offset_storage=args.storage,
            )
        except TypeError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.", file=sys.stderr
            )
            raise

        client.close()

        if results:
            final_error_str = ("Error: Unable to commit consumer offsets for:\n")
            for result in results:
                error_str = (
                    "  Topic: {topic} Partition: {partition} Error: {error}\n".format(
                        topic=result.topic,
                        partition=result.partition,
                        error=result.error
                    )
                )
                final_error_str += error_str
            print(final_error_str, file=sys.stderr)
            sys.exit(1)
