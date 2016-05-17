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

import json
import sys
from collections import defaultdict

from kafka.common import KafkaUnavailableError

from .offset_manager import OffsetManagerBase
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.monitoring import get_consumer_offsets_metadata


class OffsetSave(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_save = subparsers.add_parser(
            "offset_save",
            description="Save current consumer offsets for the"
            " specified consumer group.",
            add_help=False,
        )
        parser_offset_save.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        parser_offset_save.add_argument(
            'groupid',
            help="Consumer Group ID whose offsets shall be fetched.",
        )
        parser_offset_save.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be fetched. If no topic is "
            "specified, offsets from all topics that the consumer is "
            "subscribed to, shall be fetched.",
        )
        parser_offset_save.add_argument(
            "--partitions",
            nargs='+',
            type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be fetched.",
        )
        parser_offset_save.add_argument(
            "json_file",
            type=str,
            help="Export data in json format in the given file.",
        )
        parser_offset_save.add_argument(
            '--storage', choices=['zookeeper', 'kafka', 'dual'],
            help="String describing where to fetch the committed offsets.",
            default='dual'
        )
        parser_offset_save.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
        )
        try:
            consumer_offsets_metadata = get_consumer_offsets_metadata(
                client,
                args.groupid,
                topics_dict,
                offset_storage=args.storage,
            )
        except KafkaUnavailableError:
            print(
                "Error: Encountered error with Kafka, please try again later.",
                file=sys.stderr,
            )
            raise

        # Warn the user if a topic being subscribed to does not exist in Kafka.
        for topic in topics_dict:
            if topic not in consumer_offsets_metadata:
                print(
                    "Warning: Topic {topic} does not exist in Kafka"
                    .format(topic=topic),
                    file=sys.stderr,
                )

        cls.save_offsets(
            consumer_offsets_metadata,
            topics_dict,
            args.json_file,
            args.groupid,
        )
        client.close()

    @classmethod
    def save_offsets(
        cls,
        consumer_offsets_metadata,
        topics_dict,
        json_file,
        groupid,
    ):
        """Built offsets for given topic-partitions in required format from current
        offsets metadata and write to given json-file.

        :param consumer_offsets_metadata: Fetched consumer offsets from kafka.
        :param topics_dict: Dictionary of topic-partitions.
        :param json_file: Filename to store consumer-offsets.
        :param groupid: Current consumer-group.
        """
        # Build consumer-offset data in desired format
        current_consumer_offsets = defaultdict(dict)
        for topic, topic_offsets in consumer_offsets_metadata.iteritems():
            for partition_offset in topic_offsets:
                current_consumer_offsets[topic][partition_offset.partition] = \
                    partition_offset.current
        consumer_offsets_data = {'groupid': groupid, 'offsets': current_consumer_offsets}

        cls.write_offsets_to_file(json_file, consumer_offsets_data)

    @classmethod
    def write_offsets_to_file(cls, json_file_name, consumer_offsets_data):
        """Save built consumer-offsets data to given json file."""
        # Save consumer-offsets to file
        with open(json_file_name, "w") as json_file:
            try:
                json.dump(consumer_offsets_data, json_file)
            except ValueError:
                print("Error: Invalid json data {data}".format(data=consumer_offsets_data))
                raise
            print("Consumer offset data saved in json-file {file}".format(file=json_file_name))
