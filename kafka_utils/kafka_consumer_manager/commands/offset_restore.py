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
from contextlib import closing

from .offset_manager import OffsetManagerBase
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.monitoring import get_consumer_offsets_metadata
from kafka_utils.util.offsets import set_consumer_offsets


class OffsetRestore(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_restore = subparsers.add_parser(
            "offset_restore",
            description="Commit current consumer offsets for consumer group"
            " specified in given json file.",
            add_help=False,
        )
        parser_offset_restore.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        parser_offset_restore.add_argument(
            "json_file",
            type=str,
            help="Json file containing offset information",
        )
        parser_offset_restore.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing where to store the committed offsets.",
            default='zookeeper'
        )
        parser_offset_restore.set_defaults(command=cls.run)

    @classmethod
    def parse_consumer_offsets(cls, json_file):
        """Parse current offsets from json-file."""
        with open(json_file, 'r') as consumer_offsets_json:
            try:
                parsed_offsets = {}
                parsed_offsets_data = json.load(consumer_offsets_json)
                # Create new dict with partition-keys as integers
                parsed_offsets['groupid'] = parsed_offsets_data['groupid']
                parsed_offsets['offsets'] = {}
                for topic, topic_data in parsed_offsets_data['offsets'].iteritems():
                    parsed_offsets['offsets'][topic] = {}
                    for partition, offset in topic_data.iteritems():
                        parsed_offsets['offsets'][topic][int(partition)] = offset
                return parsed_offsets
            except ValueError:
                print(
                    "Error: Given consumer-data json data-file {file} could not be "
                    "parsed".format(file=json_file),
                    file=sys.stderr,
                )
                raise

    @classmethod
    def build_new_offsets(cls, client, topics_offset_data, topic_partitions, current_offsets):
        """Build complete consumer offsets from parsed current consumer-offsets
        and lowmarks and highmarks from current-offsets for.
        """
        new_offsets = defaultdict(dict)
        try:
            for topic, partitions in topic_partitions.iteritems():
                # Validate current offsets in range of low and highmarks
                # Currently we only validate for positive offsets and warn
                # if out of range of low and highmarks
                valid_partitions = set()
                for topic_partition_offsets in current_offsets[topic]:
                    partition = topic_partition_offsets.partition
                    valid_partitions.add(partition)
                    # Skip the partition not present in list
                    if partition not in topic_partitions[topic]:
                        continue
                    lowmark = topic_partition_offsets.lowmark
                    highmark = topic_partition_offsets.highmark
                    new_offset = topics_offset_data[topic][partition]
                    if new_offset < 0:
                        print(
                            "Error: Given offset: {offset} is negative"
                            .format(offset=new_offset),
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    if new_offset < lowmark or new_offset > highmark:
                        print(
                            "Warning: Given offset {offset} for topic-partition "
                            "{topic}:{partition} is outside the range of lowmark "
                            "{lowmark} and highmark {highmark}".format(
                                offset=new_offset,
                                topic=topic,
                                partition=partition,
                                lowmark=lowmark,
                                highmark=highmark,
                            )
                        )
                    new_offsets[topic][partition] = new_offset
                if not set(partitions).issubset(valid_partitions):
                    print(
                        "Error: Some invalid partitions {partitions} for topic "
                        "{topic} found. Valid partition-list {valid_partitions}. "
                        "Exiting...".format(
                            partitions=', '.join([str(p) for p in partitions]),
                            valid_partitions=', '.join([str(p) for p in valid_partitions]),
                            topic=topic,
                        ),
                        file=sys.stderr,
                    )
                    sys.exit(1)
        except KeyError as ex:
            print(
                "Error: Possible invalid topic or partition. Error msg: {ex}. "
                "Exiting...".format(ex=ex),
            )
            sys.exit(1)
        return new_offsets

    @classmethod
    def run(cls, args, cluster_config):
        # Fetch offsets from given json-file
        parsed_consumer_offsets = cls.parse_consumer_offsets(args.json_file)
        # Setup the Kafka client
        with closing(KafkaToolClient(cluster_config.broker_list)) as client:
            client.load_metadata_for_topics()

            cls.restore_offsets(client, parsed_consumer_offsets, args.storage)

    @classmethod
    def restore_offsets(cls, client, parsed_consumer_offsets, storage):
        """Fetch current offsets from kafka, validate them against given
        consumer-offsets data and commit the new offsets.

        :param client: Kafka-client
        :param parsed_consumer_offsets: Parsed consumer offset data from json file
        :type parsed_consumer_offsets: dict(group: dict(topic: partition-offsets))
        :param storage: String describing where to store the committed offsets.
        """
        # Fetch current offsets
        try:
            consumer_group = parsed_consumer_offsets['groupid']
            topics_offset_data = parsed_consumer_offsets['offsets']
            topic_partitions = dict(
                (topic, [partition for partition in offset_data.keys()])
                for topic, offset_data in topics_offset_data.iteritems()
            )
        except IndexError:
            print(
                "Error: Given parsed consumer-offset data {consumer_offsets} "
                "could not be parsed".format(consumer_offsets=parsed_consumer_offsets),
                file=sys.stderr,
            )
            raise
        current_offsets = get_consumer_offsets_metadata(
            client,
            consumer_group,
            topic_partitions,
            offset_storage=storage,
        )
        # Build new offsets
        new_offsets = cls.build_new_offsets(
            client,
            topics_offset_data,
            topic_partitions,
            current_offsets,
        )

        # Commit offsets
        consumer_group = parsed_consumer_offsets['groupid']
        set_consumer_offsets(
            client,
            consumer_group,
            new_offsets,
            offset_storage=storage,
        )
        print("Restored to new offsets {offsets}".format(offsets=dict(new_offsets)))
