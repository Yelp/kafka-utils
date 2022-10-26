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
from collections import OrderedDict
from typing import Any

from .offset_manager import OffsetManagerBase
from kafka_utils.util import print_json
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.monitoring import ConsumerPartitionOffsets
from kafka_utils.util.monitoring import get_consumer_offsets_metadata


class OffsetGet(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers: Any) -> None:
        parser_offset_get = subparsers.add_parser(
            "offset_get",
            description="Get consumer offsets for the"
            " specified consumer group",
            add_help=False
        )
        parser_offset_get.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_offset_get.add_argument(
            'groupid',
            help="Consumer Group ID whose offsets shall be fetched."
        )
        parser_offset_get.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be fetched. If no topic is "
            "specified, offsets from all topics that the consumer is "
            "subscribed to, shall be fetched."
        )
        parser_offset_get.add_argument(
            "--partitions", nargs='+', type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be fetched."
        )
        parser_offset_get.add_argument(
            "--watermark",
            choices=["high", "low", "current", "distance", "all"],
            help="Type of offset watermark. \"high\" represents the offset "
            "corresponding to the latest message. \"low\" represents "
            "the offset corresponding to the earliest message. \"current\" "
            "represents the current committed offset for this consumer. "
            "\"distance\" represents the offset distance b/w high-watermark "
            "and current-offset.",
            default="all"
        )
        parser_offset_get.add_argument(
            "-j", "--json", action="store_true",
            help="Export data in json format."
        )
        sort_parser = parser_offset_get.add_mutually_exclusive_group()
        sort_parser.add_argument(
            "--sort-by-distance", action="store_true",
            help="Sort the output by increasing topic distance."
        )
        sort_parser.add_argument(
            "--sort-by-distance-percentage", action="store_true",
            help="Sort the output by increasing topic distance percentage."
        )
        parser_offset_get.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args: argparse.Namespace, cluster_config: ClusterConfig) -> None:
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            groupid=args.groupid,
            topic=args.topic,
            partitions=args.partitions,
            cluster_config=cluster_config,
            client=client,
            quiet=args.json,
            use_admin_client=args.use_admin_client,
        )

        consumer_offsets_metadata = cls.get_offsets(
            client,
            args.groupid,
            topics_dict,
        )
        client.close()

        if args.sort_by_distance:
            consumer_offsets_metadata = cls.sort_by_distance(consumer_offsets_metadata)
        elif args.sort_by_distance_percentage:
            consumer_offsets_metadata = cls.sort_by_distance_percentage(consumer_offsets_metadata)

        if args.json:
            partitions_info = []
            for partitions in consumer_offsets_metadata.values():
                for partition in partitions:
                    partition_info = partition._asdict()
                    partition_info['offset_distance'] = partition_info['highmark'] - partition_info['current']
                    partition_info['percentage_distance'] = cls.percentage_distance(
                        partition_info['highmark'],
                        partition_info['current']
                    )
                    partitions_info.append(partition_info)
            print_json(partitions_info)
        else:
            # Warn the user if a topic being subscribed to does not exist in
            # Kafka.
            for topic in topics_dict:
                if topic not in consumer_offsets_metadata:
                    print(
                        "Warning: Topic {topic} or one or more of it's partitions "
                        "do not exist in Kafka".format(topic=topic),
                        file=sys.stderr,
                    )
            cls.print_output(consumer_offsets_metadata, args.watermark)

    @classmethod
    def sort_by_distance(cls, consumer_offsets_metadata: dict[str, list[ConsumerPartitionOffsets]]) -> dict[str, list[ConsumerPartitionOffsets]]:
        """Receives a dict of (topic_name: ConsumerPartitionOffsets) and returns a
        similar dict where the topics are sorted by total offset distance."""
        sorted_offsets = sorted(
            list(consumer_offsets_metadata.items()),
            key=lambda topic_offsets: sum([o.highmark - o.current for o in topic_offsets[1]])
        )
        return OrderedDict(sorted_offsets)

    @classmethod
    def sort_by_distance_percentage(cls, consumer_offsets_metadata: dict[str, list[ConsumerPartitionOffsets]]) -> dict[str, list[ConsumerPartitionOffsets]]:
        """Receives a dict of (topic_name: ConsumerPartitionOffset) and returns an
        similar dict where the topics are sorted by average offset distance
        in percentage."""
        sorted_offsets = sorted(
            list(consumer_offsets_metadata.items()),
            key=lambda topic_offsets1: sum(
                [cls.percentage_distance(o.highmark, o.current) for o in topic_offsets1[1]]
            )
        )
        return OrderedDict(sorted_offsets)

    @classmethod
    def get_offsets(cls, client: KafkaToolClient, group: str, topics_dict: dict[str, list[int]]) -> dict[str, list[ConsumerPartitionOffsets]]:
        try:
            return get_consumer_offsets_metadata(
                client, group, topics_dict, False,
            )
        except Exception:
            print(
                "Error: Encountered error with Kafka, please try again later.",
                file=sys.stderr
            )
            raise

    @classmethod
    def print_output(cls, consumer_offsets_metadata: dict[str, list[ConsumerPartitionOffsets]], watermark_filter: str) -> None:
        for topic, metadata_tuples in consumer_offsets_metadata.items():
            diff_sum = sum([t.highmark - t.current for t in metadata_tuples])
            print(f"Topic Name: {topic}  Total Distance: {diff_sum}")
            for metadata_tuple in metadata_tuples:
                print(
                    "\tPartition ID: {partition}".format(
                        partition=metadata_tuple.partition
                    )
                )
                if watermark_filter == "all" or watermark_filter == "high":
                    print(
                        "\t\tHigh Watermark: {high}".format(
                            high=metadata_tuple.highmark
                        )
                    )
                if watermark_filter == "all" or watermark_filter == "low":
                    print(
                        "\t\tLow Watermark: {low}".format(
                            low=metadata_tuple.lowmark
                        )
                    )
                if watermark_filter == "all" or watermark_filter == "current":
                    print(
                        "\t\tCurrent Offset: {current}".format(
                            current=metadata_tuple.current
                        )
                    )
                if watermark_filter == "all" or watermark_filter == "distance":
                    per_distance = cls.percentage_distance(
                        metadata_tuple.highmark,
                        metadata_tuple.current
                    )
                    print(
                        "\t\tOffset Distance: {distance}".format(
                            distance=(metadata_tuple.highmark - metadata_tuple.current)
                        )
                    )
                    print(
                        "\t\tPercentage Distance: {per_distance}%".format(
                            per_distance=per_distance
                        )
                    )

    @classmethod
    def percentage_distance(cls, highmark: int, current: int) -> float:
        """Percentage of distance the current offset is behind the highmark."""
        highmark = int(highmark)
        current = int(current)
        if highmark > 0:
            return round(
                (highmark - current) * 100.0 / highmark,
                2,
            )
        else:
            return 0.0
