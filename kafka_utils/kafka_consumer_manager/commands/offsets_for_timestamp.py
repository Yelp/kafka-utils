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

import argparse
from collections import OrderedDict
from datetime import datetime

import pytz
import six
from kafka import KafkaConsumer

from .offset_manager import OffsetManagerBase
from kafka_utils.kafka_consumer_manager.util import topic_offsets_for_timestamp


class OffsetsForTimestamp(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offsets_timestamp = subparsers.add_parser(
            "offsets_for_timestamp",
            description="Get the offset(s) associated with the given timestamp."
            " Can query for a single topic or for all subscribed topics of a"
            " consumer group. Note: all times are displayed in US/Pacific Standard Time."
            "\nExample usage:"
            "\n kafka-consumer-manager -t standard 1535147283123 --topics mytopic1 mytopic2",
            add_help=False,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        parser_offsets_timestamp.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit.",
        )
        parser_offsets_timestamp.add_argument(
            "timestamp",
            type=int,
            help="Unix Epoch timestamp, in milliseconds, to fetch offsets for.",
        )
        topic_or_cgroup = parser_offsets_timestamp.add_mutually_exclusive_group(required=True)
        topic_or_cgroup.add_argument(
            "--topics",
            nargs="+",
            help="The topic(s) to query for the offset associated with the timestamp.",
        )
        topic_or_cgroup.add_argument(
            "--groupid",
            help="The consumer group ID to query for all offsets associated with the"
            " timestamp in the subscribed topics.",
        )
        parser_offsets_timestamp.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Set up the KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=cluster_config.broker_list,
            client_id="kafka-consumer-manager",
            group_id=args.groupid,
            enable_auto_commit=False,
        )

        if args.groupid:
            args.topics = cls.get_topics_from_consumer_group_id(cluster_config, args.groupid)

        partition_to_offset = topic_offsets_for_timestamp(consumer, args.timestamp, topics=args.topics)

        cls.print_offsets(partition_to_offset, args.timestamp)

    @classmethod
    def print_offsets(cls, partition_to_offset, orig_timestamp):
        topics = {}
        for tp, offset_timestamp in six.iteritems(partition_to_offset):
            if tp.topic not in topics:
                topics[tp.topic] = {}
            topics[tp.topic][tp.partition] = offset_timestamp
        topics = OrderedDict(sorted(topics.items(), key=lambda k: k[0]))
        for topic in six.iterkeys(topics):
            topics[topic] = OrderedDict(sorted(topics[topic].items(), key=lambda k: k[0]))
            print("Topic Name: {}".format(topic))
            for partition, offset_timestamp in six.iteritems(topics[topic]):
                print(
                    "\tPartition ID: {}".format(partition),
                )
                offset = "not found"
                timestamp = orig_timestamp
                if offset_timestamp is not None:
                    offset = offset_timestamp.offset
                    timestamp = offset_timestamp.timestamp
                date = datetime.fromtimestamp(
                    timestamp / 1000.0,
                    tz=pytz.timezone("US/Pacific"),
                ).strftime("%Y-%m-%d %H:%M:%S %Z")
                print(
                    "\t\tTimestamp: {timestamp} ({date})".format(
                        timestamp=timestamp,
                        date=date,
                    ),
                )
                print("\t\tOffset: {offset}".format(offset=offset))
