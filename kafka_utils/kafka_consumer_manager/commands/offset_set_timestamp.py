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
from collections import OrderedDict
from datetime import datetime

import pytz
import six
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from .offset_manager import OffsetManagerBase
from kafka_utils.kafka_consumer_manager.util import consumer_commit_for_times
from kafka_utils.kafka_consumer_manager.util import topic_offsets_for_timestamp


class OffsetSetTimestamp(OffsetManagerBase):

    new_offsets_dict = defaultdict(dict)

    @classmethod
    def topics_dict(cls, string):
        try:
            topic, partition_timestamp = string.rsplit(".", 1)
            partition, timestamp = partition_timestamp.split("=", 1)
            cls.new_offsets_dict[topic][int(partition)] = int(timestamp)
        except ValueError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.",
                file=sys.stderr,
            )
            sys.exit(1)

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_set = subparsers.add_parser(
            "offset_set_timestamp",
            description="Sets the offset(s) associated with the given timestamp for"
            " the given TopicPartitions for the consumer group. A single timestamp can"
            " also be provided to set offsets for all topic partitions the consumer group"
            " has offsets for. Note: all times are displayed in US/Pacific Standard Time.",
            add_help=False,
        )
        parser_offset_set.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit.",
        )
        parser_offset_set.add_argument(
            "groupid",
            help="Consumer Group ID whose consumer offsets shall be modified.",
        )
        parser_offset_set.add_argument(
            "--timestamp",
            type=int,
            help="The Unix Epoch timestamp, in milliseconds, to set all offsets"
            " of the consumer group to.",
        )
        parser_offset_set.add_argument(
            "newoffsets", nargs="*", metavar=("<topic>.<partition>=<timestamp>"),
            type=cls.topics_dict,
            help="Tuple containing the Kafka topic, partition and"
            " the intended timestamp to use for setting offsets.",
        )
        parser_offset_set.add_argument(
            "--atomic",
            action="store_true",
            help="Don't commit any offsets if any TopicPartition: timestamp mapping"
            " has no offsets found. By default, any offsets that can be committed are"
            " committed.",
        )
        parser_offset_set.set_defaults(command=cls.run)

    @classmethod
    def validate_args(cls, args):
        if args.timestamp and len(cls.new_offsets_dict) > 0:
            print("Error: cannot supply both a timestamp and list of topic.partition=timestamp.")
            print("Re-run with --help for more information.")
            return False
        return True

    @classmethod
    def run(cls, args, cluster_config):

        if not cls.validate_args(args):
            return

        # Set up the KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=cluster_config.broker_list,
            client_id="kafka-consumer-manager",
            group_id=args.groupid,
            enable_auto_commit=False,
        )

        tp_timestamps = {}
        # Get topics that the consumer group has committed offsets for in the past
        topics = cls.get_topics_from_consumer_group_id(cluster_config, args.groupid)

        if args.timestamp and len(cls.new_offsets_dict) == 0:
            # Committing for using --timestamp for all topics consumer group follows
            if len(topics) == 0:
                return
            partition_to_offset = topic_offsets_for_timestamp(consumer, args.timestamp, topics)
            for tp in six.iterkeys(partition_to_offset):
                tp_timestamps[tp] = args.timestamp
        else:
            # Committing via a list of topic.partition=timestamp
            for topic in six.iterkeys(cls.new_offsets_dict):
                for partition in six.iterkeys(cls.new_offsets_dict[topic]):
                    tp_timestamps[TopicPartition(topic, partition)] = cls.new_offsets_dict[topic][partition]
            partition_to_offset = consumer.offsets_for_times(tp_timestamps)

        cls.print_offsets(partition_to_offset, tp_timestamps)

        # Display warning if consumer is attempting to commit offsets for a topic they
        # have never committed offsets for.
        for tp in six.iterkeys(partition_to_offset):
            if tp.topic not in topics:
                print(
                    "WARNING: Consumer group {cg} has never committed offsets for topic {topic}.".format(
                        cg=args.groupid,
                        topic=tp.topic,
                    )
                )

        if cls.prompt_user():
            consumer_commit_for_times(consumer, partition_to_offset, atomic=args.atomic)
        else:
            print("Offsets not committed")

    @classmethod
    def prompt_user(cls):
        print("The above TopicPartitions and offsets will attempt to be committed.")
        print("Continue? (y/n)")

        return six.moves.input() == "y"

    @classmethod
    def print_offsets(cls, partition_to_offset, tp_timestamps):
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
                timestamp = tp_timestamps[TopicPartition(topic, partition)]
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
