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

import sys

from kafka import KafkaClient
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetWriter
from kafka_tools.util.zookeeper import ZK


class UnsubscribeTopics(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_unsubscribe_topics = subparsers.add_parser(
            "unsubscribe_topics",
            description="Delete topics and partitions by consumer group. This "
            "tool shall delete all offset metadata from Zookeeper.",
            add_help=False
        )
        parser_unsubscribe_topics.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_unsubscribe_topics.add_argument(
            'groupid',
            help="Consumer Group IDs whose metadata shall be deleted."
        )
        parser_unsubscribe_topics.add_argument(
            '--topic',
            help="Topic whose metadata shall be deleted. If no topic is "
            "specified, all topics that the consumer is subscribed to, shall "
            "be deleted."
        )
        parser_unsubscribe_topics.add_argument(
            '--partitions', nargs='+', type=int,
            help="List of partitions whose metadata shall be deleted. If no "
            "partitions are specified, all partitions within the topic shall "
            "be deleted."
        )
        parser_unsubscribe_topics.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid, args.topic, args.partitions, cluster_config, client
        )
        with ZK(cluster_config) as zk:
            if args.topic and args.partitions:
                cls.unsubscribe_partitions(
                    zk,
                    args.groupid,
                    args.topic,
                    args.partitions,
                )
            elif args.topic:
                zk.delete_topic(args.groupid, args.topic)
            else:
                for topic, partitions in topics_dict.iteritems():
                    zk.delete_topic(args.groupid, topic)

    @classmethod
    def unsubscribe_partitions(cls, zk, groupid, topic, partitions):
        try:
            zk.delete_topic_partitions(groupid, topic, partitions)
        except NoNodeError:
            print(
                "WARNING: No node found for topic {}, partition {}".format(
                    topic,
                    partitions,
                ),
                file=sys.stderr,
            )
        if not zk.get_my_subscribed_partitions(groupid, topic):
            zk.delete_topic(groupid, topic)
