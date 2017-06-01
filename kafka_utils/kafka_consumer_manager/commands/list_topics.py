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

import six

from .offset_manager import OffsetManagerBase
from kafka_utils.util.client import KafkaToolClient


class ListTopics(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_topics = subparsers.add_parser(
            "list_topics",
            description="List topics by consumer group.",
            add_help=False
        )
        parser_list_topics.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_list_topics.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing which storage source to query for topics.",
            default='kafka'
        )
        parser_list_topics.add_argument(
            'groupid',
            help="Consumer Group ID whose topics shall be fetched."
        )
        parser_list_topics.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            groupid=args.groupid,
            topic=None,
            partitions=None,
            cluster_config=cluster_config,
            client=client,
            storage=args.storage,
            fail_on_error=False,
        )
        if not topics_dict:
            print("Consumer Group ID: {group} does not exist in {storage}".format(
                group=args.groupid,
                storage=args.storage,
            ))
            sys.exit(1)

        print("Consumer Group ID: {groupid}".format(groupid=args.groupid))
        for topic, partitions in six.iteritems(topics_dict):
            print("\tTopic: {topic}".format(topic=topic))
            print("\t\tPartitions: {partitions}".format(partitions=partitions))
