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

import six
from kafka.common import FailedPayloadsError

from .offset_manager import OffsetManagerBase
from kafka_utils.util import print_json
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.error import UnknownPartitions
from kafka_utils.util.error import UnknownTopic
from kafka_utils.util.monitoring import get_watermark_for_regex
from kafka_utils.util.monitoring import get_watermark_for_topic


class WatermarkGet(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_get = subparsers.add_parser(
            "get_topic_watermark", description="Get consumer offsets for the"
            " specified topics", add_help=False
        )
        parser_offset_get.add_argument(
            "-h", "--help", action="help", help="Show help message and exit."
        )
        parser_offset_get.add_argument(
            "topic", help="Kafka topic whose offsets shall be fetched. Regex"
            "are also supported. Default is regex, unless -e or --exact is "
            "provided, when exact topic name is considered"
        )
        parser_offset_get.add_argument(
            "-e", "--exact", action="store_true", help="Exactly match the "
            "topic, if not provided, topic will be considered as a regex input"
        )
        parser_offset_get.add_argument(
            "-j", "--json", action="store_true",
            help="Export data in json format."
        )
        parser_offset_get.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()
        watermarks = {}

        if args.exact:
            watermarks = cls.get_watermarks(
                client,
                args.topic,
                exact=True,
            )
        else:
            watermarks = cls.get_watermarks(
                client,
                args.topic,
                exact=False,
            )

        client.close()
        if args.json:
            print_json(watermarks)
        else:
            cls.print_output(watermarks)

    @classmethod
    def get_watermarks(cls, client, topic, exact=False):
        try:
            if not exact:
                return get_watermark_for_regex(client, topic)
            else:
                return get_watermark_for_topic(client, topic)
        except (UnknownPartitions, UnknownTopic, FailedPayloadsError) as e:
            print(
                "Error: Encountered error with Kafka, please try again later: ",
                e.message
            )
            raise

    @classmethod
    def print_output(cls, watermark):
        for key, value in six.iteritems(watermark):
            print("Topic Name: {topic}".format(
                topic=key
            ))
            for partition in value.values():
                print("\tPartition ID: {id}".format(
                    id=partition[1]
                ))
                print("\tHigh Watermark: {highmark}".format(
                    highmark=partition[2]
                ))
                print("\tLow Watermark: {lowmark}\n".format(
                    lowmark=partition[3]
                ))
