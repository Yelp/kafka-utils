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

import logging
import sys
from collections import defaultdict

from kafka.common import ConsumerTimeout
from kafka.common import FailedPayloadsError
from kafka.common import KafkaUnavailableError
from kafka.common import LeaderNotAvailableError
from kafka.common import NotLeaderForPartitionError
from kafka.consumer import KafkaConsumer
from kafka.util import read_short_string
from kafka.util import relative_unpack
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from kafka_utils.util.offsets import get_topics_watermarks
from kafka_utils.util.zookeeper import ZK

CONSUMER_OFFSET_TOPIC = '__consumer_offsets'


class ListGroups(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_groups = subparsers.add_parser(
            "list_groups",
            description="List consumer groups.",
            add_help=False,
        )
        parser_list_groups.add_argument(
            '--storage',
            choices=['zookeeper', 'kafka', 'dual'],
            help="String describing where to fetch the committed offsets.",
            default='dual'
        )
        parser_list_groups.set_defaults(command=cls.run)

    @classmethod
    def get_zookeeper_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Zookeeper.'''
        with ZK(cluster_config) as zk:
            try:
                return zk.get_children("/consumers")
            except NoNodeError:
                print(
                    "Error: No consumers node found in zookeeper",
                    file=sys.stderr,
                )

    @classmethod
    def get_kafka_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Kafka.'''
        kafka_group_reader = KafkaGroupReader(cluster_config)
        try:
            return kafka_group_reader.read_groups().keys()
        except:
            print(
                "Error: No consumer offsets topic found in Kafka",
                file=sys.stderr,
            )

    @classmethod
    def print_groups(cls, groups, cluster_config):
        print("Consumer Groups:")
        for groupid in groups:
            print("\t{groupid}".format(groupid=groupid))
        print(
            "{num_groups} groups found for cluster {cluster_name} "
            "of type {cluster_type}".format(
                num_groups=len(groups),
                cluster_name=cluster_config.name,
                cluster_type=cluster_config.type,
            ),
        )

    @classmethod
    def run(cls, args, cluster_config):
        groups = set()

        if args.storage in ('dual', 'zookeeper'):
            zk_groups = cls.get_zookeeper_groups(cluster_config)
            if zk_groups:
                groups.update(zk_groups)

        if args.storage in ('dual', 'kafka'):
            kafka_groups = cls.get_kafka_groups(cluster_config)
            if kafka_groups:
                groups.update(kafka_groups)

        cls.print_groups(groups, cluster_config)


class InvalidMessageException(Exception):
    pass


class KafkaGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.kafka_config = kafka_config
        self.kafka_groups = defaultdict(set)
        self.finished_partitions = set()

    def read_groups(self):
        self.log.info("Kafka consumer running")
        self.consumer = KafkaConsumer(
            CONSUMER_OFFSET_TOPIC,
            group_id='offset_monitoring_consumer',
            bootstrap_servers=self.kafka_config.broker_list,
            auto_offset_reset='smallest',
            auto_commit_enable=False,
            consumer_timeout_ms=10000,
        )
        self.log.info("Consumer ready")
        self.watermarks = self.get_current_watermarks()
        while not self.finished():
            try:
                message = self.consumer.next()
                max_offset = self.get_max_offset(message.partition)
                if message.offset >= max_offset - 1:
                    self.finished_partitions.add(message.partition)
            except ConsumerTimeout:
                break
            except (
                    FailedPayloadsError,
                    KafkaUnavailableError,
                    LeaderNotAvailableError,
                    NotLeaderForPartitionError,
            ) as e:
                self.log.warning("Got %s, retrying", e.__class__.__name__)
            self.process_consumer_offset_message(message)
        return self.kafka_groups

    def parse_consumer_offset_message(self, message):
        key = bytearray(message.key)
        ((key_schema,), cur) = relative_unpack('>h', key, 0)
        if key_schema not in [0, 1]:
            raise InvalidMessageException()   # This is not an offset commit message
        (group, cur) = read_short_string(key, cur)
        (topic, cur) = read_short_string(key, cur)
        ((partition,), cur) = relative_unpack('>l', key, cur)
        if message.value:
            value = bytearray(message.value)
            ((value_schema,), cur) = relative_unpack('>h', value, 0)
            if value_schema not in [0, 1]:
                raise InvalidMessageException()  # Unrecognized message value
            ((offset,), cur) = relative_unpack('>q', value, cur)
        else:
            offset = None  # Offset was deleted
        return str(group), str(topic), partition, offset

    def process_consumer_offset_message(self, message):
        try:
            group, topic, partition, offset = self.parse_consumer_offset_message(message)
        except InvalidMessageException:
            return

        if offset:
            self.kafka_groups[group].add(topic)
        else:  # No offset means group deletion
            self.kafka_groups.pop(group, None)

    def get_current_watermarks(self):
        self.consumer._client.load_metadata_for_topics()
        offsets = get_topics_watermarks(
            self.consumer._client,
            [CONSUMER_OFFSET_TOPIC],
        )
        return {partition: offset for partition, offset
                in offsets[CONSUMER_OFFSET_TOPIC].iteritems()
                if offset.highmark > offset.lowmark}

    def get_max_offset(self, partition):
        return self.watermarks[partition].highmark

    def finished(self):
        return len(self.finished_partitions) >= len(self.watermarks)
