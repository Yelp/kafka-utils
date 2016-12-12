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
from kazoo.exceptions import NodeExistsError

from kafka_utils.util.offsets import get_topics_watermarks


CONSUMER_OFFSET_TOPIC = '__consumer_offsets'
CONSUMER_OFFSET_TOPIC_PARTITIONS = 50  # Kafka default


def preprocess_topics(source_groupid, source_topics, dest_groupid, topics_dest_group):
    """Pre-process the topics in source and destination group for duplicates."""
    # Is the new consumer already subscribed to any of these topics?
    common_topics = [topic for topic in topics_dest_group if topic in source_topics]
    if common_topics:
        print(
            "Error: Consumer Group ID: {groupid} is already "
            "subscribed to following topics: {topic}.\nPlease delete this "
            "topics from new group before re-running the "
            "command.".format(
                groupid=dest_groupid,
                topic=', '.join(common_topics),
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    # Let's confirm what the user intends to do.
    if topics_dest_group:
        in_str = (
            "New Consumer Group: {dest_groupid} already "
            "exists.\nTopics subscribed to by the consumer groups are listed "
            "below:\n{source_groupid}: {source_group_topics}\n"
            "{dest_groupid}: {dest_group_topics}\nDo you intend to copy into"
            "existing consumer destination-group? (y/n)".format(
                source_groupid=source_groupid,
                source_group_topics=source_topics,
                dest_groupid=dest_groupid,
                dest_group_topics=topics_dest_group,
            )
        )
        prompt_user_input(in_str)


def create_offsets(zk, consumer_group, offsets):
    """Create path with offset value for each topic-partition of given consumer
    group.

    :param zk: Zookeeper client
    :param consumer_group: Consumer group id for given offsets
    :type consumer_group: int
    :param offsets: Offsets of all topic-partitions
    :type offsets: dict(topic, dict(partition, offset))
    """
    # Create new offsets
    for topic, partition_offsets in offsets.iteritems():
        for partition, offset in partition_offsets.iteritems():
            new_path = "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                groupid=consumer_group,
                topic=topic,
                partition=partition,
            )
            try:
                zk.create(new_path, value=bytes(offset), makepath=True)
            except NodeExistsError:
                print(
                    "Error: Path {path} already exists. Please re-run the "
                    "command.".format(path=new_path),
                    file=sys.stderr,
                )
                raise


def fetch_offsets(zk, consumer_group, topics):
    """Fetch offsets for given topics of given consumer group.

    :param zk: Zookeeper client
    :param consumer_group: Consumer group id for given offsets
    :type consumer_group: int
    :rtype: dict(topic, dict(partition, offset))
    """
    source_offsets = defaultdict(dict)
    for topic, partitions in topics.iteritems():
        for partition in partitions:
            offset, _ = zk.get(
                "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                    groupid=consumer_group,
                    topic=topic,
                    partition=partition,
                )
            )
            source_offsets[topic][partition] = offset
    return source_offsets


def prompt_user_input(in_str):
    while(True):
        answer = raw_input(in_str + ' ')
        if answer == "n" or answer == "no":
            sys.exit(0)
        if answer == "y" or answer == "yes":
            return


# The mapping of group information to partition for the __consumer_offsets
# topic is determinated by a hash of the group id, modulo the number of
# partitions in the topic.
# Kafka code (from main/scala/kafka/coordinator/GroupMetadataManager.scala):
#   def partitionFor(groupId: String): Int =
#       Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
# hashCode returns the hash of the string according to the Java default string
# hashing algorithm. The algorithm is implemented in the inner function
# java_string_hashcode.
def get_group_partition(group):
    """Given a group name, return the partition number of the consumer offset
    topic containing the data associated to that group."""
    def java_string_hashcode(s):
        h = 0
        for c in s:
            h = (31 * h + ord(c)) & 0xFFFFFFFF
        return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
    return abs(java_string_hashcode(group)) % CONSUMER_OFFSET_TOPIC_PARTITIONS


class InvalidMessageException(Exception):
    pass


class KafkaGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.kafka_config = kafka_config
        self.kafka_groups = defaultdict(set)
        self.finished_partitions = set()
        self.retry_max = 3

    def read_group(self, group_id):
        partition = get_group_partition(group_id)
        return self.read_groups(partition).get(group_id, [])

    def read_groups(self, partition=None):
        self.log.info("Kafka consumer running")
        if partition:
            topic_partition = {CONSUMER_OFFSET_TOPIC: [partition]}
        else:
            topic_partition = CONSUMER_OFFSET_TOPIC

        self.consumer = KafkaConsumer(
            topic_partition,
            group_id='offset_monitoring_consumer',
            bootstrap_servers=self.kafka_config.broker_list,
            auto_offset_reset='smallest',
            auto_commit_enable=False,
            consumer_timeout_ms=3000,
        )
        self.log.info("Consumer ready")
        self.watermarks = self.get_current_watermarks(partition)
        self.retry = 0
        while not self.finished():
            try:
                message = self.consumer.next()
                max_offset = self.get_max_offset(message.partition)
                if message.offset >= max_offset - 1:
                    self.finished_partitions.add(message.partition)
                self.process_consumer_offset_message(message)
            except ConsumerTimeout:
                break
            except (
                    FailedPayloadsError,
                    KafkaUnavailableError,
                    LeaderNotAvailableError,
                    NotLeaderForPartitionError,
            ) as e:
                self.retry += 1
                self.log.warning("Got %s, retrying", e.__class__.__name__)
        return self.kafka_groups

    def parse_consumer_offset_message(self, message):
        key = bytearray(message.key)
        ((key_schema,), cur) = relative_unpack(b'>h', key, 0)
        if key_schema not in [0, 1]:
            raise InvalidMessageException()   # This is not an offset commit message
        (group, cur) = read_short_string(key, cur)
        (topic, cur) = read_short_string(key, cur)
        ((partition,), cur) = relative_unpack(b'>l', key, cur)
        if message.value:
            value = bytearray(message.value)
            ((value_schema,), cur) = relative_unpack(b'>h', value, 0)
            if value_schema not in [0, 1]:
                raise InvalidMessageException()  # Unrecognized message value
            ((offset,), cur) = relative_unpack(b'>q', value, cur)
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

    def get_current_watermarks(self, partition=None):
        self.consumer._client.load_metadata_for_topics()
        offsets = get_topics_watermarks(
            self.consumer._client,
            [CONSUMER_OFFSET_TOPIC],
        )
        return {part: offset for part, offset
                in offsets[CONSUMER_OFFSET_TOPIC].iteritems()
                if offset.highmark > offset.lowmark and
                (partition is None or part == partition)}

    def get_max_offset(self, partition):
        return self.watermarks[partition].highmark

    def finished(self):
        return (self.retry > self.retry_max) or len(self.finished_partitions) >= len(self.watermarks)
