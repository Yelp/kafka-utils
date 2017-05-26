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

import six
from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.util import read_short_string
from kafka.util import relative_unpack
from kazoo.exceptions import NodeExistsError
from six.moves import input

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.error import UnknownTopic
from kafka_utils.util.metadata import get_topic_partition_metadata
from kafka_utils.util.offsets import get_topics_watermarks


CONSUMER_OFFSET_TOPIC = '__consumer_offsets'


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
    for topic, partition_offsets in six.iteritems(offsets):
        for partition, offset in six.iteritems(partition_offsets):
            new_path = "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                groupid=consumer_group,
                topic=topic,
                partition=partition,
            )
            try:
                zk.create(new_path, value=offset, makepath=True)
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
    for topic, partitions in six.iteritems(topics):
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
        answer = input(in_str + ' ')
        if answer == "n" or answer == "no":
            sys.exit(0)
        if answer == "y" or answer == "yes":
            return


def get_offset_topic_partition_count(kafka_config):
    """Given a kafka cluster configuration, return the number of partitions
    in the offset topic. It will raise an UnknownTopic exception if the topic
    cannot be found."""
    metadata = get_topic_partition_metadata(kafka_config.broker_list)
    if CONSUMER_OFFSET_TOPIC not in metadata:
        raise UnknownTopic("Consumer offset topic is missing.")
    return len(metadata[CONSUMER_OFFSET_TOPIC])


# The mapping of group information to partition for the __consumer_offsets
# topic is determinated by a hash of the group id, modulo the number of
# partitions in the topic.
# Kafka code (from main/scala/kafka/coordinator/GroupMetadataManager.scala):
#   def partitionFor(groupId: String): Int =
#       Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
# hashCode returns the hash of the string according to the Java default string
# hashing algorithm. The algorithm is implemented in the inner function
# java_string_hashcode.
def get_group_partition(group, partition_count):
    """Given a group name, return the partition number of the consumer offset
    topic containing the data associated to that group."""
    def java_string_hashcode(s):
        h = 0
        for c in s:
            h = (31 * h + ord(c)) & 0xFFFFFFFF
        return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
    return abs(java_string_hashcode(group)) % partition_count


class InvalidMessageException(Exception):
    pass


class KafkaGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.kafka_config = kafka_config
        self.kafka_groups = defaultdict(set)
        self.active_partitions = {}
        self._finished = False

    def read_group(self, group_id):
        partition_count = get_offset_topic_partition_count(self.kafka_config)
        partition = get_group_partition(group_id, partition_count)
        return self.read_groups(partition).get(group_id, [])

    def read_groups(self, partition=None):
        self.consumer = KafkaConsumer(
            group_id='offset_monitoring_consumer',
            bootstrap_servers=self.kafka_config.broker_list,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=30000,
            fetch_max_wait_ms=2000,
            max_partition_fetch_bytes=10 * 1024 * 1024,  # 10MB
        )

        if partition is not None:
            self.active_partitions = {
                partition: TopicPartition(CONSUMER_OFFSET_TOPIC, partition),
            }
        else:
            self.active_partitions = {
                p: TopicPartition(CONSUMER_OFFSET_TOPIC, p)
                for p in self.consumer.partitions_for_topic(CONSUMER_OFFSET_TOPIC)
            }
        self.watermarks = self.get_current_watermarks(list(self.active_partitions.values()))
        # Active partitions are not empty. Remove the empty ones.
        self.active_partitions = {
            p: tp for p, tp in self.active_partitions.items()
            if tp.partition in self.watermarks and
            self.watermarks[tp.partition].highmark > 0 and
            self.watermarks[tp.partition].highmark > self.watermarks[tp.partition].lowmark
        }
        # Cannot consume if there are no active partitions
        if not self.active_partitions:
            return {}

        self.consumer.assign(list(self.active_partitions.values()))
        self.log.info("Consuming from %s", self.active_partitions)

        message_iterator = iter(self.consumer)

        while not self.finished():
            try:
                message = next(message_iterator)
            except StopIteration:
                continue
            # Stop when reaching the last message written to the
            # __consumer_offsets topic when KafkaGroupReader first started
            if message.offset >= self.watermarks[message.partition].highmark - 1:
                self.remove_partition_from_consumer(message.partition)
            self.process_consumer_offset_message(message)

        return {
            group: topics
            for group, topics in self.kafka_groups.items()
            if topics
        }

    def remove_partition_from_consumer(self, partition):
        deleted = self.active_partitions.pop(partition)
        # Terminate if there are no more partitions to consume
        if not self.active_partitions:
            self.log.info("Completed reading from all partitions")
            self._finished = True
            return
        # Reassign the remaining partitions to the consumer while saving the
        # position
        positions = [
            (p, self.consumer.position(p))
            for p in self.active_partitions.values()
        ]
        self.consumer.assign(list(self.active_partitions.values()))
        for topic_partition, position in positions:
            self.consumer.seek(topic_partition, position)
        self.log.info(
            "Completed reading from %s. Remaining partitions: %s",
            deleted,
            self.active_partitions,
        )

    def parse_consumer_offset_message(self, message):
        key = message.key
        ((key_schema,), cur) = relative_unpack(b'>h', key, 0)
        if key_schema not in [0, 1]:
            raise InvalidMessageException()   # This is not an offset commit message
        (group, cur) = read_short_string(key, cur)
        (topic, cur) = read_short_string(key, cur)
        ((partition,), cur) = relative_unpack(b'>l', key, cur)
        if message.value:
            value = message.value
            ((value_schema,), cur) = relative_unpack(b'>h', value, 0)
            if value_schema not in [0, 1]:
                raise InvalidMessageException()  # Unrecognized message value
            ((offset,), cur) = relative_unpack(b'>q', value, cur)
        else:
            offset = None  # Offset was deleted
        return group.decode(), topic.decode(), partition, offset

    def process_consumer_offset_message(self, message):
        try:
            group, topic, partition, offset = self.parse_consumer_offset_message(message)
        except InvalidMessageException:
            return

        if offset and (group not in self.kafka_groups or
                       topic not in self.kafka_groups[group]):
            self.kafka_groups[group].add(topic)
            self.log.info("Added group %s topic %s to list of groups", group, topic)
        elif not offset and group in self.kafka_groups and \
                topic in self.kafka_groups[group]:  # No offset means topic deletion
            self.kafka_groups[group].discard(topic)
            self.log.info("Removed group %s topic %s from list of groups", group, topic)

    def get_current_watermarks(self, partitions=None):
        client = KafkaToolClient(self.kafka_config.broker_list)
        client.load_metadata_for_topics(CONSUMER_OFFSET_TOPIC)
        offsets = get_topics_watermarks(
            client,
            [CONSUMER_OFFSET_TOPIC],
        )
        partitions_set = set(tp.partition for tp in partitions) if partitions else None
        return {part: offset for part, offset
                in six.iteritems(offsets[CONSUMER_OFFSET_TOPIC])
                if offset.highmark > offset.lowmark and
                (partitions is None or part in partitions_set)}

    def finished(self):
        return self._finished
