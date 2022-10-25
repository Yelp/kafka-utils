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

import logging
import sys
from collections import defaultdict

from kafka.admin import KafkaAdminClient
from kafka.consumer import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition
from kafka.util import read_short_string
from kafka.util import relative_unpack
from kazoo.exceptions import NodeExistsError

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
    for topic, partition_offsets in offsets.items():
        for partition, offset in partition_offsets.items():
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
    for topic, partitions in topics.items():
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
    while True:
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


def topic_offsets_for_timestamp(consumer, timestamp, topics):
    """Given an initialized KafkaConsumer, timestamp, and list of topics,
    looks up the offsets for the given topics by timestamp. The returned
    offset for each partition is the earliest offset whose timestamp is greater than or
    equal to the given timestamp in the corresponding partition.

    Arguments:
        consumer (KafkaConsumer): an initialized kafka-python consumer
        timestamp (int): Unix epoch milliseconds. Unit should be milliseconds
            since beginning of the epoch (midnight Jan 1, 1970 (UTC))
        topics (list): List of topics whose offsets are to be fetched.
    :returns:
        ``{TopicPartition: OffsetAndTimestamp}``: mapping from partition
        to the timestamp and offset of the first message with timestamp
        greater than or equal to the target timestamp.
        Returns ``{TopicPartition: None}`` for specific topic-partiitons if:
          1. Timestamps are not supported in messages
          2. No offsets in the partition after the given timestamp
          3. No data in the topic-partition
    :raises:
        ValueError: If the target timestamp is negative
        UnsupportedVersionError: If the broker does not support looking
            up the offsets by timestamp.
        KafkaTimeoutError: If fetch failed in request_timeout_ms
    """
    tp_timestamps = {}
    for topic in topics:
        topic_partitions = consumer_partitions_for_topic(consumer, topic)
        for tp in topic_partitions:
            tp_timestamps[tp] = timestamp
    return consumer.offsets_for_times(tp_timestamps)


def consumer_partitions_for_topic(consumer, topic):
    """Returns a list of all TopicPartitions for a given topic.

    Arguments:
        consumer: an initialized KafkaConsumer
        topic: a topic name to fetch TopicPartitions for

    :returns:
        list(TopicPartition): A list of TopicPartitions that belong to the given topic
    """
    topic_partitions = []
    partitions = consumer.partitions_for_topic(topic)
    if partitions is not None:
        for partition in partitions:
            topic_partitions.append(TopicPartition(topic, partition))
    else:
        logging.error(
            f"No partitions found for topic {topic}. Maybe it doesn't exist?",
        )
    return topic_partitions


def consumer_commit_for_times(consumer, partition_to_offset, atomic=False):
    """Commits offsets to Kafka using the given KafkaConsumer and offsets, a mapping
    of TopicPartition to Unix Epoch milliseconds timestamps.

    Arguments:
        consumer (KafkaConsumer): an initialized kafka-python consumer.
        partitions_to_offset (dict TopicPartition: OffsetAndTimestamp): Map of TopicPartition to OffsetAndTimestamp. Return value of offsets_for_times.
        atomic (bool): Flag to specify whether the commit should fail if offsets are not found for some
            TopicPartition: timestamp pairs.
    """
    no_offsets = set()
    for tp, offset in partition_to_offset.items():
        if offset is None:
            logging.error(
                "No offsets found for topic-partition {tp}. Either timestamps not supported"
                " for the topic {tp}, or no offsets found after timestamp specified, or there is no"
                " data in the topic-partition.".format(tp=tp),
            )
            no_offsets.add(tp)
    if atomic and len(no_offsets) > 0:
        logging.error(
            "Commit aborted; offsets were not found for timestamps in"
            " topics {}".format(",".join([str(tp) for tp in no_offsets])),
        )
        return

    offsets_metadata = {
        tp: OffsetAndMetadata(partition_to_offset[tp].offset, metadata=None)
        for tp in partition_to_offset.keys() if tp not in no_offsets
    }

    if len(offsets_metadata) != 0:
        consumer.commit(offsets_metadata)


class InvalidMessageException(Exception):
    pass


class KafkaAdminGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_config.broker_list,
        )

    def read_group(self, groupid: int) -> list[str]:
        topics = set()
        group_offsets = self.admin_client.list_consumer_group_offsets(groupid)
        for tp in group_offsets.keys():
            topics.add(tp.topic)

        return list(topics)

    def read_groups(self, groupids: list[int] | None = None, list_only: bool = False) -> dict[int, list[str]]:
        if groupids is None:
            groupids = self._list_groups()

        if list_only:
            return {groupid: [] for groupid in groupids}

        groups = {}
        for groupid in groupids:
            topics = self.read_group(groupid)
            groups[groupid] = topics

        return groups

    def _list_groups(self) -> list[int]:
        groups_and_protocol_types = self.admin_client.list_consumer_groups()
        return [
            gpt[0]
            for gpt in groups_and_protocol_types
        ]


class KafkaGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.kafka_config = kafka_config
        self._kafka_groups = defaultdict(lambda: defaultdict(dict))
        self.active_partitions = {}
        self._finished = False

    def read_group(self, group_id):
        partition_count = get_offset_topic_partition_count(self.kafka_config)
        partition = get_group_partition(group_id, partition_count)
        return self.read_groups(partition)[group_id]

    def read_groups(self, partition=None, list_only: bool = False) -> dict[int, list[str]]:
        self.consumer = KafkaConsumer(
            group_id='offset_monitoring_consumer',
            bootstrap_servers=self.kafka_config.broker_list,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=30000,
            fetch_max_wait_ms=2000,
            max_partition_fetch_bytes=10 * 1024 * 1024,  # 10MB
        )

        # Fetch metadata as partitions_for_topic only returns locally cached metadata
        # See https://github.com/dpkp/kafka-python/issues/1742
        self.consumer.topics()

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

        self._remove_unsubscribed_topics()

        return {
            group: topics.keys()
            for group, topics in self._kafka_groups.items()
            if topics
        }

    def _remove_unsubscribed_topics(self):
        for group, topics in list(self._kafka_groups.items()):
            for topic, partitions in list(topics.items()):
                # If offsets for all partitions are 0, consider the topic as unsubscribed
                if not any(partitions.values()):
                    del self._kafka_groups[group][topic]
                    self.log.info(f"Removed group {group} topic {topic} from list of groups")

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

    def parse_consumer_offset_message(self, message) -> tuple[str, str, int, int]:
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
            if value_schema not in [0, 1, 3]:
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

        if offset is not None:
            self._kafka_groups[group][topic][partition] = offset
            self.log.info(
                "Updated group {group} topic {topic} and updated offset in list of groups".format(
                    group=group,
                    topic=topic,
                ),
            )
        # TODO: check if we can ever find an offset commit message with message.value is None
        elif offset is None and group in self._kafka_groups and \
                topic in self._kafka_groups[group]:  # No offset means topic deletion
            del self._kafka_groups[group][topic]
            self.log.info(f"Removed group {group} topic {topic} from list of groups")

    def get_current_watermarks(self, partitions=None):
        client = KafkaToolClient(self.kafka_config.broker_list)
        client.load_metadata_for_topics(CONSUMER_OFFSET_TOPIC)
        offsets = get_topics_watermarks(
            client,
            [CONSUMER_OFFSET_TOPIC],
        )
        partitions_set = {tp.partition for tp in partitions} if partitions else None
        return {part: offset for part, offset
                in offsets[CONSUMER_OFFSET_TOPIC].items()
                if offset.highmark > offset.lowmark and
                (partitions is None or part in partitions_set)}

    def finished(self):
        return self._finished


def get_kafka_group_reader(cluster_config, use_admin_client=False) -> KafkaGroupReader | KafkaAdminGroupReader:
    if use_admin_client:
        return KafkaAdminGroupReader(cluster_config)
    else:
        return KafkaGroupReader(cluster_config)
