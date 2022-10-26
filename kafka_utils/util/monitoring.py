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
import re
from typing import NamedTuple

from kafka.common import KafkaUnavailableError

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import get_topics_watermarks
from kafka_utils.util.offsets import PartitionOffsets
from kafka_utils.util.offsets import TopicsCollection


log = logging.getLogger(__name__)


class ConsumerPartitionOffsets(NamedTuple):
    r"""Tuple representing the consumer offsets for a topic partition.

    * **topic**\(``str``): Name of the topic
    * **partition**\(``int``): Partition number
    * **current**\(``int``): current group offset
    * **highmark**\(``int``): high watermark
    * **lowmark**\(``int``): low watermark
    """
    topic: str
    partition: int
    current: int
    highmark: int
    lowmark: int


def get_consumer_offsets_metadata(
    kafka_client: KafkaToolClient,
    group: str,
    topics: TopicsCollection,
    raise_on_error: bool = True,
) -> dict[str, list[ConsumerPartitionOffsets]]:
    """This method:
        * refreshes metadata for the kafka client
        * fetches group offsets
        * fetches watermarks

    :param kafka_client: KafkaToolClient instance
    :param group: group id
    :param topics: list of topics
    :param raise_on_error: if False the method ignores missing topics and
      missing partitions. It still may fail on the request send.
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    """
    # Refresh client metadata. We do not use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    group_offsets = get_current_consumer_offsets(
        kafka_client, group, topics, raise_on_error
    )

    watermarks = get_topics_watermarks(
        kafka_client, topics, raise_on_error
    )

    result = {}
    for topic, partitions in group_offsets.items():
        result[topic] = [
            ConsumerPartitionOffsets(
                topic=topic,
                partition=partition,
                current=group_offsets[topic][partition],
                highmark=watermarks[topic][partition].highmark,
                lowmark=watermarks[topic][partition].lowmark,
            ) for partition in partitions
        ]
    return result


def get_watermark_for_regex(
    kafka_client: KafkaToolClient,
    topic_regex: str,
) -> dict[str, dict[int, PartitionOffsets]]:
    """This method:
        * refreshes metadata for the kafka client
        * fetches watermarks

    :param kafka_client: KafkaToolClient instance
    :param topic: the topic regex
    :returns: {topic: {partition_id: PartitionOffsets}}
    """
    # Refresh client metadata. We do not use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    topics_to_be_considered = []

    for topic in kafka_client.topic_partitions:
        if re.search(topic_regex, topic):
            topics_to_be_considered.append(topic)

    watermarks = get_topics_watermarks(
        kafka_client, topics_to_be_considered
    )
    return watermarks


def get_watermark_for_topic(
    kafka_client: KafkaToolClient,
    topic: str,
) -> dict[str, dict[int, PartitionOffsets]]:
    """This method:
        * refreshes metadata for the kafka client
        * fetches watermarks

    :param kafka_client: KafkaToolClient instance
    :param topic: the topic
    :returns: {topic: {partition_id: PartitionOffsets}}
    """
    # Refresh client metadata. We do not use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    watermarks = get_topics_watermarks(
        kafka_client, [topic]
    )
    return watermarks


def merge_offsets_metadata(topics: TopicsCollection, *offsets_responses: dict[str, dict[int, int]]) -> dict[str, dict[int, int]]:
    """Merge the offset metadata dictionaries from multiple responses.

    :param topics: list of topics
    :param offsets_responses: list of dict topic: partition: offset
    :returns: dict topic: partition: offset
    """
    result = dict()
    for topic in topics:
        partition_offsets = [
            response[topic]
            for response in offsets_responses
            if topic in response
        ]
        result[topic] = merge_partition_offsets(*partition_offsets)
    return result


def merge_partition_offsets(*partition_offsets: dict[int, int]) -> dict[int, int]:
    """Merge the partition offsets of a single topic from multiple responses.

    :param partition_offsets: list of dict partition: offset
    :returns: dict partition: offset
    """
    output: dict[int, int] = dict()
    for partition_offset in partition_offsets:
        for partition, offset in partition_offset.items():
            prev_offset = output.get(partition, 0)
            output[partition] = max(prev_offset, offset)
    return output
