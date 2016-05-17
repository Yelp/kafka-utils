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
import logging
from collections import namedtuple

from kafka.common import ConsumerCoordinatorNotAvailableCode
from kafka.common import KafkaUnavailableError

from kafka_utils.util.error import InvalidOffsetStorageError
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import get_topics_watermarks


log = logging.getLogger(__name__)

ConsumerPartitionOffsets = namedtuple(
    'ConsumerPartitionOffsets',
    ['topic', 'partition', 'current', 'highmark', 'lowmark']
)
"""Tuple representing the consumer offsets for a topic partition.

* **topic**\(``str``): Name of the topic
* **partition**\(``int``): Partition number
* **current**\(``int``): current group offset
* **highmark**\(``int``): high watermark
* **lowmark**\(``int``): low watermark
"""


def get_consumer_offsets_metadata(
    kafka_client,
    group,
    topics,
    raise_on_error=True,
    offset_storage='zookeeper',
):
    """This method:
        * refreshes metadata for the kafka client
        * fetches group offsets
        * fetches watermarks

    :param kafka_client: KafkaToolClient instance
    :param group: group id
    :param topics: list of topics
    :param raise_on_error: if False the method ignores missing topics and
      missing partitions. It still may fail on the request send.
    :param offset_storage: String, one of {zookeeper, kafka, dual}.
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    :raises:
      :py:class:`kafka_utils.util.error.InvalidOffsetStorageError: upon unknown
      offset_storage choice.
    """
    # Refresh client metadata. We do now use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    group_offsets = get_current_offsets(
        kafka_client, group, topics, raise_on_error, offset_storage
    )

    watermarks = get_topics_watermarks(
        kafka_client, topics, raise_on_error
    )

    result = {}
    for topic, partitions in group_offsets.iteritems():
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


def get_current_offsets(
        kafka_client,
        group,
        topics,
        raise_on_error,
        offset_storage,
):
    """Get the current consumer offsets from either Zookeeper or Kafka
    or the combination of both.
    """
    if offset_storage in ['zookeeper', 'kafka']:
        return get_current_consumer_offsets(
            kafka_client, group, topics, raise_on_error, offset_storage
        )
    elif offset_storage == 'dual':
        return _get_current_offsets_dual(
            kafka_client, group, topics, raise_on_error,
        )
    else:
        raise InvalidOffsetStorageError(offset_storage)


def _get_current_offsets_dual(
    kafka_client,
    group,
    topics,
    raise_on_error,
):
    """Get current consumer offsets from Zookeeper and from Kafka
    and return the higher partition offsets from the responses.
    """
    zk_offsets = get_current_consumer_offsets(
        kafka_client, group, topics, False, 'zookeeper',
    )
    try:
        kafka_offsets = get_current_consumer_offsets(
            kafka_client, group, topics, False, 'kafka',
        )
    except ConsumerCoordinatorNotAvailableCode:
        kafka_offsets = {}
    return merge_offsets_metadata(topics, zk_offsets, kafka_offsets)


def merge_offsets_metadata(topics, *offsets_responses):
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


def merge_partition_offsets(*partition_offsets):
    """Merge the partition offsets of a single topic from multiple responses.

    :param partition_offsets: list of dict partition: offset
    :returns: dict partition: offset
    """
    output = dict()
    for partition_offset in partition_offsets:
        for partition, offset in partition_offset.iteritems():
            prev_offset = output.get(partition, None)
            output[partition] = max(prev_offset, offset)
    return output
