import logging
import operator
from collections import namedtuple

from kafka.common import KafkaUnavailableError
from kafka.common import NotCoordinatorForConsumerCode

from yelp_kafka_tool.util.offsets import get_current_consumer_offsets
from yelp_kafka_tool.util.offsets import get_topics_watermarks


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

    :param kafka_client: KafkaClient instance
    :param group: group id
    :param topics: list of topics
    :param raise_on_error: if False the method ignores missing topics and
      missing partitions. It still may fail on the request send.
    :param offset_storage: String, one of {zookeeper, kafka}.
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    """

    # Refresh client metadata. We do now use the topic list, because we
    # don't want to accidentally create the topic if it does not exist.
    # If Kafka is unavailable, let's retry loading client metadata (YELPKAFKA-30)
    try:
        kafka_client.load_metadata_for_topics()
    except KafkaUnavailableError:
        kafka_client.load_metadata_for_topics()

    group_offsets = get_current_consumer_offsets(
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


def get_higher_consumer_offsets_metadata(
    kafka_client,
    group,
    topics,
):
    """
    Attempts to fetch offsets metadata from both Zookeeper and Kafka.
    For each partition, returns offset metadata for whichever one has
    higher offsets.

    :param kafka_client: KafkaClient instance
    :param group: group id
    :param topics: list of topics
    :returns: dict <topic>: [ConsumerPartitionOffsets]
    """
    zk_offsets = get_consumer_offsets_metadata(
        kafka_client,
        group,
        topics,
        raise_on_error=False,
        offset_storage='zookeeper',
    )
    try:
        kafka_offsets = get_consumer_offsets_metadata(
            kafka_client,
            group,
            topics,
            raise_on_error=False,
            offset_storage='kafka',
        )
    except NotCoordinatorForConsumerCode:
        kafka_offsets = {}
    return merge_offsets_metadata(topics, zk_offsets, kafka_offsets)


def merge(d1, d2, merge_fn=lambda x, y: y):
    """
    Merges two dictionaries, non-destructively, combining
    values on duplicate keys as defined by the optional merge
    function.
    """
    result = dict(d1)
    for k, v in d2.iteritems():
        if k in result:
            result[k] = merge_fn(result[k], v)
        else:
            result[k] = v
    return result


def merge_offsets_metadata(topics, first, second):
    def to_dict(lst):
        return {item.partition: item for item in lst}

    def from_dict(dct):
        return dct.values()

    result = dict()
    for topic in topics:
        result[topic] = from_dict(
            merge(
                to_dict(first[topic]),
                to_dict(second[topic]),
                lambda x, y: max(x, y, key=operator.attrgetter('highmark'))
            )
        )
    return result
