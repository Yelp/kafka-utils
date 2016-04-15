import logging
from collections import namedtuple

from kafka.common import KafkaUnavailableError

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


def topics_offset_distance(
    kafka_client,
    group,
    topics,
    offset_storage='zookeeper',
):
    """Get the distance a group_id is from the current latest offset
    for topics.

    If the group is unkown to kafka it's assumed to be an offset 0. All other
    errors will not be caught.

    This method force the client to use fresh metadata by calling
    kafka_client.load_metadata_for_topics(topics) before getting
    the group offsets.

    :param kafka_client: KafkaClient instance
    :param group: consumer group id
    :param topics: topics list or dict <topic>: <[partitions]>
    :param offset_storage: String, one of {zookeeper, kafka}.
    :returns: dict <topic>: {<partition>: <distance>}
    """

    distance = {}
    for topic, offsets in get_consumer_offsets_metadata(
        kafka_client,
        group,
        topics,
        offset_storage,
    ).iteritems():
        distance[topic] = dict([
            (offset.partition, offset.highmark - offset.current)
            for offset in offsets
        ])
    return distance


def offset_distance(
    kafka_client,
    group,
    topic,
    partitions=None,
    offset_storage='zookeeper',
):
    """Get the distance a group_id is from the current latest in a topic.

    If the group is unknown to kafka, it's assumed to be on offset 0. All other
    errors will not be caught. Be prepared for KafkaUnavailableError and its
    ilk.

    This method force the client to use fresh metadata by calling
    kafka_client.load_metadata_for_topics(topics) before getting
    the group offsets.

    :param kafka_client: KafkaClient instance
    :param group: consumer group id
    :param topic: topic name
    :partitions: partitions list
    :param offset_storage: String, one of {zookeeper, kafka}.
    :returns: dict <partition>: <distance>
    """

    if partitions:
        topics = {topic: partitions}
    else:
        topics = [topic]
    consumer_offsets = get_consumer_offsets_metadata(
        kafka_client,
        group,
        topics,
        offset_storage,
    )
    return dict(
        [(offset.partition, offset.highmark - offset.current)
         for offset in consumer_offsets[topic]]
    )
