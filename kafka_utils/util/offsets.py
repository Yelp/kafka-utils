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

from collections import defaultdict
from typing import cast
from typing import Collection
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import TypeVar
from typing import Union

from kafka.common import UnknownTopicOrPartitionError
from kafka.errors import BrokerResponseError
from kafka.errors import check_error
from kafka.structs import OffsetCommitRequestPayload
from kafka.structs import OffsetCommitResponsePayload
from kafka.structs import OffsetFetchRequestPayload
from kafka.structs import OffsetFetchResponsePayload
from kafka.structs import OffsetRequestPayload
from kafka.structs import OffsetResponsePayload
from typing_extensions import Protocol
from typing_extensions import TypedDict

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.error import OffsetCommitError
from kafka_utils.util.error import UnknownPartitions
from kafka_utils.util.error import UnknownTopic


TopicsCollection = Union[List[str], Mapping[str, Collection[int]]]


class PartitionOffsets(NamedTuple):
    r"""Tuple representing the offsets for a topic partition.

    * **topic**\(``str``): Name of the topic
    * **partition**\(``int``): Partition number
    * **highmark**\(``int``): high watermark
    * **lowmark**\(``int``): low watermark
    """
    topic: str
    partition: int
    highmark: int
    lowmark: int


HIGH_WATERMARK = "high"
LOW_WATERMARK = "low"


def pluck_topic_offset_or_zero_on_unknown(resp: OffsetFetchResponsePayload) -> OffsetFetchResponsePayload:
    try:
        check_error(resp)
    except UnknownTopicOrPartitionError:
        # If the server doesn't have any commited offsets by this group for
        # this topic, assume it's zero.
        pass
    # The API spec says server wont set an error, but 0.8.1.1 does. The actual
    # check is if the offset is -1.
    if resp.offset == -1:
        return OffsetFetchResponsePayload(
            resp.topic,
            resp.partition,
            0,
            resp.metadata,
            0,
        )
    return resp


def _check_fetch_response_error(resp: OffsetResponsePayload) -> OffsetResponsePayload:
    try:
        check_error(resp)
    except BrokerResponseError:
        # In case of error we set the offset to (-1,)
        return OffsetResponsePayload(
            resp.topic,
            resp.partition,
            resp.error,
            (-1,),
        )
    return resp


class Response(Protocol):
    topic: str
    partition: int
    error: int


ResponseT = TypeVar('ResponseT', bound=Response)


def _check_commit_response_error(resp: ResponseT) -> ResponseT | OffsetCommitError:
    try:
        check_error(resp)
    except BrokerResponseError as e:
        exception = OffsetCommitError(
            resp.topic,
            resp.partition,
            e.message
        )
        return exception
    return resp


def _validate_topics_list_or_dict(topics: TopicsCollection) -> Mapping[str, Collection[int]]:
    if isinstance(topics, Mapping):
        return topics
    elif isinstance(topics, (list, set, tuple)):
        return {topic: [] for topic in topics}
    else:
        raise TypeError("Invalid topics: {topics}. It must be either a "
                        "list of topics or a dict "
                        "topic: [partitions]".format(topics=topics))


def _verify_topics_and_partitions(kafka_client: KafkaToolClient, topics: TopicsCollection, raise_on_error: bool) -> Mapping[str, Collection[int]]:
    topics = _validate_topics_list_or_dict(topics)
    valid_topics = {}
    partitions: Collection[int]
    for topic, partitions in topics.items():
        # Check topic exists
        if not kafka_client.has_metadata_for_topic(topic):
            if raise_on_error:
                raise UnknownTopic("Topic {topic!r} does not exist in "
                                   "kafka".format(topic=topic))
            else:
                continue
        if partitions:
            # Check the partitions really exist
            unknown_partitions = set(partitions) - \
                set(kafka_client.get_partition_ids_for_topic(topic))
            if unknown_partitions:
                if raise_on_error:
                    raise UnknownPartitions(
                        "Partitions {partitions!r} for topic {topic!r} do not"
                        "exist in kafka".format(
                            partitions=unknown_partitions,
                            topic=topic,
                        )
                    )
                else:
                    # We only use the available partitions in this case
                    partitions = set(partitions) - unknown_partitions
        else:
            # Default get all partitions metadata
            partitions = kafka_client.get_partition_ids_for_topic(topic)
        valid_topics[topic] = partitions
    return valid_topics


def _verify_commit_offsets_requests(kafka_client: KafkaToolClient, new_offsets: dict[str, dict[int, int]], raise_on_error: bool) -> dict[str, dict[int, int]]:
    type_error_str = (
        "Invalid new_offsets: {new_offsets}. It must be a "
        "dict of the format: "
        "{{<topic>: {{<partition>: <offset>}}}}"
    ).format(new_offsets=new_offsets)

    if not isinstance(new_offsets, dict):
        raise TypeError(type_error_str)

    for topic, partitions in new_offsets.items():
        if not isinstance(partitions, dict):
            raise TypeError(type_error_str)

    topics = {
        topic: list(partitions.keys())
        for topic, partitions in new_offsets.items()
    }

    valid_topics = _verify_topics_and_partitions(kafka_client, topics, raise_on_error)

    return {
        topic: {
            partition: new_offsets[topic][partition]
            for partition in partitions
        }
        for topic, partitions in valid_topics.items()
        if partitions
    }


def get_current_consumer_offsets(
    kafka_client: KafkaToolClient,
    group: str,
    topics: TopicsCollection,
    raise_on_error: bool = True,
) -> dict[str, dict[int, int]]:
    """ Get current consumer offsets.

    NOTE: This method does not refresh client metadata. It is up to the caller
    to avoid using stale metadata.

    If any partition leader is not available, the request fails for all the
    other topics. This is the tradeoff of sending all topic requests in batch
    and save both in performance and Kafka load.

    :param kafka_client: a connected KafkaToolClient
    :param group: kafka group_id
    :param topics: topic list or dict {<topic>: [partitions]}
    :param raise_on_error: if False the method ignores missing topics and
      missing partitions. It still may fail on the request send.
    :returns: a dict topic: partition: offset
    :raises:
      :py:class:`kafka_utils.util.error.UnknownTopic`: upon missing
      topics and raise_on_error=True

      :py:class:`kafka_utils.util.error.UnknownPartition`: upon missing
      partitions and raise_on_error=True

      FailedPayloadsError: upon send request error.
    """

    topics_dict = _verify_topics_and_partitions(kafka_client, topics, raise_on_error)

    group_offset_reqs = [
        OffsetFetchRequestPayload(topic, partition)
        for topic, partitions in topics_dict.items()
        for partition in partitions
    ]

    group_offsets: dict[str, dict[int, int]] = {}

    send_api = kafka_client.send_offset_fetch_request_kafka

    if group_offset_reqs:
        # fail_on_error = False does not prevent network errors
        group_resps = send_api(
            group=group,
            payloads=group_offset_reqs,
            fail_on_error=False,
            callback=pluck_topic_offset_or_zero_on_unknown,
        )
        for resp in group_resps:
            group_offsets.setdefault(
                resp.topic,
                {},
            )[resp.partition] = resp.offset

    return group_offsets


class WatermarksDict(TypedDict, total=False):
    lowmark: int
    highmark: int


def get_topics_watermarks(kafka_client: KafkaToolClient, topics: TopicsCollection, raise_on_error: bool = True) -> dict[str, dict[int, PartitionOffsets]]:
    """ Get current topic watermarks.

    NOTE: This method does not refresh client metadata. It is up to the caller
    to use avoid using stale metadata.

    If any partition leader is not available, the request fails for all the
    other topics. This is the tradeoff of sending all topic requests in batch
    and save both in performance and Kafka load.

    :param kafka_client: a connected KafkaToolClient
    :param topics: topic list or dict {<topic>: [partitions]}
    :param raise_on_error: if False the method ignores missing topics
      and missing partitions. It still may fail on the request send.
    :returns: a dict topic: partition: Part
    :raises:
      :py:class:`~kafka_utils.util.error.UnknownTopic`: upon missing
      topics and raise_on_error=True

      :py:class:`~kafka_utils.util.error.UnknownPartition`: upon missing
      partitions and raise_on_error=True

      FailedPayloadsError: upon send request error.
    """
    topics_dict = _verify_topics_and_partitions(
        kafka_client,
        topics,
        raise_on_error,
    )
    highmark_offset_reqs = []
    lowmark_offset_reqs = []

    for topic, partitions in topics_dict.items():
        # Batch watermark requests
        for partition in partitions:
            # Request the the latest offset
            highmark_offset_reqs.append(
                OffsetRequestPayload(
                    topic, partition, -1, max_offsets=1
                )
            )
            # Request the earliest offset
            lowmark_offset_reqs.append(
                OffsetRequestPayload(
                    topic, partition, -2, max_offsets=1
                )
            )

    watermark_offsets: dict[str, dict[int, PartitionOffsets]] = {}

    if not (len(highmark_offset_reqs) + len(lowmark_offset_reqs)):
        return watermark_offsets

    # fail_on_error = False does not prevent network errors
    highmark_resps = kafka_client.send_offset_request(
        highmark_offset_reqs,
        fail_on_error=False,
        callback=_check_fetch_response_error,
    )
    lowmark_resps = kafka_client.send_offset_request(
        lowmark_offset_reqs,
        fail_on_error=False,
        callback=_check_fetch_response_error,
    )

    # At this point highmark and lowmark should ideally have the same length.
    assert len(highmark_resps) == len(lowmark_resps)
    aggregated_offsets: dict[str, dict[int, WatermarksDict]] = defaultdict(lambda: defaultdict(lambda: cast(WatermarksDict, {})))
    for resp in highmark_resps:
        aggregated_offsets[resp.topic][resp.partition]['highmark'] = resp.offsets[0]
    for resp in lowmark_resps:
        aggregated_offsets[resp.topic][resp.partition]['lowmark'] = resp.offsets[0]

    for topic, partition_watermarks in aggregated_offsets.items():
        for partition, watermarks in partition_watermarks.items():
            watermark_offsets.setdefault(
                topic,
                {},
            )[partition] = PartitionOffsets(
                topic,
                partition,
                watermarks['highmark'],
                watermarks['lowmark'],
            )
    return watermark_offsets


def _commit_offsets_to_watermark(
    kafka_client: KafkaToolClient,
    group: str,
    topics: TopicsCollection,
    watermark: str,
    raise_on_error: bool,
) -> list[OffsetCommitResponsePayload | OffsetCommitError]:
    topics_dict = _verify_topics_and_partitions(kafka_client, topics, raise_on_error)

    watermark_offsets = get_topics_watermarks(kafka_client, topics_dict, raise_on_error)

    if watermark == HIGH_WATERMARK:
        group_offset_reqs = [
            OffsetCommitRequestPayload(
                topic, partition,
                watermark_offsets[topic][partition].highmark,
                metadata=''
            )
            for topic, partitions in topics_dict.items()
            for partition in partitions
        ]
    elif watermark == LOW_WATERMARK:
        group_offset_reqs = [
            OffsetCommitRequestPayload(
                topic, partition,
                watermark_offsets[topic][partition].lowmark,
                metadata=''
            )
            for topic, partitions in topics_dict.items()
            for partition in partitions
        ]
    else:
        raise ValueError(
            f"Unknown watermark: {watermark}"
        )

    send_api = kafka_client.send_offset_commit_request_kafka

    status = []
    if group_offset_reqs:
        status = send_api(
            group,
            group_offset_reqs,
            raise_on_error,
            callback=_check_commit_response_error
        )

    return [_f for _f in status if _f]


def advance_consumer_offsets(
    kafka_client: KafkaToolClient,
    group: str,
    topics: TopicsCollection,
    raise_on_error: bool = True,
) -> list[OffsetCommitResponsePayload | OffsetCommitError]:
    """Advance consumer offsets to the latest message in the topic
    partition (the high watermark).

    This method shall refresh the client metadata prior to updating
    the offsets.

    If any partition leader is not available, the request fails for all the
    other topics. This is the tradeoff of sending all topic requests in batch
    and save both in performance and Kafka load.

    :param kafka_client: a connected KafkaToolClient
    :param group: kafka group_id
    :param topics: topic list or dict {<topic>: [partitions]}
    :param raise_on_error: if False the method does not raise exceptions
      on missing topics/partitions. It may still fail on the request send.
    :returns: a list of errors for each partition offset update that failed.
    :rtype: list [OffsetCommitError]
    :raises:
      :py:class:`kafka_utils.util.error.UnknownTopic`: upon missing
      topics and raise_on_error=True

      :py:class:`kafka_utils.util.error.UnknownPartition`: upon missing
      partitions and raise_on_error=True

      FailedPayloadsError: upon send request error.
    """
    kafka_client.load_metadata_for_topics()

    return _commit_offsets_to_watermark(
        kafka_client, group, topics,
        HIGH_WATERMARK, raise_on_error,
    )


def rewind_consumer_offsets(
    kafka_client: KafkaToolClient,
    group: str,
    topics: TopicsCollection,
    raise_on_error: bool = True,
) -> list[OffsetCommitResponsePayload | OffsetCommitError]:
    """Rewind consumer offsets to the earliest message in the topic
    partition (the low watermark).

    This method shall refresh the client metadata prior to updating
    the offsets.

    If any partition leader is not available, the request fails for all the
    other topics. This is the tradeoff of sending all topic requests in batch
    and save both in performance and Kafka load.

    :param kafka_client: a connected KafkaToolClient
    :param group: kafka group_id
    :param topics: topic list or dict {<topic>: [partitions]}
    :param raise_on_error: if False the method does not raise exceptions
      on missing topics/partitions. It may still fail on the request send.
    :returns: a list of errors for each partition offset update that failed.
    :rtype: list [OffsetCommitError]
    :raises:
      :py:class:`kafka_utils.util.error.UnknownTopic`: upon missing
      topics and raise_on_error=True

      :py:class:`kafka_utils.util.error.UnknownPartition`: upon missing
      partitions and raise_on_error=True

      FailedPayloadsError: upon send request error.
    """
    kafka_client.load_metadata_for_topics()

    return _commit_offsets_to_watermark(
        kafka_client, group, topics,
        LOW_WATERMARK, raise_on_error,
    )


def set_consumer_offsets(
    kafka_client: KafkaToolClient,
    group: str,
    new_offsets: dict[str, dict[int, int]],
    raise_on_error: bool = True,
) -> list[OffsetCommitResponsePayload | OffsetCommitError]:
    """Set consumer offsets to the specified offsets.

    This method does not validate the specified offsets, it is up to
    the caller to specify valid offsets within a topic partition.

    If any partition leader is not available, the request fails for all the
    other topics. This is the tradeoff of sending all topic requests in batch
    and save both in performance and Kafka load.

    :param kafka_client: a connected KafkaToolClient
    :param group: kafka group_id
    :param topics: dict {<topic>: {<partition>: <offset>}}
    :param raise_on_error: if False the method does not raise exceptions
      on errors encountered. It may still fail on the request send.
    :returns: a list of errors for each partition offset update that failed.
    :rtype: list [OffsetCommitError]
    :raises:
      :py:class:`kafka_utils.util.error.UnknownTopic`: upon missing
      topics and raise_on_error=True

      :py:class:`kafka_utils.util.error.UnknownPartition`: upon missing
      partitions and raise_on_error=True

      :py:class:`exceptions.TypeError`: upon badly formatted input
      new_offsets

      FailedPayloadsError: upon send request error.
    """
    valid_new_offsets = _verify_commit_offsets_requests(
        kafka_client,
        new_offsets,
        raise_on_error
    )

    group_offset_reqs = [
        OffsetCommitRequestPayload(
            topic,
            partition,
            offset,
            metadata='',
        )
        for topic, new_partition_offsets in valid_new_offsets.items()
        for partition, offset in new_partition_offsets.items()
    ]

    send_api = kafka_client.send_offset_commit_request_kafka

    status = []
    if group_offset_reqs:
        status = send_api(
            group,
            group_offset_reqs,
            raise_on_error,
            callback=_check_commit_response_error
        )

    return [_f for _f in status
            if _f and _f.error != 0]


def _nullify_partition_offsets(partition_offsets: dict[int, int]) -> dict[int, int]:
    result = {}
    for partition in partition_offsets:
        result[partition] = 0
    return result


def nullify_offsets(offsets: dict[str, dict[int, int]]) -> dict[str, dict[int, int]]:
    """Modify offsets metadata so that the partition offsets
    have null payloads.

    :param offsets: dict {<topic>: {<partition>: <offset>}}
    :returns: a dict topic: partition: offset
    """
    result = {}
    for topic, partition_offsets in offsets.items():
        result[topic] = _nullify_partition_offsets(partition_offsets)
    return result
