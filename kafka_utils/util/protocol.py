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

import kafka.protocol.commit
from kafka.protocol import KafkaProtocol
from kafka.protocol.api import Response
from kafka.protocol.commit import GroupCoordinatorRequest_v0
from kafka.protocol.commit import OffsetCommitRequest_v2
from kafka.structs import ConsumerMetadataResponse
from kafka.structs import OffsetCommitRequestPayload
from kafka.util import group_by_topic_and_partition


class KafkaToolProtocol(KafkaProtocol):

    @classmethod
    def encode_offset_commit_request_kafka(cls, group: str, payloads: list[OffsetCommitRequestPayload]) -> OffsetCommitRequest_v2:
        """
        Encode an OffsetCommitRequest struct
        Arguments:
            group: string, the consumer group you are committing offsets for
            payloads: list of OffsetCommitRequestPayload
        """
        return OffsetCommitRequest_v2(
            consumer_group=group,
            consumer_group_generation_id=kafka.protocol.commit.OffsetCommitRequest[2].DEFAULT_GENERATION_ID,
            consumer_id='',
            retention_time=kafka.protocol.commit.OffsetCommitRequest[2].DEFAULT_RETENTION_TIME,
            topics=[(
                topic,
                [(
                    partition,
                    payload.offset,
                    payload.metadata)
                    for partition, payload in topic_payloads.items()])
                for topic, topic_payloads in group_by_topic_and_partition(payloads).items()])

    @classmethod
    def encode_consumer_metadata_request(cls, payloads: str) -> GroupCoordinatorRequest_v0:
        """
        Encode a GroupCoordinatorRequest. Note that ConsumerMetadataRequest is
        renamed to GroupCoordinatorRequest in 0.9+. Interface is unchanged
        Arguments:
            payloads: string (consumer group)
        """
        return GroupCoordinatorRequest_v0(payloads)

    @classmethod
    def decode_consumer_metadata_response(cls, response: Response) -> ConsumerMetadataResponse:
        """
        Decode GroupCoordinatorResponse. Note that ConsumerMetadataResponse is
        renamed to GroupCoordinatorResponse in 0.9+
        Arguments:
            response: response to decode
        """
        return ConsumerMetadataResponse(
            response.error_code,
            response.coordinator_id,
            response.host,
            response.port,
        )
