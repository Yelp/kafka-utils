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
import struct

from kafka.protocol import KafkaProtocol
from kafka.util import group_by_topic_and_partition
from kafka.util import write_short_string


class KafkaToolProtocol(KafkaProtocol):

    @classmethod
    def encode_offset_commit_request_kafka(cls, client_id, correlation_id,
                                           group, payloads):
        """
        Encode some OffsetCommitRequest structs
        Arguments:
            client_id: string
            correlation_id: int
            group: string, the consumer group you are committing offsets for
            payloads: list of OffsetCommitRequest
        """
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(
            client_id, correlation_id,
            KafkaProtocol.OFFSET_COMMIT_KEY,
            version=2))
        message.append(write_short_string(group))
        message.append(struct.pack('>i', -1))   # ConsumerGroupGenerationId
        message.append(write_short_string(''))  # ConsumerId
        message.append(struct.pack('>q', -1))   # Retention time
        message.append(struct.pack('>i', len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(write_short_string(topic))
            message.append(struct.pack('>i', len(topic_payloads)))

            for partition, payload in topic_payloads.items():
                message.append(struct.pack('>iq', partition, payload.offset))
                message.append(write_short_string(payload.metadata))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)
