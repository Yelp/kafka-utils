import struct

from kafka.protocol import KafkaProtocol
from kafka.util import group_by_topic_and_partition
from kafka.util import write_short_string


class YelpKafkaProtocol(KafkaProtocol):

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
