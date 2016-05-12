import struct

from kafka.common import OffsetCommitRequest

from kafka_tools.util.protocol import KafkaToolProtocol


class TestProtocol(object):

    def test_encode_offset_commit_request_kafka(self):

        header = b"".join([
            struct.pack('>i', 113),              # Total message length

            struct.pack('>h', 8),                # Message type = offset commit
            struct.pack('>h', 2),                # API version
            struct.pack('>i', 42),               # Correlation ID
            struct.pack('>h9s', 9, b"client_id"),  # The client ID
            struct.pack('>h8s', 8, b"group_id"),  # The group to commit for
            struct.pack('>i', -1),               # Consumer group generation id
            struct.pack(">h0s", 0, b""),         # Consumer id
            struct.pack('>q', -1),               # Retention time
            struct.pack('>i', 2),                # Num topics
        ])

        topic1 = b"".join([
            struct.pack(">h6s", 6, b"topic1"),   # Topic for the request
            struct.pack(">i", 2),                # Two partitions
            struct.pack(">i", 0),                # Partition 0
            struct.pack(">q", 123),              # Offset 123
            struct.pack(">h", -1),               # Null metadata
            struct.pack(">i", 1),                # Partition 1
            struct.pack(">q", 234),              # Offset 234
            struct.pack(">h", -1),               # Null metadata
        ])

        topic2 = b"".join([
            struct.pack(">h6s", 6, b"topic2"),   # Topic for the request
            struct.pack(">i", 1),                # One partition
            struct.pack(">i", 2),                # Partition 2
            struct.pack(">q", 345),              # Offset 345
            struct.pack(">h", -1),               # Null metadata
        ])

        expected1 = b"".join([header, topic1, topic2])
        expected2 = b"".join([header, topic2, topic1])

        encoded = KafkaToolProtocol.encode_offset_commit_request_kafka(b"client_id", 42, b"group_id", [
            OffsetCommitRequest(b"topic1", 0, 123, None),
            OffsetCommitRequest(b"topic1", 1, 234, None),
            OffsetCommitRequest(b"topic2", 2, 345, None),
        ])

        assert encoded in [expected1, expected2]
