import struct
from collections import namedtuple

import mock
from kafka.common import ConsumerTimeout
from kafka.common import KafkaMessage
from kafka.common import LeaderNotAvailableError

from kafka_utils.kafka_consumer_manager.util import InvalidMessageException
from kafka_utils.kafka_consumer_manager.util import KafkaGroupReader
from kafka_utils.util.offsets import PartitionOffsets

Message = namedtuple("Message", ["partition", "offset", "key", "value"])


class TestKafkaGroupReader(object):

    groups = ['^test\..*', '^my_test$', '^my_test2$']

    key_ok = b''.join([
        struct.pack('>h', 0),  # Schema: offset commit
        struct.pack('>h6s', 6, b'group1'),  # Group name
        struct.pack('>h6s', 6, b'topic1'),  # Topic name
        struct.pack('>l', 15),  # Partition
    ])

    value_ok = b''.join([
        struct.pack('>h', 0),  # Schema: version 0
        struct.pack('>q', 123),  # Offset 123
    ])

    key_wrong = b''.join([
        struct.pack('>h', 2),  # Schema: group message
        struct.pack('>h6s', 6, b'group1'),  # Group name
        struct.pack('>h6s', 6, b'topic1'),  # Topic name
        struct.pack('>l', 15),  # Partition
    ])

    value_wrong = b''.join([
        struct.pack('>h', 3),  # Schema: invalid
        struct.pack('>q', 123),  # Offset 123
    ])

    def test_parse_consumer_offset_message_correct(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = Message(0, '__consumer_offsets', self.key_ok, self.value_ok)
        group, topic, partition, offset = kafka_group_reader.parse_consumer_offset_message(message)

        assert group == 'group1'
        assert topic == 'topic1'
        assert partition == 15
        assert offset == 123

    def test_parse_consumer_offset_message_no_value(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = Message(0, '__consumer_offsets', self.key_ok, None)
        group, topic, partition, offset = kafka_group_reader.parse_consumer_offset_message(message)

        assert group == 'group1'
        assert topic == 'topic1'
        assert partition == 15
        assert offset is None

    @mock.patch.object(KafkaGroupReader, 'parse_consumer_offset_message')
    def test_process_consumer_offset_message_grous(self, parse_mock):
        parse_mock.side_effect = [('test.a', 'topic1', 0, 123),
                                  ('test.a', 'topic1', 1, 124),
                                  ('test.a', 'topic2', 0, 125),
                                  ('my_test', 'topic1', 0, 123),
                                  ('my_test', 'topic2', 0, 124),
                                  ('my_test', 'topic2', 0, None),
                                  ('my_test2', 'topic3', 0, 123), ]
        kafka_group_reader = KafkaGroupReader(mock.Mock())
        for _ in range(7):
            message = mock.MagicMock(spec=KafkaMessage)
            kafka_group_reader.process_consumer_offset_message(message)

        expected = {'test.a': {'topic1', 'topic2'}, 'my_test2': {'topic3'}}
        assert kafka_group_reader.kafka_groups == expected

    @mock.patch.object(KafkaGroupReader, 'parse_consumer_offset_message')
    def test_process_consumer_offset_message_invalid_message(self, parse_mock):
        parse_mock.side_effect = InvalidMessageException
        kafka_group_reader = KafkaGroupReader(mock.Mock())
        message = mock.MagicMock(spec=KafkaMessage)
        kafka_group_reader.process_consumer_offset_message(message)

        assert kafka_group_reader.kafka_groups == dict()

    def test_process_consumer_offset_message_topic_get(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)

        assert kafka_group_reader.kafka_groups == {}

        with mock.patch.object(
            kafka_group_reader,
            'parse_consumer_offset_message',
            return_value=[
                'test_group',
                'test_topic',
                0,
                45
            ],
            autospec=True
        ):
            kafka_group_reader.process_consumer_offset_message('test message')
            assert kafka_group_reader.kafka_groups['test_group'] == {'test_topic'}

    def test_process_consumer_offset_message_topic_pop_no_offset(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)

        kafka_group_reader.kafka_groups['test_group'] = {'test_topic'}
        assert kafka_group_reader.kafka_groups['test_group'] == {'test_topic'}

        with mock.patch.object(
            kafka_group_reader,
            'parse_consumer_offset_message',
            return_value=[
                'test_group',
                'test_topic',
                0,
                None
            ],
            autospec=True
        ):
            kafka_group_reader.process_consumer_offset_message('test message')
            assert kafka_group_reader.kafka_groups == {}

    def test_read_groups(self):

        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
                autospec=True
        ) as mock_consumer:
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={
                    0: PartitionOffsets(
                    'test_topic',
                    0,
                    45,
                    0
                    )
                },
                autospec=True
            ):
                with mock.patch.object(
                    kafka_group_reader,
                    'parse_consumer_offset_message',
                    return_value=[
                        'test_group',
                        'test_topic',
                        0,
                        45
                    ],
                    autospec=True
                ):
                    mock_consumer.return_value.next.return_value = mock.Mock(partition=0, topic='test_topic')
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader.kafka_groups['test_group'] == {"test_topic"}
                    assert len(kafka_group_reader.finished_partitions) == 1

    def test_read_groups_with_consumer_timeout(self):

        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
                autospec=True
        ) as mock_consumer:
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={
                    0: PartitionOffsets(
                    'test_topic',
                    0,
                    45,
                    0
                    )
                },
                autospec=True
            ):
                with mock.patch.object(
                    kafka_group_reader,
                    'process_consumer_offset_message',
                    autospec=True
                ) as mock_process_consumer_offset_message:
                    mock_consumer.return_value.next.side_effect = ConsumerTimeout
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader.kafka_groups == {}
                    assert mock_process_consumer_offset_message.call_count == 0
                    assert len(kafka_group_reader.finished_partitions) == 0
                    assert kafka_group_reader.retry != kafka_group_reader.retry_max
                    assert kafka_group_reader.retry == 0

    def test_read_groups_with_leader_not_available_error(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
                autospec=True
        ) as mock_consumer:
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={
                    0: PartitionOffsets(
                        'test_topic',
                        0,
                        45,
                        0
                    )
                },
                    autospec=True
            ):
                with mock.patch.object(
                        kafka_group_reader,
                        'process_consumer_offset_message',
                        autospec=True
                ) as mock_process_consumer_offset_message:
                    mock_consumer.return_value.next.side_effect = LeaderNotAvailableError
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader.kafka_groups == {}
                    assert mock_process_consumer_offset_message.call_count == 0
                    assert len(kafka_group_reader.finished_partitions) == 0
                    assert kafka_group_reader.retry == kafka_group_reader.retry_max + 1

    def test_get_max_offset(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch.object(
            kafka_group_reader,
            'get_current_watermarks',
            return_value={
                0: PartitionOffsets(
                'test_topic',
                0,
                45,
                0
                )
            },
                autospec=True
        ) as mock_get_watermarks:
            kafka_group_reader.watermarks = mock_get_watermarks()
            highmark = kafka_group_reader.get_max_offset(0)
            assert highmark == 45
