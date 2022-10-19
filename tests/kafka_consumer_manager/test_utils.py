import struct
from collections import namedtuple
from unittest import mock

import pytest
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata
from kafka.structs import OffsetAndTimestamp
from kafka.structs import TopicPartition

from kafka_utils.kafka_consumer_manager.util import consumer_commit_for_times
from kafka_utils.kafka_consumer_manager.util import consumer_partitions_for_topic
from kafka_utils.kafka_consumer_manager.util import get_group_partition
from kafka_utils.kafka_consumer_manager.util import get_kafka_group_reader
from kafka_utils.kafka_consumer_manager.util import get_offset_topic_partition_count
from kafka_utils.kafka_consumer_manager.util import InvalidMessageException
from kafka_utils.kafka_consumer_manager.util import KafkaAdminGroupReader
from kafka_utils.kafka_consumer_manager.util import KafkaGroupReader
from kafka_utils.kafka_consumer_manager.util import topic_offsets_for_timestamp
from kafka_utils.util.error import UnknownTopic
from kafka_utils.util.offsets import PartitionOffsets

Message = namedtuple("Message", ["partition", "offset", "key", "value"])


class TestKafkaAdminGroupReader:

    @pytest.fixture
    def mock_admin_client(self):
        with mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "util.KafkaAdminClient",
            autospec=True,
        ) as mock_admin_client:
            yield mock_admin_client

    def test_read_group(self, mock_admin_client):
        reader = KafkaAdminGroupReader(mock.MagicMock())

        reader.admin_client.list_consumer_group_offsets.return_value = {
            TopicPartition("t1", 0): OffsetAndMetadata(42, ""),
            TopicPartition("t1", 1): OffsetAndMetadata(42, ""),
            TopicPartition("t2", 1): OffsetAndMetadata(42, ""),
        }

        topics = reader.read_group("group")
        assert set(topics) == {"t1", "t2"}

    def test_read_groups(self, mock_admin_client):
        reader = KafkaAdminGroupReader(mock.MagicMock())

        with mock.patch.object(
            reader,
            "_list_groups",
            autospec=True,
            return_value=["group1", "group2"],
        ), mock.patch.object(
            reader,
            "read_group",
            autospec=True,
        ) as mock_read_group:
            mock_read_group.side_effect = lambda group: {
                "group1": ["t1"],
                "group2": ["t2", "t3"],
            }[group]

            groups = reader.read_groups(list_only=False)
            assert set(groups.keys()) == {"group1", "group2"}
            assert groups["group1"] == ["t1"]
            assert set(groups["group2"]) == {"t2", "t3"}

    def test_read_groups_list_only(self, mock_admin_client):
        reader = KafkaAdminGroupReader(mock.MagicMock())

        with mock.patch.object(
            reader,
            "_list_groups",
            autospec=True,
            return_value=["group1", "group2"],
        ), mock.patch.object(
            reader,
            "read_group",
            autospec=True,
        ) as mock_read_group:
            reader.read_groups(list_only=True)
            assert mock_read_group.call_count == 0

    def test_list_groups(self, mock_admin_client):
        reader = KafkaAdminGroupReader(mock.MagicMock())

        # https://github.com/dpkp/kafka-python/blob/1.4.7/kafka/admin/client.py#L1009
        reader.admin_client.list_consumer_groups.return_value = [
            ("group1", "consumer"),
            ("group2", ""),
        ]

        groups = reader._list_groups()
        assert groups == ["group1", "group2"]


class TestKafkaGroupReader:

    groups = [r'^test\..*', r'^my_test$', r'^my_test2$']

    key_v0_ok = b''.join([
        struct.pack('>h', 0),  # Schema: offset commit
        struct.pack('>h6s', 6, b'group1'),  # Group name
        struct.pack('>h6s', 6, b'topic1'),  # Topic name
        struct.pack('>l', 15),  # Partition
    ])

    value_v0_ok = b''.join([
        struct.pack('>h', 0),  # Schema: version 0
        struct.pack('>q', 123),  # Offset 123
    ])

    value_v3_ok = b''.join([
        struct.pack('>h', 3),
        struct.pack('>q', 1230552),
        struct.pack('!l', -1),
        struct.pack('>h', 0),
        struct.pack('>q', 1567674248094),
    ])

    def test_parse_consumer_offset_message_correct_v0(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = Message(0, '__consumer_offsets', self.key_v0_ok, self.value_v0_ok)
        group, topic, partition, offset = kafka_group_reader.parse_consumer_offset_message(message)

        assert group == 'group1'
        assert topic == 'topic1'
        assert partition == 15
        assert offset == 123

    def test_parse_consumer_offset_message_correct_v3(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = Message(0, '__consumer_offsets', self.key_v0_ok, self.value_v3_ok)
        group, topic, partition, offset = kafka_group_reader.parse_consumer_offset_message(message)

        assert group == 'group1'
        assert topic == 'topic1'
        assert partition == 15
        assert offset == 1230552

    def test_parse_consumer_offset_message_no_value(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = Message(0, '__consumer_offsets', self.key_v0_ok, None)
        group, topic, partition, offset = kafka_group_reader.parse_consumer_offset_message(message)

        assert group == 'group1'
        assert topic == 'topic1'
        assert partition == 15
        assert offset is None

    @mock.patch.object(KafkaGroupReader, 'parse_consumer_offset_message')
    def test_process_consumer_offset_message_group(self, parse_mock):
        parse_mock.side_effect = [('test.a', 'topic1', 0, 123),
                                  ('test.a', 'topic1', 1, 124),
                                  ('test.a', 'topic2', 0, 125),
                                  ('my_test', 'topic1', 0, 123),
                                  ('my_test', 'topic2', 0, 124),
                                  ('my_test', 'topic2', 0, None),
                                  ('my_test2', 'topic3', 0, 123), ]
        kafka_group_reader = KafkaGroupReader(mock.Mock())
        for _ in range(7):
            message = mock.MagicMock(spec=ConsumerRecord)
            kafka_group_reader.process_consumer_offset_message(message)

        expected = {
            'test.a': {
                'topic1': {0: 123, 1: 124},
                'topic2': {0: 125},
            },
            'my_test2': {
                'topic3': {0: 123},
            },
            'my_test': {
                'topic1': {0: 123},
            },
        }

        # Convert the defaultdict to a normal dict for comparison
        actual = {}
        for group, topics in kafka_group_reader._kafka_groups.items():
            actual[group] = {}
            for topic, partitions in topics.items():
                actual[group][topic] = {}
                for partition, offset in partitions.items():
                    actual[group][topic][partition] = offset

        assert actual == expected

    @mock.patch.object(KafkaGroupReader, 'parse_consumer_offset_message')
    def test_process_consumer_offset_message_invalid_message(self, parse_mock):
        parse_mock.side_effect = InvalidMessageException
        kafka_group_reader = KafkaGroupReader(mock.Mock())
        message = mock.MagicMock(spec=ConsumerRecord)
        kafka_group_reader.process_consumer_offset_message(message)

        assert kafka_group_reader._kafka_groups == dict()

    def test_process_consumer_offset_message_topic_get(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)

        assert kafka_group_reader._kafka_groups == {}

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
            assert kafka_group_reader._kafka_groups['test_group'] == {'test_topic': {0: 45}}

    def test_process_consumer_offset_message_topic_pop_no_offset(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)

        kafka_group_reader._kafka_groups['test_group'] = {'test_topic': {0: 45}}
        assert kafka_group_reader._kafka_groups['test_group'] == {'test_topic': {0: 45}}

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
            assert kafka_group_reader._kafka_groups == {'test_group': {}}

    def test_read_groups(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
        ) as mock_consumer:
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={
                    0: PartitionOffsets('__consumer_offsets', 0, 45, 0),
                    1: PartitionOffsets('__consumer_offsets', 1, 20, 0),
                    2: PartitionOffsets('__consumer_offsets', 2, 25, 25),
                    3: PartitionOffsets('__consumer_offsets', 3, 0, 0),
                },
                autospec=True
            ):
                with mock.patch.object(
                    kafka_group_reader,
                    'parse_consumer_offset_message',
                    side_effect=iter([
                        ('test_group', 'test_topic', 0, 45),
                        ('test_group2', 'test_topic2', 0, 20),
                    ]),
                    autospec=True,
                ):
                    mock_consumer.return_value.__iter__.return_value = iter([
                        mock.Mock(offset=44, partition=0, topic='test_topic'),
                        mock.Mock(offset=19, partition=1, topic='test_topic'),
                    ])
                    mock_consumer.return_value.partitions_for_topic.return_value = [0, 1]
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader._kafka_groups['test_group'] == {"test_topic": {0: 45}}
                    assert kafka_group_reader._kafka_groups['test_group2'] == {"test_topic2": {0: 20}}
                    mock_consumer.return_value.assign.call_args_list == [
                        mock.call([
                            TopicPartition("__consumer_offsets", 0),
                            TopicPartition("__consumer_offsets", 1),
                        ]),
                        mock.call([TopicPartition("__consumer_offsets", 0)]),
                    ]

    def test_read_groups_with_partition(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
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
                    mock_consumer.return_value.__iter__.return_value = iter([mock.Mock(partition=0, topic='test_topic', offset=45)])
                    kafka_group_reader.read_groups(partition=0)
                    assert kafka_group_reader._kafka_groups['test_group'] == {"test_topic": {0: 45}}
                    mock_consumer.return_value.assign.assert_called_once_with(
                        [TopicPartition("__consumer_offsets", 0)]
                    )

    def test_remove_unsubscribed_topics(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        kafka_group_reader._kafka_groups = {
            'my_group1': {
                'topic1': {0: 0, 1: 1},
                'topic2': {0: 1},
            },
            'my_group2': {
                'topic1': {0: 0, 1: 0},
                'topic2': {0: 1},
            },
        }

        expected = {
            'my_group1': {
                'topic1': {0: 0, 1: 1},
                'topic2': {0: 1},
            },
            'my_group2': {
                'topic2': {0: 1},
            },
        }

        kafka_group_reader._remove_unsubscribed_topics()

        assert kafka_group_reader._kafka_groups == expected

    @mock.patch("kafka_utils.kafka_consumer_manager.util.get_topic_partition_metadata")
    def test_get_offset_topic_partition_count_raise(self, mock_get_metadata):
        mock_get_metadata.return_value = {'topic1': {0: None}}
        kafka_config = mock.Mock(broker_list=['localhost:9092'])
        with pytest.raises(UnknownTopic):
            get_offset_topic_partition_count(kafka_config)

    @mock.patch("kafka_utils.kafka_consumer_manager.util.get_topic_partition_metadata")
    def test_get_offset_topic_partition_count(self, mock_get_metadata):
        mock_get_metadata.return_value = {'topic1': {0: None},
                                          '__consumer_offsets': {0: None, 1: None}}
        kafka_config = mock.Mock(broker_list=['localhost:9092'])
        assert get_offset_topic_partition_count(kafka_config) == 2

    def test_get_group_partition(self):
        result1 = get_group_partition('815e79b2-be20-11e6-96b6-0697c842cbe5', 50)
        result2 = get_group_partition('83e3f292-be26-11e6-b509-0697c842cbe5', 50)
        result3 = get_group_partition('adaceffc-be26-11e6-8eab-0697c842cbe5', 20)

        assert result1 == 10
        assert result2 == 44
        assert result3 == 5


class TestKafkaConsumerTimestamps:

    @mock.patch("kafka.KafkaConsumer")
    def test_topic_offsets_timestamp(self, mock_kconsumer):
        timestamp = 123
        topics = ["topic1", "topic2", "topic3"]
        parts = [0, 1]
        mock_kconsumer.partitions_for_topic.return_value = set(parts)
        expected = {TopicPartition(topic, part): timestamp for topic in topics for part in parts}

        topic_offsets_for_timestamp(mock_kconsumer, timestamp, topics)

        mock_kconsumer.offsets_for_times.assert_called_once_with(expected)

    @mock.patch("kafka.KafkaConsumer")
    def test_commit_for_times(self, mock_kconsumer):
        timestamp = 123
        topics = ["topic1", "topic2", "topic3"]
        parts = [0, 1]

        partition_to_offset = {
            TopicPartition(topic, part): OffsetAndTimestamp(42, timestamp)
            for topic in topics for part in parts
        }

        expected = {
            TopicPartition(topic, part): OffsetAndMetadata(42, metadata=None)
            for topic in topics for part in parts
        }

        consumer_commit_for_times(mock_kconsumer, partition_to_offset)

        mock_kconsumer.commit.assert_called_once_with(expected)

    @mock.patch("kafka.KafkaConsumer")
    def test_commit_for_times_atomic(self, mock_kconsumer):
        partition_to_offset = {
            TopicPartition("topic1", 0): None,
            TopicPartition("topic2", 0): OffsetAndTimestamp(123, 123),
        }

        consumer_commit_for_times(mock_kconsumer, partition_to_offset, atomic=True)
        assert mock_kconsumer.commit.call_count == 0

    @mock.patch("kafka.KafkaConsumer")
    def test_commit_for_times_none(self, mock_kconsumer):
        partition_to_offset = {
            TopicPartition("topic1", 0): None,
            TopicPartition("topic2", 0): None,
        }

        consumer_commit_for_times(mock_kconsumer, partition_to_offset)
        assert mock_kconsumer.commit.call_count == 0

    @mock.patch("kafka.KafkaConsumer")
    def test_consumer_partitions_for_topic(self, mock_kconsumer):
        topic = "topic1"
        partitions = {0, 1, 2, 3, 4}
        mock_kconsumer.partitions_for_topic.return_value = partitions
        expected = [TopicPartition(topic, part) for part in partitions]

        actual = consumer_partitions_for_topic(mock_kconsumer, topic)

        assert set(actual) == set(expected)


class TestGetKafkaGroupReader:

    @pytest.fixture
    def mock_admin_group_reader(self):
        with mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "util.KafkaAdminGroupReader",
            autospec=True,
        ):
            yield

    @pytest.fixture
    def mock_group_reader(self):
        with mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "util.KafkaGroupReader",
            autospec=True,
        ):
            yield

    def test_get_kafka_group_reader(self, mock_admin_group_reader, mock_group_reader):
        admin = get_kafka_group_reader(mock.Mock(), use_admin_client=True)
        assert isinstance(admin, KafkaAdminGroupReader)

        default = get_kafka_group_reader(mock.Mock(), use_admin_client=False)
        assert isinstance(default, KafkaGroupReader)
