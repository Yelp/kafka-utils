import mock
from kafka.common import ConsumerTimeout

from kafka_utils.kafka_consumer_manager.util import KafkaGroupReader
from kafka_utils.util.offsets import PartitionOffsets


class TestUtils(object):

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
            ]
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
                ]
        ):
            kafka_group_reader.process_consumer_offset_message('test message')
            assert kafka_group_reader.kafka_groups == {}

    def test_read_groups(self):

        class Mock_Consumer(object):

            def next(self):
                return message

        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        message = mock.Mock(partition=0, topic='test_topic')
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
                return_value=Mock_Consumer()
        ):
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={0: PartitionOffsets(
                    'test_topic',
                    0,
                    45,
                    0
                )
                }
            ):
                with mock.patch.object(
                    kafka_group_reader,
                    'parse_consumer_offset_message',
                    return_value=[
                        'test_group',
                        'test_topic',
                        0,
                        45
                    ]
                ):
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader.kafka_groups['test_group'] == {"test_topic"}
                    assert len(kafka_group_reader.finished_partitions) == 1

    def test_read_groups_with_consumer_timeout(self):

        class Mock_Consumer(object):

            def next(self):
                raise ConsumerTimeout

        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.util.KafkaConsumer',
                return_value=Mock_Consumer()
        ):
            with mock.patch.object(
                kafka_group_reader,
                'get_current_watermarks',
                return_value={0: PartitionOffsets(
                    'test_topic',
                    0,
                    45,
                    0
                )
                }
            ):
                with mock.patch.object(
                    kafka_group_reader,
                    'process_consumer_offset_message',
                ) as mock_process_consumer_offset_message:
                    kafka_group_reader.read_groups()
                    assert kafka_group_reader.kafka_groups == {}
                    assert mock_process_consumer_offset_message.call_count == 0
                    assert len(kafka_group_reader.finished_partitions) == 0

    def test_get_max_offset(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)
        with mock.patch.object(
            kafka_group_reader,
            'get_current_watermarks',
            return_value={0: PartitionOffsets(
                'test_topic',
                0,
                45,
                0
            )
            }
        ) as mock_get_watermarks:
            kafka_group_reader.watermarks = mock_get_watermarks()
            highmark = kafka_group_reader.get_max_offset(0)
            assert highmark == 45
