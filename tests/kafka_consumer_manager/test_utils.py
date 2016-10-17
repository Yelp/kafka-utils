import mock

from kafka_utils.kafka_consumer_manager.util import KafkaGroupReader


class TestUtils(object):

    def test_process_consumer_offset_message_topic_get(self):
        kafka_config = mock.Mock()
        kafka_group_reader = KafkaGroupReader(kafka_config)

        assert kafka_group_reader.kafka_groups == {}

        with mock.patch.object(kafka_group_reader,
                               'parse_consumer_offset_message',
                               return_value=['test_group',
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

        with mock.patch.object(kafka_group_reader,
                               'parse_consumer_offset_message',
                               return_value=['test_group',
                                             'test_topic',
                                             0,
                                             None
                                             ]
                               ):
            kafka_group_reader.process_consumer_offset_message('test message')
            assert kafka_group_reader.kafka_groups == {}
