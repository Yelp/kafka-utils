import mock

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.offset_get import OffsetGet


class TestOffsetGet(object):

    @mock.patch(
        'yelp_kafka_tool.kafka_consumer_manager.'
        'commands.offset_save.KafkaClient', autospec=True)
    def test_get_offsets(self, mock_client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'yelp_kafka_tool.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                mock_client,
                consumer_group,
                topics,
                'zookeeper',
            )

            assert mock_client.load_metadata_for_topics.call_count == 1
            assert mock_client.send_offset_fetch_request.call_count == 1
            assert mock_client.send_offset_fetch_request_kafka.call_count == 0

    @mock.patch(
        'yelp_kafka_tool.kafka_consumer_manager.'
        'commands.offset_save.KafkaClient', autospec=True)
    def test_get_offsets_kafka(self, mock_client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'yelp_kafka_tool.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                mock_client,
                consumer_group,
                topics,
                'kafka',
            )

            assert mock_client.load_metadata_for_topics.call_count == 1
            assert mock_client.send_offset_fetch_request.call_count == 0
            assert mock_client.send_offset_fetch_request_kafka.call_count == 1
