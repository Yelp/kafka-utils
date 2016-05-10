import mock
import pytest

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.offset_get import OffsetGet


class TestOffsetGet(object):

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
                'yelp_kafka_tool.kafka_consumer_manager.'
                'commands.offset_get.KafkaClient',
                autospec=True,
        ) as mock_client:
            yield mock_client

    def test_get_offsets(self, client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'yelp_kafka_tool.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                client,
                consumer_group,
                topics,
                'zookeeper',
            )

            assert client.load_metadata_for_topics.call_count == 1
            assert client.send_offset_fetch_request.call_count == 1
            assert client.send_offset_fetch_request_kafka.call_count == 0

    def test_get_offsets_kafka(self, client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'yelp_kafka_tool.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                client,
                consumer_group,
                topics,
                'kafka',
            )

            assert client.load_metadata_for_topics.call_count == 1
            assert client.send_offset_fetch_request.call_count == 0
            assert client.send_offset_fetch_request_kafka.call_count == 1

    def test_get_offsets_foo(self, client):
        # this should fail:
        # assert client.send_offset_fetch_request_foo.call_count == 0

        assert hasattr(client, 'send_offset_fetch_request')
        assert hasattr(client, 'send_offset_fetch_request_kafka')
        assert not hasattr(client, 'send_offset_fetch_request_foo')
