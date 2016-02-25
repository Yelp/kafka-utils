import mock
import pytest
from kafka.client import KafkaClient
from yelp_kafka.monitoring import ConsumerPartitionOffsets

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.offset_restore import OffsetRestore


class TestOffsetRestore(object):

    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1, 2, 3],
        "topic3": [0, 1],
    }
    consumer_offsets_metadata = {
        'topic1':
        [
            ConsumerPartitionOffsets(topic='topic1', partition=0, current=20, highmark=655, lowmark=655),
            ConsumerPartitionOffsets(topic='topic1', partition=1, current=10, highmark=655, lowmark=655)
        ]
    }
    parsed_consumer_offsets = {'groupid': 'group1', 'offsets': {'topic1': {0: 10, 1: 20}}}
    new_consumer_offsets = {'topic1': {0: 10, 1: 20}}
    kafka_consumer_offsets = {'topic1': [
        ConsumerPartitionOffsets(topic='topic1', partition=0, current=30, highmark=40, lowmark=10),
        ConsumerPartitionOffsets(topic='topic1', partition=1, current=20, highmark=40, lowmark=10),
    ]}

    @pytest.fixture
    def mock_kafka_client(self):
        mock_kafka_client = mock.MagicMock(
            spec=KafkaClient
        )
        mock_kafka_client.get_partition_ids_for_topic. \
            side_effect = self.topics_partitions
        return mock_kafka_client

    def test_restore_offsets(self, mock_kafka_client):
        with mock.patch(
            "yelp_kafka_tool.kafka_consumer_manager."
            "commands.offset_restore.set_consumer_offsets",
            return_value=[],
            autospec=True,
        ) as mock_set_offsets, mock.patch.object(
            OffsetRestore,
            "parse_consumer_offsets",
            spec=OffsetRestore.parse_consumer_offsets,
            return_value=self.parsed_consumer_offsets,
        ), mock.patch(
            "yelp_kafka_tool.kafka_consumer_manager."
            "commands.offset_restore.get_consumer_offsets_metadata",
            return_value=self.consumer_offsets_metadata,
            autospec=True,
        ):
            OffsetRestore.restore_offsets(mock_kafka_client, self.parsed_consumer_offsets)

            ordered_args, _ = mock_set_offsets.call_args
            assert ordered_args[1] == 'group1'
            assert ordered_args[2] == self.new_consumer_offsets

    def test_build_new_offsets(self, mock_kafka_client):
        new_offsets = OffsetRestore.build_new_offsets(
            mock_kafka_client,
            {'topic1': {0: 10, 1: 20}},
            {'topic1': [0, 1]},
            self.kafka_consumer_offsets,
        )

        assert new_offsets == self.new_consumer_offsets
