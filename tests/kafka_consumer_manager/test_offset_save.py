import mock
from yelp_kafka.monitoring import ConsumerPartitionOffsets

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.offset_save import OffsetSave


class TestOffsetSave(object):
    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1, 2, 3],
        "topic3": [0, 1],
    }
    consumer_offsets_metadata = {
        'topic1':
        [
            ConsumerPartitionOffsets(topic='topic1', partition=0, current=10, highmark=655, lowmark=655),
            ConsumerPartitionOffsets(topic='topic1', partition=1, current=20, highmark=655, lowmark=655),
        ]
    }
    parsed_consumer_offsets = {'groupid': 'group1', 'offsets': {'topic1': {'0': 10, '1': 20}}}

    @mock.patch('yelp_kafka_tool.kafka_consumer_manager.'
                'commands.offset_save.KafkaClient')
    def test_save_offsets(self, mock_client):
        with mock.patch.object(
            OffsetSave,
            "write_offsets_to_file",
            spec=OffsetSave.write_offsets_to_file,
            return_value=[],
        ) as mock_write_offsets:
            filename = 'offset_file'
            consumer_group = 'group1'
            OffsetSave.save_offsets(
                self.consumer_offsets_metadata,
                self.topics_partitions,
                filename,
                consumer_group
            )

            ordered_args, _ = mock_write_offsets.call_args
            assert ordered_args[0] == filename
            assert ordered_args[1] == self.parsed_consumer_offsets

    @mock.patch('yelp_kafka_tool.kafka_consumer_manager.'
                'commands.offset_save.KafkaClient')
    def test_run(self, mock_client):
        with mock.patch.object(
            OffsetSave,
            "preprocess_args",
            spec=OffsetSave.preprocess_args,
            return_value=self.topics_partitions,
        ), mock.patch(
            "yelp_kafka_tool.kafka_consumer_manager."
            "commands.offset_save.get_consumer_offsets_metadata",
            return_value=self.consumer_offsets_metadata,
            autospec=True,
        ), mock.patch.object(
            OffsetSave,
            "write_offsets_to_file",
            spec=OffsetSave.write_offsets_to_file,
            return_value=[],
        ) as mock_write_offsets:
            args = mock.Mock(
                groupid="group1",
                json_file="some_file",
            )
            cluster_config = mock.Mock()
            OffsetSave.run(args, cluster_config)

            mock_client.return_value.load_metadata_for_topics. \
                assert_called_once_with()
            mock_client.return_value.close.assert_called_once_with()
            ordered_args, _ = mock_write_offsets.call_args
            assert ordered_args[0] == "some_file"
            assert ordered_args[1] == self.parsed_consumer_offsets
