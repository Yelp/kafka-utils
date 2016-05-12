import mock
import pytest

from kafka_tools.kafka_consumer_manager. \
    commands.offset_rewind import OffsetRewind


class TestOffsetRewind(object):
    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1]
    }

    @mock.patch('kafka_tools.kafka_consumer_manager.'
                'commands.offset_rewind.KafkaClient')
    def test_run(self, mock_client):
        with mock.patch.object(
            OffsetRewind,
            "preprocess_args",
            spec=OffsetRewind.preprocess_args,
            return_value=self.topics_partitions,
        ), mock.patch(
            "kafka_tools.kafka_consumer_manager."
            "commands.offset_rewind.rewind_consumer_offsets",
            autospec=True
        ) as mock_rewind:
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock()
            OffsetRewind.run(args, cluster_config)

            ordered_args, _ = mock_rewind.call_args
            assert ordered_args[1] == args.groupid
            assert ordered_args[2] == self.topics_partitions
            mock_client.return_value.load_metadata_for_topics. \
                assert_called_once_with()
            mock_client.return_value.close.assert_called_once_with()

    @mock.patch('kafka_tools.kafka_consumer_manager'
                '.commands.offset_rewind.KafkaClient')
    def test_run_type_error(self, mock_client):
        with mock.patch.object(
            OffsetRewind,
            "preprocess_args",
            spec=OffsetRewind.preprocess_args,
            return_value="some_string",
        ):
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock()
            with pytest.raises(TypeError):
                OffsetRewind.run(args, cluster_config)
