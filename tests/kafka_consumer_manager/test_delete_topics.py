import contextlib
import mock
import pytest

from kazoo.exceptions import (
    NoNodeError,
    ZookeeperError,
)

from yelp_kafka_tool.kafka_consumer_manager.commands. \
    delete_topics import DeleteTopics


@mock.patch('yelp_kafka_tool.kafka_consumer_manager.'
            'commands.delete_topics.KafkaClient')
class TestDeleteTopics(object):
    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1]
    }

    @contextlib.contextmanager
    def mock_kafka_info(self):
        with contextlib.nested(
            mock.patch.object(
                DeleteTopics,
                "preprocess_args",
                spec=DeleteTopics.preprocess_args,
                return_value=self.topics_partitions,
            ),
            mock.patch(
                'yelp_kafka_tool.kafka_consumer_manager.'
                'commands.delete_topics.ZK',
                autospec=True
            )
        ) as (mock_writer_process_args, mock_ZK):
            mock_ZK.return_value.__enter__.return_value = mock_ZK
            yield mock_writer_process_args, mock_ZK

    def test_run(self, mock_client):
        with self.mock_kafka_info(
        ) as (mock_writer_process_args, mock_ZK):
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock(zookeeper='some_ip')
            DeleteTopics.run(args, cluster_config)

            calls = [
                mock.call(
                    args.groupid,
                    "topic1",
                    [0, 1, 2]
                ),
                mock.call(
                    args.groupid,
                    "topic2",
                    [0, 1]
                ),
            ]

            mock_ZK.return_value.__enter__.assert_called_once()
            obj = mock_ZK.return_value.__enter__.return_value
            obj.delete_topic_partitions.call_args_list == calls
            obj.__exit__.assert_called_once()

    def test_run_no_node_error(self, mock_client):
        with self.mock_kafka_info(
        ) as (mock_writer_process_args, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.delete_topic_partitions.side_effect = NoNodeError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            DeleteTopics.run(args, cluster_config)

            mock_ZK.return_value.__enter__.assert_called_once()
            obj.__exit__.assert_called_once()
            # We should not raise the exception

    def test_run_any_other_exception(self, mock_client):
        with self.mock_kafka_info(
        ) as (mock_writer_process_args, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            obj.delete_topic_partitions.side_effect = ZookeeperError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            with pytest.raises(ZookeeperError):
                DeleteTopics.run(args, cluster_config)
            mock_ZK.return_value.__enter__.assert_called_once()
            obj.__exit__.assert_called_once()
