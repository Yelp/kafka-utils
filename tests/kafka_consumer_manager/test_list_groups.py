import contextlib
import sys

import mock
from kazoo.exceptions import NoNodeError

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.list_groups import ListGroups


class TestListGroups(object):

    @contextlib.contextmanager
    def mock_kafka_info(self, topics_partitions):
        with mock.patch.object(
            ListGroups,
            "preprocess_args",
            spec=ListGroups.preprocess_args,
            return_value=topics_partitions,
        ) as mock_process_args, mock.patch(
            "yelp_kafka_tool.kafka_consumer_manager."
            "commands.list_groups.ZK",
            autospec=True
        ) as mock_ZK:
            mock_ZK.return_value.__enter__.return_value = mock_ZK
            yield mock_process_args, mock_ZK

    @mock.patch("yelp_kafka_tool.kafka_consumer_manager.commands.list_groups.print", create=True)
    def test_run(self, mock_print):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.get_children.return_value = [
                'group1', 'group2', 'group3'
            ]
            cluster_config = mock.Mock(zookeeper='some_ip', type='some_cluster_type')
            cluster_config.configure_mock(name='some_cluster_name')
            args = mock.Mock()

            expected_print = [
                mock.call("Consumer Groups:"),
                mock.call("\tgroup1"),
                mock.call("\tgroup2"),
                mock.call("\tgroup3"),
                mock.call("3 groups found for cluster some_cluster_name "
                          "of type some_cluster_type"),
            ]

            ListGroups.run(args, cluster_config)
            assert mock_print.call_args_list == expected_print

    @mock.patch("yelp_kafka_tool.kafka_consumer_manager.commands.list_groups.print", create=True)
    def test_run_zknode_error(self, mock_print):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            cluster_config = mock.Mock(zookeeper='some_ip')
            args = mock.Mock()
            obj.get_children.side_effect = NoNodeError("Boom!")

            ListGroups.run(args, cluster_config)
            mock_print.assert_called_with(
                "Error: No consumers node found in zookeeper",
                file=sys.stderr,
            )
