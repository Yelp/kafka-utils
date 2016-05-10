import contextlib

import mock
import pytest
from kazoo.exceptions import ZookeeperError

from yelp_kafka_tool.kafka_consumer_manager.commands. \
    delete_group import DeleteGroup


@mock.patch('yelp_kafka_tool.kafka_consumer_manager.'
            'commands.delete_group.KafkaClient')
class TestDeleteGroup(object):

    @contextlib.contextmanager
    def mock_kafka_info(self):
        with mock.patch(
            'yelp_kafka_tool.kafka_consumer_manager.'
            'commands.delete_group.ZK',
            autospec=True
        ) as mock_ZK:
            mock_ZK.return_value.__enter__.return_value = mock_ZK.return_value
            yield mock_ZK

    def test_run_wipe_delete_group(self, mock_client):
        with self.mock_kafka_info() as mock_ZK:
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            DeleteGroup.run(args, cluster_config)

            obj = mock_ZK.return_value
            assert obj.delete_group.call_args_list == [
                mock.call(args.groupid),
            ]

    def test_run_wipe_delete_group_error(self, mock_client):
        with self.mock_kafka_info() as mock_ZK:
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            obj.delete_group.side_effect = ZookeeperError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            with pytest.raises(ZookeeperError):
                DeleteGroup.run(args, cluster_config)
