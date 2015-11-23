import contextlib
import sys

import mock
import pytest
from kazoo.exceptions import ZookeeperError

from yelp_kafka_tool.kafka_consumer_manager. \
    commands.rename_group import RenameGroup


@mock.patch(
    'yelp_kafka_tool.kafka_consumer_manager.'
    'commands.rename_group.KafkaClient',
    autospec=True,
)
class TestRenameGroup(object):

    @contextlib.contextmanager
    def mock_kafka_info(self, topics_partitions):
        with contextlib.nested(
            mock.patch.object(
                RenameGroup,
                "preprocess_args",
                spec=RenameGroup.preprocess_args,
                return_value=topics_partitions,
            ),
            mock.patch.object(
                RenameGroup,
                "prompt_user_input",
                spec=RenameGroup.prompt_user_input,
            ),
            mock.patch(
                "yelp_kafka_tool.kafka_consumer_manager."
                "commands.rename_group.ZK",
                autospec=True
            ),
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            mock_ZK.return_value.__enter__.return_value = mock_ZK
            yield mock_process_args, mock_user_confirm, mock_ZK

    def test_run(self, mock_client):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.get_children.return_value = [
                'some_topic', 'another_topic'
            ]
            obj.get.return_value = (0, 0)
            cluster_config = mock.Mock(zookeeper='some_ip')
            args = mock.Mock(old_groupid='old_group', new_groupid='new_group')
            zk_old_group_get_calls = [
                mock.call("/consumers/old_group/offsets/topic1/0"),
                mock.call("/consumers/old_group/offsets/topic1/1"),
                mock.call("/consumers/old_group/offsets/topic1/2"),
                mock.call("/consumers/old_group/offsets/topic2/0"),
                mock.call("/consumers/old_group/offsets/topic2/1"),
            ]
            zk_old_group_delete_calls = [
                mock.call("/consumers/old_group", recursive=True),
            ]
            zk_new_group_calls = [
                mock.call(
                    "/consumers/new_group/offsets/topic1/0",
                    value=bytes(0),
                    makepath=True
                ),
                mock.call(
                    "/consumers/new_group/offsets/topic1/1",
                    value=bytes(0),
                    makepath=True
                ),
                mock.call(
                    "/consumers/new_group/offsets/topic1/2",
                    value=bytes(0),
                    makepath=True
                ),
                mock.call(
                    "/consumers/new_group/offsets/topic2/0",
                    value=bytes(0),
                    makepath=True
                ),
                mock.call(
                    "/consumers/new_group/offsets/topic2/1",
                    value=bytes(0),
                    makepath=True
                ),
            ]

            RenameGroup.run(args, cluster_config)

            assert mock_user_confirm.call_count == 1
            obj.get.call_args_list == zk_old_group_get_calls
            obj.delete.call_args_list == zk_old_group_delete_calls
            obj.create.call_args_list == zk_new_group_calls

    def test_run_same_groupids(self, mock_client):
        topics_partitions = {}
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            with mock.patch.object(sys, "exit", autospec=True) as mock_exit:
                cluster_config = mock.Mock(zookeeper='some_ip')
                args = mock.Mock(
                    old_groupid='my_group',
                    new_groupid='my_group',
                )

                RenameGroup.run(args, cluster_config)

                mock_exit.assert_called_once_with(1)

    def test_run_topic_already_subscribed_to_error(self, mock_client):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            with mock.patch.object(sys, "exit", autospec=True) as mock_exit:
                obj = mock_ZK.return_value.__enter__.return_value
                cluster_config = mock.Mock(zookeeper='some_ip')
                args = mock.Mock(
                    old_groupid='old_group',
                    new_groupid='new_group',
                )
                obj.get_children.return_value = ['topic1']
                obj.get.return_value = (0, 0)

                RenameGroup.run(args, cluster_config)

                mock_exit.assert_called_once_with(1)

    def test_run_create_zknode_error(self, mock_client):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            cluster_config = mock.Mock(zookeeper='some_ip')
            args = mock.Mock(old_groupid='old_group', new_groupid='new_group')
            obj.get_children.return_value = [
                'some_topic', 'another_topic'
            ]
            obj.get.return_value = (0, 0)
            obj.create.side_effect = ZookeeperError("Boom!")

            with pytest.raises(ZookeeperError):
                RenameGroup.run(args, cluster_config)
            assert mock_user_confirm.call_count == 1

    def test_run_delete_zknode_error(self, mock_client):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            cluster_config = mock.Mock(zookeeper='some_ip')
            args = mock.Mock(old_groupid='old_group', new_groupid='new_group')
            obj.get_children.return_value = []
            obj.get.return_value = (0, 0)
            obj.delete.side_effect = ZookeeperError("Boom!")

            with pytest.raises(ZookeeperError):
                RenameGroup.run(args, cluster_config)
            assert mock_user_confirm.call_count == 0
