import mock
from yelp_kafka.config import ClusterConfig

from yelp_kafka_tool.util.zookeeper import ZK


@mock.patch(
    'yelp_kafka_tool.util.zookeeper.KazooClient',
    autospec=True
)
class TestZK(object):
    cluster_config = ClusterConfig(
        type='mytype',
        name='some_cluster',
        broker_list='some_list',
        zookeeper='some_ip'
    )

    def test_create(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.create(
                '/kafka/consumers/some_group/offsets'
            )
            zk.create(
                '/kafka/consumers/some_group/offsets',
                value='some_val',
                acl=None,
                ephemeral=True,
                sequence=True,
                makepath=True
            )
            mock_obj = mock.Mock()
            zk.create(
                '/kafka/consumers/some_group/offsets',
                value='some_val',
                acl=mock_obj,
            )

            call_list = [
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    '', None, False, False, False
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    'some_val', None, True, True, True
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    'some_val', mock_obj, False, False, False
                ),
            ]
            assert mock_client.return_value.create.call_args_list == call_list

    def test_delete(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.delete(
                '/kafka/consumers/some_group/offsets',
            )
            zk.delete(
                '/kafka/consumers/some_group/offsets',
                recursive=True
            )
            call_list = [
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    recursive=False
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    recursive=True
                ),
            ]
            assert mock_client.return_value.delete.call_args_list == call_list

    def test_delete_topic_partitions(self, mock_client):
        with mock.patch.object(
            ZK,
            'delete',
            autospec=True
        ) as mock_delete:
            with ZK(self.cluster_config) as zk:
                zk.delete_topic_partitions(
                    'some_group',
                    'some_topic',
                    [0, 1, 2]
                )
                call_list = [
                    mock.call(
                        zk,
                        '/consumers/some_group/offsets/some_topic/0'
                    ),
                    mock.call(
                        zk,
                        '/consumers/some_group/offsets/some_topic/1'
                    ),
                    mock.call(
                        zk,
                        '/consumers/some_group/offsets/some_topic/2'
                    ),
                ]
                assert mock_delete.call_args_list == call_list

    def test_delete_topic(self, _):
        with mock.patch.object(
            ZK,
            'delete',
            autospec=True
        ) as mock_delete:
            with ZK(self.cluster_config) as zk:
                zk.delete_topic(
                    'some_group',
                    'some_topic',
                )
                mock_delete.assert_called_once_with(
                    zk,
                    '/consumers/some_group/offsets/some_topic',
                )

    def test_get_my_subscribed_partitions(self, _):
        with mock.patch.object(
            ZK,
            'get_children',
            autospec=True,
        ) as mock_children:
            with ZK(self.cluster_config) as zk:
                zk.get_my_subscribed_partitions(
                    'some_group',
                    'some_topic',
                )
                mock_children.assert_called_once_with(
                    zk,
                    '/consumers/some_group/offsets/some_topic',
                )
