from mock import sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker


class TestReplicationGroup(object):

    # Initial broker-set empty
    def test_add_broker_empty(self):
        rg = ReplicationGroup('test_rg1', None)
        # new_mock_broker = mock.Mock(Broker("new_broker", ['p1', 'p2']))
        # rg.add_broker(new_mock_broker)
        rg.add_broker(sentinel.broker)
        expected = [sentinel.broker]
        actual = rg.brokers

        assert expected == actual

    def test_add_broker(self):
        rg = ReplicationGroup(
            'test_rg1',
            [sentinel.broker1, sentinel.broker2],
        )
        # new_mock_broker = self.mock_brokers(['new-broker'])
        # rg.add_broker(new_mock_broker)
        rg.add_broker(sentinel.broker)
        expected = sentinel.broker
        actual = rg.brokers

        assert expected in actual

    def test_id(self):
        rg = ReplicationGroup('test_rg1', None)
        expected = 'test_rg1'
        actual = rg.id

        assert expected == actual

    def test_partitions(self):
        mock_brokers = [sentinel.broker1, sentinel.broker2],
        rg = ReplicationGroup(
            'test_rg1',
            mock_brokers,
        )
        sentinel.broker1.partitions = ['p1', 'p2']
        sentinel.broker2.partitions = ['p3', 'p4']
        expected = ['p1']
        # [broker.partitions for broker in mock_brokers]
        actual = rg.partitions

        assert expected == actual

    def test_brokers(self):
        rg = ReplicationGroup(
            'test_rg1',
            [sentinel.broker1, sentinel.broker2],
        )
        expected = [sentinel.broker1, sentinel.broker2]
        actual = rg.brokers

        assert expected == actual
