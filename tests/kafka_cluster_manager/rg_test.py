from mock import sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup


class TestReplicationGroup(object):

    # Initial broker-set empty
    def test_add_broker_empty(self):
        rg = ReplicationGroup('test_rg', None)
        rg.add_broker(sentinel.broker)
        expected = [sentinel.broker]
        actual = rg.brokers

        assert expected == actual

    def test_add_broker(self):
        rg = ReplicationGroup(
            'test_rg',
            [sentinel.broker1, sentinel.broker2],
        )
        rg.add_broker(sentinel.broker)
        expected = sentinel.broker
        actual = rg.brokers

        assert expected in actual

    def test_id(self):
        rg = ReplicationGroup('test_rg', None)
        expected = 'test_rg'
        actual = rg.id

        assert expected == actual

    def test_partitions(self):
        mock_brokers = [sentinel.broker1, sentinel.broker2]
        rg = ReplicationGroup(
            'test_rg',
            mock_brokers,
        )
        sentinel.broker1.partitions = ['p1', 'p2']
        sentinel.broker2.partitions = ['p3']
        expected = [broker.partitions for broker in mock_brokers]
        actual = rg.partitions

        assert expected == actual

    def test_brokers(self):
        rg = ReplicationGroup(
            'test_rg',
            [sentinel.broker1, sentinel.broker2],
        )
        expected = [sentinel.broker1, sentinel.broker2]
        actual = rg.brokers

        assert expected == actual
