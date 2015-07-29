from mock import Mock, sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker


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
        mock_brokers = [
            Mock(
                spec=Broker,
                partitions=[sentinel.partition1, sentinel.partition2],
            ),
            Mock(spec=Broker, partitions=[sentinel.partition3]),
        ]
        rg = ReplicationGroup(
            'test_rg',
            mock_brokers,
        )
        expected = [
            sentinel.partition1,
            sentinel.partition2,
            sentinel.partition3,
        ]
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
