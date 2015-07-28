import mock

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker


class TestReplicationGroup(object):

    def mock_brokers(self, name_list):
        mock_brokers = []
        for name in name_list:
            p1 = 'p1_{0}'.format(name)
            p2 = 'p2_{0}'.format(name)
            partitions = [p1, p2]
            mock_brokers.append(mock.Mock(Broker(name, partitions)))
        return mock_brokers

        mock_brokers = mock.Mock()
        return mock_brokers

    # Initial broker-set empty
    def test_add_broker_empty(self):
        rg = ReplicationGroup('test_rg1', None)
        new_mock_broker = mock.Mock(Broker("new_broker", ['p1', 'p2']))
        rg.add_broker(new_mock_broker)
        expected_broker = [new_mock_broker]

        assert expected_broker == rg.brokers

    def test_add_broker(self):
        mock_brokers = self.mock_brokers(['b1', 'b2'])
        rg = ReplicationGroup('test_rg1', mock_brokers)
        new_mock_broker = self.mock_brokers(['new-broker'])
        rg.add_broker(new_mock_broker)
        new_mock_brokers = rg.brokers

        assert new_mock_broker in new_mock_brokers

    def test_id(self):
        rg = ReplicationGroup('test_rg1', None)
        expected = 'test_rg1'
        actual = rg.id

        assert expected == actual

    def test_partitions(self):
        mock_brokers = self.mock_brokers(['b1', 'b2'])
        rg = ReplicationGroup('test_rg1', mock_brokers)
        expected = [broker.partitions for broker in mock_brokers]
        actual = rg.partitions

        assert expected == actual

    def test_brokers(self):
        mock_brokers = self.mock_brokers(['b1', 'b2'])
        rg = ReplicationGroup('test_rg1', mock_brokers)
        expected = mock_brokers
        actual = rg.brokers

        assert expected == actual
