from mock import Mock, sentinel, MagicMock

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker


class TestBroker(object):

    # Initial broker-set empty
    def test_partitions_empty(self):
        broker = Broker('test-broker')
        assert broker.partitions == []

    def test_partitions(self):
        broker = Broker('test-broker', [sentinel.p1, sentinel.p2])
        assert broker.partitions == [sentinel.p1, sentinel.p2]

    def test_remove_partition(self):
        broker = Broker('test-broker', [sentinel.p1, sentinel.p2])
        broker.remove_partition(sentinel.p1)
        assert sentinel.p1 not in broker.partitions

    def test_add_partition(self):
        broker = Broker('test-broker', [sentinel.p1])
        broker.add_partition(sentinel.p2)
        assert broker.partitions == [sentinel.p1, sentinel.p2]

    def test_topics(self):
        partitions = [
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t2, replicas=sentinel.r1),
        ]
        broker = Broker('test-broker', partitions)
        assert broker.topics == set([sentinel.t1, sentinel.t2])
