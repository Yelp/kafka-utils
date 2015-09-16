from pytest import fixture
from mock import Mock, sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.topic import Topic


class TestBroker(object):

    @fixture
    def partition(self):
        mock_topic = Mock(spec=Topic, id='t1')
        return Partition(mock_topic, 0)

    # Initial broker-set empty
    def test_partitions_empty(self):
        broker = Broker('test-broker')

        assert broker.partitions == set()

    def test_partitions(self):
        broker = Broker('test-broker', set([sentinel.p1, sentinel.p2]))

        assert broker.partitions == set([sentinel.p1, sentinel.p2])

    def test_remove_partition(self, partition):
        p1 = partition
        b1 = Broker('test-broker', set([p1]))
        p1.add_replica(b1)

        # Remove partition
        b1.remove_partition(p1)

        assert p1 not in b1.partitions
        assert b1 not in p1.replicas

    def test_add_partition(self):
        broker = Broker('test-broker', set([sentinel.p1]))
        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[])
        broker.add_partition(p2)

        assert broker.partitions == set([sentinel.p1, p2])
        assert p2.replicas == [broker]

    def test_topics(self):
        partitions = set([
            Mock(spec=Partition, topic=sentinel.t1),
            Mock(spec=Partition, topic=sentinel.t1),
            Mock(spec=Partition, topic=sentinel.t2),
        ])
        broker = Broker('test-broker', partitions)

        assert broker.topics == set([sentinel.t1, sentinel.t2])

    def test_count_partition(self):
        t1 = sentinel.t1
        partitions = set([
            Mock(spec=Partition, topic=t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t2, replicas=sentinel.r1),
        ])
        broker = Broker('test-broker', partitions)

        assert broker.count_partitions(t1) == 2
        assert broker.count_partitions(sentinel.t3) == 0

    def test_move_partition(self, partition):
        victim_partition = partition
        source_broker = Broker('b1', set([victim_partition]))
        victim_partition.add_replica(source_broker)
        dest_broker = Broker('b2')

        # Move partition
        source_broker.move_partition(victim_partition, dest_broker)

        # Verify partition moved from source to destination broker
        assert victim_partition not in source_broker.partitions
        assert victim_partition in dest_broker.partitions

    def test_count_preferred_replica(self, partition):
        p1 = partition
        b1 = Broker('test-broker', set([p1]))
        p1.add_replica(b1)
        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[sentinel.b2])
        b1.add_partition(p2)

        assert b1.count_preferred_replica() == 1
