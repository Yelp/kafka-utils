from mock import Mock, sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker


class TestBroker(object):
    # Initial broker-set empty
    def test_partitions_empty(self):
        broker = Broker('test-broker')
        assert broker.partitions == set()

    def test_partitions(self):
        broker = Broker('test-broker', [sentinel.p1, sentinel.p2])
        assert broker.partitions == [sentinel.p1, sentinel.p2]

    def test_remove_partition(self):
        p1 = Partition(('p1', 0), topic=sentinel.t1)
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
        partitions = [
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t2, replicas=sentinel.r1),
        ]
        broker = Broker('test-broker', partitions)
        assert broker.topics == set([sentinel.t1, sentinel.t2])

    def test_partition_count(self):
        topic = sentinel.t1
        partitions = [
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t2, replicas=sentinel.r1),
        ]
        broker = Broker('test-broker', partitions)
        assert broker.count_partitions(topic) == 2
        assert broker.count_partitions(sentinel.t3) == 0

    def test_move_partition(self):
        victim_partition = Partition(('p1', 0), topic=sentinel.t1)
        source_broker = Broker('b1', set([victim_partition]))
        victim_partition.add_replica(source_broker)
        dest_broker = Broker('b2')

        source_broker.move_partition(victim_partition, dest_broker)
        assert victim_partition not in source_broker.partitions
        assert victim_partition in dest_broker.partitions

    def test_count_partitions(self):
        # Prepare broker
        p1 = Partition(('p1', 0), topic=sentinel.t1)
        b1 = Broker('test-broker', set([p1]))
        p1.add_replica(b1)

        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[])
        b1.add_partition(p2)
        assert b1.count_partitions(sentinel.t1) == 2
        assert b1.count_partitions(sentinel.t2) == 0

    def test_count_preferred_replica(self):
        p1 = Partition(('p1', 0), topic=sentinel.t1)
        b1 = Broker('test-broker', set([p1]))
        p1.add_replica(b1)
        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[])
        b1.add_partition(p2)

        assert b1.count_preferred_replica() == 1
