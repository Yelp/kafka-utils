from mock import Mock, sentinel
from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
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
        # Verify that victim-partition-replicas has replaced
        # source-broker to dest-broker
        assert source_broker not in victim_partition.replicas
        assert dest_broker in victim_partition.replicas

    def test_count_preferred_replica(self, partition):
        p1 = partition
        b1 = Broker('test-broker', set([p1]))
        p1.add_replica(b1)
        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[sentinel.b2])
        b1.add_partition(p2)

        assert b1.count_preferred_replica() == 1

    def test_get_preferred_partition(self):
        # Create topics, partitions, brokers
        t1, t1.id, t2, t2.id = sentinel.t1, 't1', sentinel.t2, 't2'
        p1, p2 = Partition(t1, 0), Partition(t1, 0)
        p3, p4 = Partition(t2, 0), Partition(t2, 0)
        # Brokers
        b1, b2 = Broker('b1', set([p1, p2, p3])), Broker('b2', set([p2]))

        # Case:1  No such partition due to presence of all replicas
        b3 = Broker('b3', set([p2, p4]))
        pref_partition = b2.get_preferred_partition(b3)

        # Verify that pref-partition is None since b3 has only p2
        # and has its replica in b2
        assert pref_partition is None

        # Case:2 Multiple solutions
        pref_partition = b1.get_preferred_partition(b3)

        # Verify preferred partition for source-broker b1 to be sent to
        # destination broker-b3
        # Cannot be p2 since b3 has replica
        # Both p1 and p3 has 1 sibling p2, p4 respectively from topic t1, t2.
        assert pref_partition in [p1, p3]

        # Case:3: Unique solution
        pref_partition = b1.get_preferred_partition(b2)

        # Verify preferred-partition from source-broker b1
        # p2: not possible since its replica is in destination broker b2
        # Between p1 and p3: p1 has 1 sibling (p2) in b2 and p3 has 1 sibling
        # So preferred partition should be p3
        assert pref_partition == p3
