from mock import Mock, sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.topic import Topic


class TestReplicationGroup(object):

    def create_partition(self, t_id='t1', p_id=0):
        mock_topic = Mock(spec=Topic, id=t_id)
        return Partition(mock_topic, p_id)

    # Initial broker-set empty
    def test_add_broker_empty(self):
        rg = ReplicationGroup('test_rg', None)
        rg.add_broker(sentinel.broker)

        assert set([sentinel.broker]) == rg.brokers

    def test_add_broker(self):
        rg = ReplicationGroup(
            'test_rg',
            set([sentinel.broker1, sentinel.broker2]),
        )
        rg.add_broker(sentinel.broker)

        assert sentinel.broker in rg.brokers

    def test_id(self):
        rg = ReplicationGroup('test_rg', None)

        assert 'test_rg' == rg.id

    def test_partitions(self):
        mock_brokers = [
            Mock(
                spec=Broker,
                partitions=set([sentinel.p1, sentinel.p2]),
            ),
            Mock(spec=Broker, partitions=set([sentinel.p3, sentinel.p1])),
        ]
        rg = ReplicationGroup('test_rg', mock_brokers)
        expected = [
            sentinel.p1,
            sentinel.p2,
            sentinel.p3,
            sentinel.p1,
        ]

        assert sorted(expected) == sorted(rg.partitions)

    def test_brokers(self):
        rg = ReplicationGroup(
            'test_rg',
            set([sentinel.broker1, sentinel.broker2]),
        )
        expected = set([sentinel.broker1, sentinel.broker2])

        assert expected == rg.brokers

    def test_elect_source_broker(self):
        over_loaded_brokers = [sentinel.broker1, sentinel.broker2]
        rg = ReplicationGroup(
            'test_rg',
            set([sentinel.broker1, sentinel.broker2]),
        )
        actual = rg._elect_source_broker(over_loaded_brokers)
        assert actual == sentinel.broker1

    def test_elect_dest_broker(self):
        # Creating 2 partitions with topic:t1 for broker: b1
        p1 = self.create_partition('t1', 0)
        p2 = self.create_partition('t2', 0)
        p3 = self.create_partition('t1', 1)
        b1 = Broker('b1', set([p1, p2, p3]))

        # Creating 1 partition with topic:t2 for broker: b2
        # and 1 partition with topic:t3 for broker:b2
        p4 = self.create_partition('t1', 0)
        p5 = self.create_partition('t3', 0)
        b2 = Broker('b2', set([p4, p5]))

        # Creating replication-group with above brokers
        rg = ReplicationGroup('test_rg', set([b1, b2, sentinel.b3]))

        under_loaded_brokers = [b1, b2]
        # Since p5.topic is t3 and b1 doesn't have any partition with
        # topic: t3 but b2 has it, preferred destination should be 'b1'
        victim_partition = p5
        actual = rg._elect_dest_broker(under_loaded_brokers, victim_partition)
        assert actual == b1

        # Since p1.topic is t1 and b2 has lesser partitions with topic t1
        # (i.e. 1) compared to 2-partitions in b1 with topic:t1
        # preferred destination should be 'b2'
        victim_partition = p1
        actual = rg._elect_dest_broker(under_loaded_brokers, victim_partition)
        assert actual == b2

    def test_select_under_loaded_brokers(self):
        # Create brokers with different partition-count
        b1 = Broker('b1', set([sentinel.p1, sentinel.p2, sentinel.p3]))
        b2 = Broker('b2', set([sentinel.p4, sentinel.p5]))
        b3 = Broker('b3', set([sentinel.p5]))

        # Creating replication-group with above brokers
        rg = ReplicationGroup('test_rg', set([b1, b2, b3]))

        # under-loaded-brokers SHOULD NOT contain victim-partition p7
        # Remaining brokers are returned in sorted-order
        victim_partition = sentinel.p7
        actual = rg._select_under_loaded_brokers(victim_partition)
        assert actual == [b3, b2, b1]

        # under-loaded-brokers SHOULD NOT contain victim-partition sentinel.p4
        # Brokers returned in sorted order of DECREASING partition-count
        victim_partition = sentinel.p4
        actual = rg._select_under_loaded_brokers(victim_partition)
        assert actual == [b3, b1]

    def test_select_over_loaded_brokers(self):
        # Create brokers with different partition-count
        b1 = Broker('b1', set([sentinel.p1, sentinel.p2, sentinel.p3]))
        b2 = Broker('b2', set([sentinel.p4, sentinel.p5]))
        b3 = Broker('b3', set([sentinel.p5]))

        # Creating replication-group with above brokers
        rg = ReplicationGroup('test_rg', set([b1, b2, b3]))

        # over-loaded-brokers should contain victim-partition p5
        # Broker-list returned in sorted order of INCREASING partition-count
        victim_partition = sentinel.p5
        actual = rg._select_over_loaded_brokers(victim_partition)
        assert actual == [b2, b3]

    def test_count_replica(self):
        # Create broker with 3 partitions
        b1 = Broker('b1', set([sentinel.p1, sentinel.p2, sentinel.p3]))

        # Create broker with 2 partitions
        b2 = Broker('b2', set([sentinel.p1, sentinel.p5]))

        # Creating replication-group with above brokers
        rg = ReplicationGroup('test_rg', set([b1, b2]))

        assert rg.count_replica(sentinel.p1) == 2
        assert rg.count_replica(sentinel.p5) == 1
        assert rg.count_replica(sentinel.p6) == 0

    def test_move_partition(self):
        # Create sample partitions
        p1 = self.create_partition('t1', 0)
        p2 = self.create_partition('t2', 0)
        p3 = self.create_partition('t1', 1)

        # Create 4 brokers
        b1 = Broker('b1', set([p1, p2, p3]))
        b2 = Broker('b2', set([p1, p3]))
        b3 = Broker('b3', set([p1, p3]))
        b4 = Broker('b4', set([p2]))

        # Update partition-replicas
        p1.add_replica(b1)
        p1.add_replica(b2)
        p2.add_replica(b1)
        p2.add_replica(b2)

        rg_source = ReplicationGroup('rg1', set([b1, b2]))
        rg_dest = ReplicationGroup('rg2', set([b3, b4]))

        old_p1_count_rg_source = rg_source.partitions.count(p1)
        old_p1_count_rg_dest = rg_dest.partitions.count(p1)
        old_p2_count_rg_source = rg_source.partitions.count(p2)
        old_p2_count_rg_dest = rg_dest.partitions.count(p2)
        old_p3_count_rg_source = rg_source.partitions.count(p3)
        old_p3_count_rg_dest = rg_dest.partitions.count(p3)
        # Move partition p1 from rg1 to rg2
        rg_source.move_partition(rg_dest, p1)
        new_p1_count_rg_source = rg_source.partitions.count(p1)
        new_p1_count_rg_dest = rg_dest.partitions.count(p1)

        # partition-count of p1 for rg1 should reduce by 1
        assert new_p1_count_rg_source == old_p1_count_rg_source - 1

        # partition-count of p1 for rg2 should increase by 1
        assert new_p1_count_rg_dest == old_p1_count_rg_dest + 1

        # Rest of the partitions are untouched
        assert old_p2_count_rg_source == rg_source.partitions.count(p2)
        assert old_p2_count_rg_dest == rg_dest.partitions.count(p2)
        assert old_p3_count_rg_source == rg_source.partitions.count(p3)
        assert old_p3_count_rg_dest == rg_dest.partitions.count(p3)

    def test_select_broker(self):
        # Tests whether source and destination broker are best match
        # Create sample partitions
        p1 = self.create_partition('t1', 0)
        p2 = self.create_partition('t2', 0)
        p3 = self.create_partition('t1', 1)

        # Create brokers for rg-source
        b0 = Broker('b0', set([p1, p2, p3]))
        b1 = Broker('b1', set([p1, p2]))
        b2 = Broker('b2', set([p2, p3]))
        b7 = Broker('b2', set([p1, p2, p3]))
        # Create source-replication-group with brokers b1, b2, b0, b7
        rg_source = ReplicationGroup('rg1', set([b0, b1, b2, b7]))

        # Create brokers for rg-dest
        b3 = Broker('b3', set([p2, p3]))
        b4 = Broker('b4', set([p2]))
        b5 = Broker('b5', set([p1]))
        b6 = Broker('b6', set([p3]))
        # Create dest-replication-group with brokers b3, b4, b5, b6
        rg_dest = ReplicationGroup('rg2', set([b3, b4, b5, b6]))

        # Select best-suitable brokers for moving partition 'p1'
        broker_source, broker_dest = rg_source._select_broker_pair(rg_dest, p1)

        # source-broker can't be b2 since it doesn't have p1
        # source-broker can't be b3, b4, b5, b6 since those don't belong to rg-source
        # source-broker shouldn't be b1 since it has lesser partitions than b7 and b0
        assert broker_source in [b0, b7]

        # dest-broker can't be b1 or b2 since those don't belong to rg-dest
        # dest-broker shouldn't be b3 since it has more partitions than b4
        # dest-broker can't be b5 since it has victim-partition p1
        assert broker_dest in [b4, b6]
