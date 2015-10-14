from collections import OrderedDict
from mock import Mock, sentinel

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.broker import Broker
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.rg import ReplicationGroup
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.topic import Topic
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import (
    get_partition_imbalance_stats,
    calculate_partition_movement,
)
from .cluster_topology_test import TestClusterToplogy as CT


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
            Mock(spec=Broker, partitions=set([sentinel.p1, sentinel.p2])),
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

        victim_partition = sentinel.p7
        actual = rg._select_under_loaded_brokers(victim_partition)
        # Since sentinel.p7 is not present in b1, b2 and b3, they should be
        # returned in increasing order of partition-count
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
        p1.add_replica(b3)
        p2.add_replica(b1)
        p2.add_replica(b2)
        p3.add_replica(b1)
        p3.add_replica(b2)
        p3.add_replica(b3)

        rg_source = ReplicationGroup('rg1', set([b1, b2]))
        rg_dest = ReplicationGroup('rg2', set([b3, b4]))

        # Move partition p1 from rg1 to rg2
        rg_source.move_partition(rg_dest, p1)

        # partition-count of p1 for rg1 should reduce by 1
        assert rg_source.partitions.count(p1) == 1

        # partition-count of p1 for rg2 should increase by 1
        assert rg_dest.partitions.count(p1) == 2

        # Rest of the partitions are untouched
        assert rg_source.partitions.count(p2) == 1
        assert rg_dest.partitions.count(p2) == 1
        assert rg_source.partitions.count(p3) == 2
        assert rg_dest.partitions.count(p3) == 1

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

    # Test brokers-rebalancing
    def test_get_target_brokers_case1(self):
        # Partition-selection decision
        # rg1: 0,1; rg2: 2, 3
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 1, 2]),
                ((u'T1', 1), [0, 3]),
                ((u'T2', 0), [2]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(5)) as ct:
            rg1 = ct.rgs['rg1']
            over_loaded = [ct.brokers[0]]
            under_loaded = [ct.brokers[1]]
            b_source, b_dest, victim_partition = \
                rg1._get_target_brokers(over_loaded, under_loaded)

            # Assert source-broker is 0 and destination-broker is 1
            assert b_source.id == 0
            assert b_dest.id == 1
            # Partition can't be T1, 0, since it already has replica on broker 1
            # Partition shouldn't be (T1, 1) since it has its sibling (T1, 0)
            # at destination broker 1
            # So ideal-partition should be (T0, 0)
            assert victim_partition.name == ('T0', 0)

    def test_get_target_brokers_case2(self):
        # Source broker selection decision
        # Partition-count plays role
        # Helps in minimum movements
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 1]),
                ((u'T1', 0), [0, 1, 2]),
                ((u'T1', 1), [0]),
                ((u'T1', 2), [4]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, ['0', '1', '2', '4']) as ct:
            rg1 = ct.rgs['rg1']
            over_loaded = [ct.brokers[1], ct.brokers[0]]
            under_loaded = [ct.brokers[4]]
            b_source, b_dest, victim_partition = \
                rg1._get_target_brokers(over_loaded, under_loaded)

            # Verify destination-broker is 4
            assert b_dest.id == 4
            # Here both 0 and 1 equal minimum siblings in broker 4 (0)
            # Both 0 and 1 could be selected as source broker, but since
            # broker 0 has more partitions (3 > 2) than 1, broker 0 should be
            # selected as source-broker.
            assert b_source.id == 0
            # Only partition with no siblings in 4 is (T0, 0)
            assert victim_partition.name == ('T0', 0)

    def test_get_target_brokers_case3(self):
        # Source broker selection decision
        # Minimum-sibling count gets preference
        # rg-groups: rg1: brokers:(0, 1, 4)
        # Broker-partition cnt: opt-partition-cnt: 10/3: 3, extra: 1
        # Over-loaded: 0:5, 1:4
        # Under-loaded: 4:1
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 1]),
                ((u'T0', 1), [0, 4]),
                ((u'T0', 2), [0, 1]),
                ((u'T0', 3), [0]),
                ((u'T0', 4), [0, 1]),
                ((u'T3', 1), [1]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, ['0', '1', '4']) as ct:
            rg1 = ct.rgs['rg1']
            over_loaded = [ct.brokers[0], ct.brokers[1]]
            under_loaded = [ct.brokers[4]]
            b_source, b_dest, victim_partition = \
                rg1._get_target_brokers(over_loaded, under_loaded)

            # Verify destination broker is 4 (only option)
            assert b_dest.id == 4
            # Note: Even though broker 0 has more partitions(5) than
            # broker 1 (4), but broker 1 will be selected as source-broker
            # since it has lesser siblings in destination-broker 4
            # Sibling of broker 0 in broker 4: 1 (for every partition: of topic T0)
            # Siblings of broker 1 in broker 4: 0 (for partition: (T3, 1)
            # Verify source-broker is 1
            assert b_source.id == 1
            # Verify partition be (T3, 1) with minimum-siblings
            assert victim_partition.name == ('T3', 1)

    def test_rebalance_brokers_balanced_1(self):
        # Single replication-group
        # Group: map(broker, p-count)
        # rg1:  (0: 2, 1:2, 4:2)
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 4]),
                ((u'T1', 0), [0, 1]),
                ((u'T2', 1), [1]),
                ((u'T3', 0), [0, 1, 4]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, ['0', '1', '4']) as ct:
            ct.rebalance_brokers()

            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 0
            # Verify no change is assignment
            assert sorted(ct.assignment) == sorted(ct.initial_assignment)

    def test_rebalance_brokers_balanced_2(self):
        # 2 replication-groups are balanced individually
        # and overall balanced as well
        # Rg-Group: map(broker, p-count)
        # rg1:      (0: 2, 1:2)
        # rg2:      (2: 1)
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 1]),
                ((u'T1', 1), [1]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(3)) as ct:
            ct.rebalance_brokers()

            # Verify no change is assignment
            assert sorted(ct.assignment) == sorted(ct.initial_assignment)
            # Verify overall-imbalance is 0
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 0
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0

    def test_rebalance_brokers_balanced_3(self):
        # 2 replication-groups are in balanced state individually
        # but overall imbalanced.
        # Rg-Group: map(broker, p-count)
        # rg1:      (0: 2, 1:2); rg2:      (2: 1, 3:0)
        # opt-overall: 5/4 ==> 1, extra-overall: 1
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 1]),
                ((u'T1', 1), [1]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(4)) as ct:
            ct.rebalance_brokers()

            # Verify no change is assignment
            assert sorted(ct.assignment) == sorted(ct.initial_assignment)
            # Verify overall-imbalance is NOT 0, since we don't move partitions
            # across rg's, to balance brokers
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 1
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0

    def test_rebalance_brokers_imbalanced_1(self):
        # 1 rg is balanced, 2nd imbalanced
        # Result: Overall-balanced and individually-balanced
        # rg1: (0: 3, 1:1); rg2: (2: 1)
        # opt-overall: 5/3 ==> 1, extra-overall: 2
        # opt-rg1: 4/2 ==> 2, extra-rg1: 0
        # ==> 0, 1 should have 2 partitions each
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 1]),
                ((u'T1', 1), [0]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(3)) as ct:
            ct.rebalance_brokers()

            # Verify partition-count of 0 and 1 as equal to 2
            assert len(ct.brokers[0].partitions) == 2
            assert len(ct.brokers[1].partitions) == 2
            # Verify overall-imbalance is 0
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 0
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0

    def test_rebalance_brokers_imbalanced_2(self):
        # 2-rg's: Both rg's imbalanced
        # Result: rgs' balanced individually and overall
        # rg1: (0: 3, 1:1); rg2: (2: 2, 3:0)
        # opt-overall: 6/4 ==> 1, extra-overall: 2
        # opt-rg1: 4/2 ==> 2, extra-rg1: 0
        # opt-rg2: 2/2 ==> 1, extra-rg1: 0
        # ==> 0, 1 should have 2 partitions each
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 1]),
                ((u'T1', 1), [0, 2]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(4)) as ct:
            ct.rebalance_brokers()

            # rg1: Verify partition-count of 0 and 1 as equal to 2
            assert len(ct.brokers[0].partitions) == 2
            assert len(ct.brokers[1].partitions) == 2
            # rg2: Verify partition-count of 2 and 3 as equal to 1
            assert len(ct.brokers[2].partitions) == 1
            assert len(ct.brokers[3].partitions) == 1
            # Verify overall-imbalance is 0
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 0
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0

    def test_rebalance_brokers_imbalanced_3(self):
        # 2-rg's: Both rg's imbalanced
        # Result: rgs' balanced individually but NOT overall
        # rg1: (0: 4, 1:2); rg2: (2: 2, 3:0)
        # opt-overall: 8/4 ==> 2, extra-overall: 0
        # opt-rg1: 6/2 ==> 3, extra-rg1: 0
        # opt-rg2: 2/2 ==> 1, extra-rg1: 0
        # ==> 0, 1 should have 3 partitions each
        # ==> 2, 3 should have 1 partition each
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2, 1]),
                ((u'T1', 0), [0, 1]),
                ((u'T1', 1), [0, 2]),
                ((u'T1', 2), [0]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(4)) as ct:
            ct.rebalance_brokers()

            # rg1: Verify partition-count of 0 and 1 as equal to 3
            assert len(ct.brokers[0].partitions) == 3
            assert len(ct.brokers[1].partitions) == 3
            # rg2: Verify partition-count of 2 and 3 as equal to 1
            assert len(ct.brokers[2].partitions) == 1
            assert len(ct.brokers[3].partitions) == 1
            # Verify overall-imbalance is NON-zero, since we don't move partitions
            # across replication-groups
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 2
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0

    def test_rebalance_brokers_imbalanced_4(self):
        # Test minimum-movements
        # 3-rg's: 2 rg's imbalanced, 1 rg is balanced
        # Result: All 3 rgs' balanced
        # rg1: (0: 3, 1:0, 4:2); rg2: (2: 2, 3:0); rg3: (4:1)
        # For minimum movements partition from 0 should move to 1
        test_ct = CT()
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T1', 0), [0, 4]),
                ((u'T1', 1), [4, 2]),
                ((u'T1', 2), [0, 5]),
            ]
        )
        with test_ct.build_cluster_topology(assignment, test_ct.srange(6)) as ct:
            # Re-balance each replication-group separately
            ct.rgs['rg1'].rebalance_brokers()
            ct.rgs['rg2'].rebalance_brokers()
            ct.rgs['rg3'].rebalance_brokers()

            # rg1: Verify partition-count of 0:2, 1:1, 4:2
            assert len(ct.brokers[0].partitions) == 2
            assert len(ct.brokers[1].partitions) == 1
            assert len(ct.brokers[4].partitions) == 2
            # rg2: Verify partition-count of 2:1, 3:1
            assert len(ct.brokers[2].partitions) == 1
            assert len(ct.brokers[3].partitions) == 1
            # rg3: Verify partition-count of 5 as equal to 1
            assert len(ct.brokers[5].partitions) == 1
            # Verify overall imbalance is also 0
            _, net_imbalance, _ = get_partition_imbalance_stats(ct.brokers.values())
            assert net_imbalance == 0
            # Verify  rg1 is balanced
            _, net_rg1, _ = get_partition_imbalance_stats(ct.rgs['rg1'].brokers)
            assert net_rg1 == 0
            # Verify  rg2 is balanced
            _, net_rg2, _ = get_partition_imbalance_stats(ct.rgs['rg2'].brokers)
            assert net_rg2 == 0
            # Verify  rg3 is balanced
            _, net_rg3, _ = get_partition_imbalance_stats(ct.rgs['rg3'].brokers)
            assert net_rg3 == 0
            # Assert Min-partition movements as previous net-imbalance for each
            # group which is 2 (1 for rg1 and 1 for rg2)
            _, total_movements = \
                calculate_partition_movement(ct.initial_assignment, ct.assignment)
            assert total_movements == 2
