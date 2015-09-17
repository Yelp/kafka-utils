import contextlib
from collections import Counter, OrderedDict
from mock import sentinel, patch, MagicMock
from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import (
    ClusterTopology,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import (
    get_replication_group_imbalance_stats,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    compute_optimal_count,
)
from yelp_kafka_tool.kafka_cluster_manager.main import ZK
from yelp_kafka.config import ClusterConfig


class TestClusterToplogy(object):
    broker_rg = {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3', 6: 'rg4'}
    topic_ids = ['T0', 'T1', 'T2', 'T3']
    brokers_info = {
        '0': sentinel.obj1,
        '1': sentinel.obj2,
        '2': sentinel.obj3,
        '3': sentinel.obj4,
        '4': sentinel.obj5,
    }
    '''
    # Example assignment properties:
    * Brokers:(0,1,2,3): rg-count = 2
    # case 1: repl-factor % rg-count == 0
        -- T0, T1:
        * 1a) T1: repl-factor > rg-count
    # case 2: repl-factor % rg-count != 0
        -- T2, T3
        * 2a): repl-factor > rg-count: T1
        * 2b): repl-factor < rg-count: T2

    rg-imbalance-status per partition:
    rg-imbalanced-partitions: T0-1, T1-1, T3-1
    rg-balanced-partitions:   T0-0, T1-0, T3-0, T2-0
    '''
    _initial_assignment = OrderedDict(
        [
            ((u'T0', 0), [1, 2]),
            ((u'T0', 1), [2, 3]),
            ((u'T1', 0), [0, 1, 2, 3]),
            ((u'T1', 1), [0, 1, 2, 4]),
            ((u'T2', 0), [2]),
            ((u'T3', 0), [0, 1, 2]),
            ((u'T3', 1), [0, 1, 4]),
        ]
    )

    @fixture
    def mock_zk(self):
        # Mock zookeeper
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        mock_zk.get_brokers.return_value = self.brokers_info
        mock_zk.get_topics.return_value = self.topic_ids
        return mock_zk

    @contextlib.contextmanager
    def mock_get_replication_group_id(self):
        with patch.object(
            ClusterTopology,
            "_get_replication_group_id",
            spec=ClusterTopology._get_replication_group_id,
            side_effect=self.get_replication_group_id,
        ) as mock_get_replication_group_id:
            yield mock_get_replication_group_id

    def get_replication_group_id(self, broker):
        return self.broker_rg[broker.id]

    @contextlib.contextmanager
    def build_cluster_topology(self, assignment=None, broker_ids=None):
        """Create cluster topology from given assignment."""
        if not assignment:
            assignment = self._initial_assignment
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        if broker_ids:
            brokers_info = {broker_id: sentinel.obj for broker_id in broker_ids}
        else:
            brokers_info = self.brokers_info
        topic_ids = sorted(set([t_p[0] for t_p in assignment.iterkeys()]))
        mock_zk.get_brokers.return_value = brokers_info
        mock_zk.get_topics.return_value = topic_ids
        with contextlib.nested(
            patch(
                "yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology.KafkaInterface",
                autospec=True,
            ),
            self.mock_get_replication_group_id(),
        ) as (mock_kafka, mock_rg_groups):
            # Create cluster-object
            mock_kafka.return_value.get_cluster_assignment.return_value = assignment
            yield ClusterTopology(mock_zk)

    def assert_valid(self, new_assignment, orig_assignment, orig_brokers):
        """Assert if new-assignment is valid based on given assignment.

        Asserts the results for following parameters:
        a) Asserts that keys in both assignments are same
        b) Asserts that replication-factor of result remains same
        c) Assert that new-replica-brokers are amongst given broker-list
        """
        # Asserts that partitions remain same
        assert(sorted(orig_assignment.keys()) == sorted(new_assignment.keys()))
        for t_p, new_replicas in new_assignment.iteritems():
            orig_replicas = orig_assignment[t_p]
            # Assert that new-replias are amongst given broker-list
            assert(all([broker in orig_brokers for broker in new_replicas]))
            # Assert that replication-factor remains same
            assert(len(new_replicas) == len(orig_replicas))

    def test_rebalance_replication_groups(self):
        with self.build_cluster_topology() as ct:
            ct.rebalance_replication_groups()
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )
            assert(net_imbal, 0)
            self.assert_valid(ct.assignment, self._initial_assignment, ct.brokers.keys())

    def test_rebalance_replication_groups_balanced(self):
        # Replication-group is already balanced
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T0', 1), [0, 3]),
                ((u'T2', 0), [2]),
                ((u'T3', 0), [0, 1, 2]),
            ]
        )
        with self.build_cluster_topology(assignment, self.srange(5)) as ct:
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )
            assert net_imbal == 0
            ct.reassign_partitions()
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )
            assert(net_imbal, 0)
            self.assert_valid(ct.assignment, assignment, ct.brokers.keys())

    def partition_data(self, ct, p_name):
        partition = ct.partitions[p_name]
        opt_cnt, extra_cnt = \
            compute_optimal_count(
                partition.replication_factor,
                len(ct.rgs.values()),
            )
        return partition, opt_cnt, not extra_cnt

    def test_segregate_replication_groups_case1(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 1: repl-factor % rg-count == 0
        """

        with self.build_cluster_topology() as ct:
            # Partition: T0-0
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T0', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_dist,
                )
            # Tests compute-optimal-count
            assert opt_cnt == 1 and evenly_dist is True
            # Tests segregation of rg's
            assert not (over_replicated_rgs or under_replicated_rgs)

            # Partition T0-1: # Already-balanced
            # opt-count == 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T0', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_dist,
                )
            assert opt_cnt == 1 and evenly_dist is True
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert (over_rg_ids == ['rg2']) and (under_rg_ids == ['rg1'])

            # Partition: T1-0 # Already-balanced: repl-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T1', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)
            assert opt_cnt == 2 and evenly_dist is True
            assert not (over_replicated_rgs or under_replicated_rgs)

    def test_segregate_replication_groups_case_2(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 2: repl-factor % rg-count != 0
        """

        with self.build_cluster_topology() as ct:
            # Partition: T2-0: repl-factor < rg-count
            # opt-count:  0
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T2', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            # Tests compute-optimal-count
            assert opt_cnt == 0 and evenly_dist is False
            # Tests segregation of rg's
            assert (over_rg_ids == ['rg2']) and (under_rg_ids == ['rg1'])

            # Partition T3-0: # Already-balanced
            # opt-count: 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T3', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert opt_cnt == 1 and evenly_dist is False
            assert (under_rg_ids == ['rg2'])and over_rg_ids == ['rg1']

            # Partition: T3-1 # not-balanced: repl-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T3', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert opt_cnt == 1 and evenly_dist is False
            assert (under_rg_ids == ['rg2'])and over_rg_ids == ['rg1']

    def test_partition_replicas(self):
        with self.build_cluster_topology() as ct:
            partition_replicas = [p.name for p in ct.partition_replicas]

            # Assert that replica-count for each partition as the value
            # Get dict for count of each partition
            partition_replicas_cnt = Counter(partition_replicas)
            assert partition_replicas_cnt[('T0', 0)] == 2
            assert partition_replicas_cnt[('T0', 1)] == 2
            assert partition_replicas_cnt[('T1', 0)] == 4
            assert partition_replicas_cnt[('T1', 1)] == 4
            assert partition_replicas_cnt[('T2', 0)] == 1
            assert partition_replicas_cnt[('T3', 0)] == 3
            assert partition_replicas_cnt[('T3', 1)] == 3

    def test_assignment(self):
        with self.build_cluster_topology() as ct:

            # Verify if the returned assignment is valid
            assert ct.assignment == self._initial_assignment
            # Assert initial-assignment
            assert ct.initial_assignment == self._initial_assignment

    def test_get_assignment_json(self):
        with self.build_cluster_topology() as ct:
            assignment_json = {'version': 1, 'partitions': []}
            for part, replicas in self._initial_assignment.iteritems():
                p_info = {'topic': part[0], 'partition': part[1], 'replicas': replicas}
                assignment_json['partitions'].append(p_info)

            # Verify newly created .json file
            assert sorted(ct.get_assignment_json()) == sorted(assignment_json)

    def test_elect_source_replication_group(self):
        # Sample assignment with 3 replication groups
        # with replica-count as as per map
        # broker_rg = {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3', 6: 'rg4'}
        # rg-id: (brokers), count
        # rg1: (0, 2, 4) = 3
        # rg2: (1, 3) = 2
        # rg3: (5) = 1
        # rg1 and rg2 are over-replicated and rg3 being under-replicated
        # source-replication-group should be rg1 having the highest replicas
        p1 = ((u'T0', 0), [0, 1, 2, 3, 4, 5, 6])
        assignment = OrderedDict([p1])
        with self.build_cluster_topology(assignment, self.srange(7)) as ct:
            # For partition p1
            p1, opt_cnt, evenly_dist = self.partition_data(ct, ('T0', 0))
            assert opt_cnt == 1  # 7/3
            assert evenly_dist is False  # 7 % 3
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(p1, opt_cnt, evenly_dist)

            # Assert 'rg1' and 'rg2' are over-replicated groups
            over_rg_ids = set([rg.id for rg in over_replicated_rgs])
            assert over_rg_ids == set(['rg1', 'rg2'])

            # Assert 'rg3' and 'rg4' are under-replicated groups
            under_rg_ids = set([rg.id for rg in under_replicated_rgs])
            assert under_rg_ids == set(['rg3', 'rg4'])

            # Get source-replication group
            rg_source = ct._elect_source_replication_group(
                over_replicated_rgs,
                p1,
            )
            # since, 'rg1' as more replicas i.e. 3, it should be selected
            assert 'rg1' == rg_source.id

            # Case 1: rg_source = 'rg1', find destination-replica
            rg_source = ct.rgs['rg1']
            source_replica_cnt = rg_source.count_replica(p1)
            # 3 replicas of p1 are in 'rg1'
            assert source_replica_cnt == 3
            # Test-destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                source_replica_cnt,
                under_replicated_rgs,
                p1,
            )
            # Dest-replica can be either 'rg3' or 'rg4' with replica-count 1
            assert rg_dest.id in ['rg3', 'rg4']

            # Case 2: rg-source == 'rg2': No destination group found
            rg_source = ct.rgs['rg2']
            source_replica_cnt = rg_source.count_replica(p1)
            # 3 replicas of p1 are in 'rg2'
            assert source_replica_cnt == 2
            # Test-destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                source_replica_cnt,
                under_replicated_rgs,
                p1,
            )
            # Since none of under-replicated-groups (rg3, and rg4) have lower
            # 2-1=0 replicas for the given partition p1
            # No eligible dest-group is there where partition-can be sent to
            assert rg_dest is None

    def test_elect_dest_replication_group(self):
        # Sample assignment with 3 replication groups
        # with replica-count as as per map
        # broker_rg: {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3'}
        # rg-id: (brokers), count
        # rg1: (0, 2, 4) = 3
        # rg2: (1, 3) = 2
        # rg3: (5) = 1
        # rg1 and rg2 are over-replicated and rg3 being under-replicated
        # source-replication-group should be rg1 having the highest replicas
        p1_info = ((u'T0', 0), [0, 1, 2, 3, 4, 5, 6])
        assignment = OrderedDict([p1_info])
        with self.build_cluster_topology(assignment, self.srange(7)) as ct:
            p1 = ct.partitions[p1_info[0]]
            # Case 1: rg_source = 'rg1', find destination-replica
            rg_source = ct.rgs['rg1']

            # 3 replicas of p1 are in 'rg1'
            source_replica_cnt = rg_source.count_replica(p1)
            under_replicated_rgs = [ct.rgs['rg3'], ct.rgs['rg4']]
            # Test-destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                source_replica_cnt,
                under_replicated_rgs,
                p1,
            )

            # Dest-replica can be either 'rg3' or 'rg4' with replica-count 1
            assert rg_dest.id in ['rg3', 'rg4']

            # Case 2: rg-source == 'rg2': No destination group found
            rg_source = ct.rgs['rg2']

            # 3 replicas of p1 are in 'rg2'
            source_replica_cnt = rg_source.count_replica(p1)

            # Test-destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                source_replica_cnt,
                under_replicated_rgs,
                p1,
            )

            # Since none of under-replicated-groups (rg3, and rg4) have lower
            # 2-1=0 replicas for the given partition p1
            # No eligible dest-group is there where partition-can be sent to
            assert rg_dest is None

    def test_rebalance_partition_imbalanced(self):
        # Test im-balanced partitions for below cases
        # Note: In initial-assignment, all partitions with id-1 are 'imbalanced'
        with self.build_cluster_topology() as ct:
            # CASE 1: repl-factor % rg-count == 0
            # 1a: repl-factor == rg-count
            # p1: replicas: ('T0', 1): [2,3]
            p1 = ct.partitions[('T0', 1)]
            opt_cnt = 1    # 2/2

            # Verify imbalanced p1
            self.assert_rg_imbalanced_partition(ct, p1, opt_cnt)
            # no change in replicas after rebalancing
            ct._rebalance_partition(p1)
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)

            # 1b:  repl-count % rg-count == 0 and repl-count > rg-count
            # p1: replicas: ('T1',1): [0,1,2,4]
            p1 = ct.partitions[('T1', 1)]

            # Verify imbalanced p1
            opt_cnt = 2    # 4/2
            self.assert_rg_imbalanced_partition(ct, p1, opt_cnt)
            # Verify no change in replicas after rebalancing
            ct._rebalance_partition(p1)
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)

            # CASE 2: repl-factor % rg-count > 0
            # p1: replicas ('T3', 1): [0,1,4]
            p1 = ct.partitions[('T3', 1)]

            # Verify imbalanced p1
            opt_cnt = 1    # 3/2
            extra_cnt = 1  # 3%2
            self.assert_rg_imbalanced_partition(ct, p1, opt_cnt)
            # Verify no change in replicas after rebalancing
            ct._rebalance_partition(p1)
            self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)

    def test_rebalance_partition_balanced(self):
        # Test already balanced partitions in given
        # example for different cases
        with self.build_cluster_topology() as ct:
            # CASE 1: repl-factor % rg-count == 0
            # 1a: repl-factor == rg-count
            # p1: replicas: ('T0', 0): [1,2]
            p1 = ct.partitions[('T0', 0)]
            opt_cnt = 1    # 2/2
            extra_cnt = 0  # 2%2

            # Verify already balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)
            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

            # 1b:  repl-count % rg-count == 0 and repl-count > rg-count
            # p1: replicas: ('T1',0): [0,1,2,3]
            p1 = ct.partitions[('T1', 0)]
            opt_cnt = 2    # 4/2
            extra_cnt = 0  # 4%2

            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

            # CASE 2: repl-factor % rg-count > 0
            # 2a: repl-factor < rg-count
            # p1: replicas ('T2', 0): [2]
            p1 = ct.partitions[('T2', 0)]
            opt_cnt = 0    # 1/2
            extra_cnt = 1  # 1%2

            # Verify already-balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)
            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

            # 2b: repl-factor > rg-count
            # p1: replicas: ('T3', 0), [0,1,2]): ['rg1', 'rg1', 'rg2']
            p1 = ct.partitions[('T3', 0)]
            opt_cnt = 1    # 3/2
            extra_cnt = 1  # 3%2

            # Verify already-balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)
            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

    def rg_rebalance_assert_no_change(self, ct, p1):
        """Verifies that there are no replica change after rebalancing."""
        old_replicas = p1.replicas
        ct._rebalance_partition(p1)

        # Verify no replica change for partition
        assert old_replicas == p1.replicas

    def assert_rg_balanced_partition(self, ct, p1, opt_cnt, extra_cnt=0):
        for rg in ct.rgs.itervalues():
            replica_cnt_rg = rg.count_replica(p1)

            # Verify for evenly-balanced partition p1
            assert replica_cnt_rg == opt_cnt or\
                replica_cnt_rg == opt_cnt + extra_cnt

    def assert_rg_imbalanced_partition(self, ct, part, opt_cnt, extra_cnt=0):
        imbal = False
        for rg in ct.rgs.itervalues():
            replica_cnt_rg = rg.count_replica(part)
            if replica_cnt_rg > opt_cnt + extra_cnt:
                imbal = True

        # Verify that atleast one of the groups is im-balanced
        assert imbal is True

    def srange(self, n):
        """Return list of integers as string from 0 to n-1."""
        return [str(x) for x in range(n)]
