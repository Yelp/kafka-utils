import contextlib
from collections import Counter, OrderedDict
from mock import sentinel, patch, MagicMock
from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import (
    ClusterTopology,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import (
    get_replication_group_imbalance_stats,
    get_leader_imbalance_stats,
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
    # Example assignment properties:
    # * Brokers:(0,1,2,3): rg-count = 2
    # case 1: replication-factor % rg-count == 0
    #    -- T0, T1:
    #   * 1a) T1: replication-factor > rg-count
    # case 2: replication-factor % rg-count != 0
    #   -- T2, T3
    #   * 2a): replication-factor > rg-count: T1
    #   * 2b): replication-factor < rg-count: T2
    # rg-imbalance-status per partition:
    #
    # rg-imbalanced-partitions: T0-1, T1-1, T3-1
    # rg-balanced-partitions:   T0-0, T1-0, T3-0, T2-0
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
            brokers_info = {
                broker_id: (sentinel.obj, broker_id)
                for broker_id in broker_ids
            }
        else:
            brokers_info = self.brokers_info
        topic_ids = sorted(set([t_p[0] for t_p in assignment.iterkeys()]))
        mock_zk.get_brokers.return_value = brokers_info
        mock_zk.get_topics.return_value = topic_ids
        with contextlib.nested(
            patch(
                'yelp_kafka_tool.kafka_cluster_manager.cluster_info.'
                'cluster_topology.KafkaInterface',
                autospec=True,
            ),
            self.mock_get_replication_group_id(),
        ) as (mock_kafka, mock_rg_groups):
            # Create cluster-object
            mock_kafka.return_value.get_cluster_assignment.return_value = assignment
            yield ClusterTopology(mock_zk)

    def test_rebalance_replication_groups(self):
        with self.build_cluster_topology() as ct:
            ct.rebalance_replication_groups()
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )

            # Verify that rg-group-balanced
            assert(net_imbal, 0)
            # Verify that new-assignment is valid
            self.assert_valid(
                ct.assignment,
                self._initial_assignment,
                ct.brokers.keys(),
            )

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

            # Verify that rg-group-balanced
            assert net_imbal == 0
            # Verify that new-assignment same as previous
            assert ct.assignment == assignment

    def test_segregate_replication_groups_case1(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 1: replication-factor % rg-count == 0
        """
        with self.build_cluster_topology() as ct:
            # Partition: T0-0 # Already-balanced
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T0', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)

            # Verify compute-optimal-count
            assert opt_cnt == 1 and evenly_dist is True
            # Verify segregation of rg's
            assert not over_replicated_rgs and not under_replicated_rgs

            # Partition T0-1: # imbalanced
            # opt-count == 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T0', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)

            # Verify for segregation of rg-groups
            assert opt_cnt == 1 and evenly_dist is True
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert over_rg_ids == ['rg2'] and under_rg_ids == ['rg1']

            # Partition: T1-0 # Already-balanced: replication-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T1', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)

            # Verify for segregation of rg-groups
            assert opt_cnt == 2 and evenly_dist is True
            assert not over_replicated_rgs or under_replicated_rgs

    def test_segregate_replication_groups_case_2(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 2: replication-factor % rg-count != 0
        Even if some partitions are balanced, as per the algorithm,
        we still have under and/or replicated groups.
        """

        with self.build_cluster_topology() as ct:
            # Partition: T2-0: Already balanced, replication-factor < rg-count
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T2', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]

            # Verify compute-optimal-count
            assert opt_cnt == 0 and evenly_dist is False
            # Verify segregation of rg's
            # Since, evenly_dist is False, therefore some rg-groups can have
            # opt-cnt+1 replicas, so we categorize every replication-group into
            # under or over-replicated
            assert over_rg_ids == ['rg2'] and under_rg_ids == ['rg1']

            # Partition T3-0: # Already-balanced
            # opt-count: 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T3', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)

            # Verify segregation of rg's
            # Since, evenly_dist is False, therefore some rg-groups can have
            # opt-cnt+1 replicas, so we categorize every replication-group into
            # under or over-replicated
            assert evenly_dist is False
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert opt_cnt == 1 and evenly_dist is False
            assert under_rg_ids == ['rg2']and over_rg_ids == ['rg1']

            # Partition: T3-1 # imbalanced: replication-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_dist = self.partition_data(ct, ('T3', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(partition, opt_cnt, evenly_dist)

            # Verify compute-optimal-count
            assert opt_cnt == 1 and evenly_dist is False
            # Verify under and over replication-group ids
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert under_rg_ids == ['rg2'] and over_rg_ids == ['rg1']

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
            assignment_json = {
                'version': 1,
                'partitions':
                [
                    {
                        'topic': partition[0],
                        'partition': partition[1],
                        'replicas': replicas
                    }
                    for partition, replicas in self._initial_assignment.iteritems()
                ]
            }
            # Verify json formatted assignment
            assert sorted(ct.get_assignment_json()) == sorted(assignment_json)

    def test_elect_source_replication_group(self):
        # Sample assignment with 3 replication groups
        # with replica-count as as per map :-
        # broker_rg = {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3', 6: 'rg4'}
        # rg-id:    (brokers), count
        # rg1:      (0, 2, 4) = 3
        # rg2:      (1, 3) = 2
        # rg3:      (5) = 1
        # rg4:      (6) = 1
        # rg1 and rg2 are over-replicated and rg3 being under-replicated
        # source-replication-group should be rg1 having the highest replicas
        p1 = ((u'T0', 0), [0, 1, 2, 3, 4, 5, 6])
        assignment = OrderedDict([p1])
        with self.build_cluster_topology(assignment, self.srange(7)) as ct:
            # Case-1: rg's have only 1 unique max replica count
            # 'rg1' and 'rg2' are over-replicated groups
            over_replicated_rgs = [ct.rgs['rg1'], ct.rgs['rg2']]

            # Get source-replication group
            rg_source = ct._elect_source_replication_group(over_replicated_rgs, p1)

            # Since, 'rg1' has more replicas i.e. 3, it should be selected
            assert rg_source.id == 'rg1'

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
            under_replicated_rgs = [ct.rgs['rg3'], ct.rgs['rg4']]
            # Get destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                rg_source.count_replica(p1),
                under_replicated_rgs,
                p1,
            )

            # Dest-replica can be either 'rg3' or 'rg4' with replica-count 1
            assert rg_dest.id in ['rg3', 'rg4']

            # Case 2: rg-source == 'rg2': No destination group found
            rg_source = ct.rgs['rg2']
            # Get destination-replication-group for partition: p1
            rg_dest = ct._elect_dest_replication_group(
                rg_source.count_replica(p1),
                under_replicated_rgs,
                p1,
            )

            # Since none of under-replicated-groups (rg3, and rg4) have lower
            # 2-1=0 replicas for the given partition p1
            # No eligible dest-group is there where partition-can be sent to
            assert rg_dest is None

    def test_rebalance_partition_imbalanced_case1(self):
        # Test imbalanced partitions for below cases
        # Note: In initial-assignment, all partitions with id-1 are 'imbalanced'
        with self.build_cluster_topology() as ct:
            # CASE 1: repl-factor % rg-count == 0
            # (1a): repl-factor == rg-count
            # p1: replicas: ('T0', 1): [2,3]
            p1 = ct.partitions[('T0', 1)]
            # rg-imbalanced p1
            opt_cnt = 1    # 2/2
            ct._rebalance_partition(p1)

            # Verify partition is rg-balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)

            # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
            # p1: replicas: ('T1',1): [0,1,2,4]
            p1 = ct.partitions[('T1', 1)]
            # Assert originally-imbalanced p1
            opt_cnt = 2    # 4/2
            ct._rebalance_partition(p1)

            # Verify partition is rg-balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)

    def test_rebalance_partition_imbalanced_case2(self):
        with self.build_cluster_topology() as ct:
            # CASE 2: repl-factor % rg-count > 0
            # p1: replicas ('T3', 1): [0,1,4]
            p1 = ct.partitions[('T3', 1)]
            # rg-imbalanced p1
            opt_cnt = 1    # 3/2
            extra_cnt = 1  # 3%2
            ct._rebalance_partition(p1)

            # Verify partition is now rg-balanced
            self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)

    def test_rebalance_partition_balanced(self):
        # Test already balanced partitions in given example for different cases
        # Analyze Cases 1a, 1b
        with self.build_cluster_topology() as ct:
            # CASE 1: repl-factor % rg-count == 0
            # (1a): repl-factor == rg-count
            # p1: replicas: ('T0', 0): [1,2]
            p1 = ct.partitions[('T0', 0)]
            opt_cnt = 1    # 2/2
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)
            ct._rebalance_partition(p1)

            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

            # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
            # p1: replicas: ('T1',0): [0,1,2,3]
            p1 = ct.partitions[('T1', 0)]
            opt_cnt = 2    # 4/2
            self.assert_rg_balanced_partition(ct, p1, opt_cnt)

            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

    def test_rebalance_partition_balanced_case2(self):
        # Test already balanced partitions in given example for different cases
        # Analyze Cases 2a, 2b
        with self.build_cluster_topology() as ct:
            # CASE 2: repl-factor % rg-count > 0
            # (2a): repl-factor < rg-count
            # p1: replicas ('T2', 0): [2]
            p1 = ct.partitions[('T2', 0)]
            opt_cnt = 0    # 1/2
            extra_cnt = 1  # 1%2
            self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)

            # Verify no change in replicas after rebalancing
            self.rg_rebalance_assert_no_change(ct, p1)

            # (2b): repl-factor > rg-count
            # p1: replicas: ('T3', 0), [0,1,2]): ['rg1', 'rg1', 'rg2']
            p1 = ct.partitions[('T3', 0)]
            opt_cnt = 1    # 3/2
            extra_cnt = 1  # 3%2
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

    def srange(self, n):
        """Return list of integers as string from 0 to n-1."""
        return [str(x) for x in range(n)]

    def partition_data(self, ct, p_name):
        partition = ct.partitions[p_name]
        opt_cnt, extra_cnt = \
            compute_optimal_count(
                partition.replication_factor,
                len(ct.rgs.values()),
            )
        return partition, opt_cnt, not extra_cnt

    def assert_valid(self, new_assignment, orig_assignment, orig_brokers):
        """Assert if new-assignment is valid based on given assignment.

        Asserts the results for following parameters:
        a) Asserts that keys in both assignments are same
        b) Asserts that replication-factor of result remains same
        c) Assert that new-replica-brokers are amongst given broker-list
        """

        # Verify that partitions remain same
        assert sorted(orig_assignment.keys()) == sorted(new_assignment.keys())
        for t_p, new_replicas in new_assignment.iteritems():
            orig_replicas = orig_assignment[t_p]
            # Verify that new-replicas are amongst given broker-list
            assert all([broker in orig_brokers for broker in new_replicas])
            # Verify that replication-factor remains same
            assert len(new_replicas) == len(orig_replicas)

    # Tests for leader-balancing
    def test_rebalance_leaders_balanced_case1(self):
        # Already balanced-assignment with evenly-distributed
        # (broker-id: leader-count): {0: 1, 1:1, 2:1}
        # opt-count: 3/3 = 1, extra-count: 3%3 = 0
        assignment = OrderedDict(
            [
                ((u'T0', 0), [1, 2]),
                ((u'T0', 1), [2, 0]),
                ((u'T1', 0), [0, 2]),
            ]
        )
        with self.build_cluster_topology(assignment, ) as ct:
            orig_assignment = ct.assignment
            ct.rebalance_leaders()
            updated_assignment = ct.assignment
            std_imbal, net_imbal, leaders_per_broker = get_leader_imbalance_stats(
                ct.brokers.values(),
            )

            # No changed in already-balanced assignment
            assert orig_assignment == updated_assignment
            # Assert leader-balanced
            assert net_imbal == 0

    def test_rebalance_leaders_balanced_case2(self):
        # Already balanced-assignment NOT evenly-distributed
        # (broker-id: leader-count): {0: 1, 1:1, 2:1}
        # opt-count: 2/3 = 0, extra-count: 2%3 = 2
        assignment = OrderedDict(
            [
                ((u'T0', 0), [1, 2]),
                ((u'T0', 1), [2, 0]),
            ]
        )
        with self.build_cluster_topology(assignment, ) as ct:
            orig_assignment = ct.assignment
            ct.rebalance_leaders()
            updated_assignment = ct.assignment
            std_imbal, net_imbal, leaders_per_broker = get_leader_imbalance_stats(
                ct.brokers.values(),
            )

            # No changed in already-balanced assignment
            assert orig_assignment == updated_assignment
            # Assert leader-balanced
            assert net_imbal == 0

    def test_rebalance_leaders_unbalanced_case1(self):
        # Balance leader-imbalance successfully
        # (broker-id: leader-count): {0: 0, 1:2, 2:1}
        # Net-leader-imbalance: 1
        # opt-count: 3/3 = 1, extra-count: 3%3 = 0
        assignment = OrderedDict(
            [
                ((u'T0', 0), [1, 2]),
                ((u'T0', 1), [2, 0]),
                ((u'T1', 0), [1, 0]),
            ]
        )
        with self.build_cluster_topology(assignment, [0, 1, 2]) as ct:
            orig_assignment = ct.assignment
            ct.rebalance_leaders()

            # Verify if valid-leader assignment
            new_assignment = ct.assignment
            self.assert_leader_valid(orig_assignment, new_assignment)

            # New-leader imbalance-count be less than previous imbal count
            new_std_imbal, new_leader_imbal, new_leaders_per_broker = \
                get_leader_imbalance_stats(ct.brokers.values())
            # Verify leader-balanced
            assert new_leader_imbal == 0
            # Verify partitions-changed assignment
            assert new_leaders_per_broker[0] == 1
            assert new_leaders_per_broker[1] == 1
            assert new_leaders_per_broker[2] == 1

    def test_rebalance_leaders_unbalanced_case2(self):
        # No leader-imbalance possible
        # Un-balanced assignment
        # (Broker: leader-count): {0: 2, 1: 1, 2:0}
        # opt-count: 3/3 = 1, extra-count = 0
        # Leader-imbalance: 1
        assignment = OrderedDict(
            [
                ((u'T0', 0), [1, 2]),
                ((u'T0', 1), [0, 1]),
                ((u'T1', 0), [0]),
            ]
        )
        with self.build_cluster_topology(assignment, [0, 1, 2]) as ct:
            orig_assignment = ct.assignment
            ct.rebalance_leaders()

            # Verify no assignment change
            assert orig_assignment == ct.assignment

            # Verify still leader-imbalanced
            _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
            # TODO: fix this, this should be balanced
            assert leader_imbal > 0

    def test_rebalance_leaders_unbalanced_case3(self):
        # Partial leader-imbalance possible
        # Un-balanced assignment
        # (Broker: leader-count): {0: 3, 1: 1, 2:0}
        # opt-count: 5/3 = 1, extra-count = 2
        assignment = OrderedDict(
            [
                ((u'T0', 0), [1, 2]),
                ((u'T0', 1), [0, 2]),
                ((u'T1', 0), [0]),
                ((u'T1', 1), [0]),
                ((u'T1', 2), [0]),
            ]
        )

        with self.build_cluster_topology(assignment, [0, 1, 2]) as ct:
            _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
            ct.rebalance_leaders()
            _, new_net_imbal, new_leaders_per_broker = get_leader_imbalance_stats(
                ct.brokers.values(),
            )
            # Verify that net-imbalance has reduced but not zero
            assert new_net_imbal < net_imbal
            assert new_net_imbal > 0
            # Verify the changes in leaders-per-broker count
            assert new_leaders_per_broker[2] == 1
            assert new_leaders_per_broker[1] == 1
            assert new_leaders_per_broker[0] == 3

    def assert_leader_valid(self, orig_assignment, new_assignment):
        """Verify that new-assignment complies with just leader changes.

        Following characteristics are verified for just leader-changes.
        a) partitions remain same
        b) replica set remains same
        """
        # Partition-list remains un-changed
        assert sorted(orig_assignment.keys()) == sorted(new_assignment.keys())
        # Replica-set remains same
        for partition, orig_replicas in orig_assignment.iteritems():
            set(orig_replicas) == set(new_assignment[partition])
