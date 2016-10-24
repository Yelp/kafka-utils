# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import mock
import pytest

from .helper import broker_range
from kafka_utils.kafka_cluster_manager.cluster_info.error import RebalanceError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .partition_count_balancer import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import calculate_partition_movement
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_leader_imbalance_stats
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_replication_group_imbalance_stats


class TestPartitionCountBalancer(object):

    @pytest.fixture
    def create_balancer(self):
        def build_balancer(cluster_topology, **kwargs):
            args = mock.Mock(**kwargs)
            return PartitionCountBalancer(cluster_topology, args)
        return build_balancer

    def assert_valid(self, new_assignment, orig_assignment, orig_brokers):
        """Assert if new-assignment is valid based on given assignment.

        Asserts the results for following parameters:
        a) Asserts that keys in both assignments are same
        b) Asserts that replication-factor of result remains same
        c) Assert that new-replica-brokers are amongst given broker-list
        """

        # Verify that partitions remain same
        assert set(orig_assignment.keys()) == set(new_assignment.keys())
        for t_p, new_replicas in new_assignment.iteritems():
            orig_replicas = orig_assignment[t_p]
            # Verify that new-replicas are amongst given broker-list
            assert all([broker in orig_brokers for broker in new_replicas])
            # Verify that replication-factor remains same
            assert len(new_replicas) == len(orig_replicas)

    def assert_leader_valid(self, orig_assignment, new_assignment):
        """Verify that new-assignment complies with just leader changes.

        Following characteristics are verified for just leader-changes.
        a) partitions remain same
        b) replica set remains same
        """
        # Partition-list remains unchanged
        assert sorted(orig_assignment.keys()) == sorted(new_assignment.keys())
        # Replica-set remains same
        for partition, orig_replicas in orig_assignment.iteritems():
            assert set(orig_replicas) == set(new_assignment[partition])

    def rg_rebalance_assert_no_change(self, cb, p1):
        """Verifies that there are no replica change after rebalancing."""
        old_replicas = p1.replicas
        cb._rebalance_partition(p1)

        # Verify no replica change for partition
        assert old_replicas == p1.replicas

    def assert_rg_balanced_partition(self, ct, p1, opt_cnt, extra_cnt=0):
        for rg in ct.rgs.itervalues():
            replica_cnt_rg = rg.count_replica(p1)

            # Verify for evenly-balanced partition p1
            assert replica_cnt_rg == opt_cnt or\
                replica_cnt_rg == opt_cnt + extra_cnt

    def test_elect_source_replication_group(
            self,
            create_balancer,
            create_cluster_topology,
    ):
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
        p1 = ((u'T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
        assignment = dict([p1])
        ct = create_cluster_topology(assignment, broker_range(7))
        cb = create_balancer(ct)
        # Case-1: rg's have only 1 unique max replica count
        # 'rg1' and 'rg2' are over-replicated groups
        over_replicated_rgs = [ct.rgs['rg1'], ct.rgs['rg2']]

        # Get source-replication group
        rg_source = cb._elect_source_replication_group(over_replicated_rgs, p1)

        # Since, 'rg1' has more replicas i.e. 3, it should be selected
        assert rg_source.id == 'rg1'

    def test_elect_dest_replication_group(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Sample assignment with 3 replication groups
        # with replica-count as as per map
        # broker_rg: {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3'}
        # rg-id: (brokers), count
        # rg1: (0, 2, 4) = 3
        # rg2: (1, 3) = 2
        # rg3: (5) = 1
        # rg1 and rg2 are over-replicated and rg3 being under-replicated
        # source-replication-group should be rg1 having the highest replicas
        p1_info = ((u'T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
        assignment = dict([p1_info])
        ct = create_cluster_topology(assignment, broker_range(7))
        p1 = ct.partitions[p1_info[0]]
        cb = create_balancer(ct)
        # Case 1: rg_source = 'rg1', find destination-replica
        rg_source = ct.rgs['rg1']
        under_replicated_rgs = [ct.rgs['rg3'], ct.rgs['rg4']]
        # Get destination-replication-group for partition: p1
        rg_dest = cb._elect_dest_replication_group(
            rg_source.count_replica(p1),
            under_replicated_rgs,
            p1,
        )

        # Dest-replica can be either 'rg3' or 'rg4' with replica-count 1
        assert rg_dest.id in ['rg3', 'rg4']

        # Case 2: rg-source == 'rg2': No destination group found
        rg_source = ct.rgs['rg2']
        # Get destination-replication-group for partition: p1
        rg_dest = cb._elect_dest_replication_group(
            rg_source.count_replica(p1),
            under_replicated_rgs,
            p1,
        )

        # Since none of under-replicated-groups (rg3, and rg4) have lower
        # 2-1=0 replicas for the given partition p1
        # No eligible dest-group is there where partition can be sent to
        assert rg_dest is None

    def test_rebalance_partition_imbalanced_case1(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Test imbalanced partitions for below cases
        # Note: In initial-assignment, all partitions with id-1 are 'imbalanced'
        ct = create_cluster_topology()
        cb = create_balancer(ct)
        # CASE 1: repl-factor % rg-count == 0
        # (1a): repl-factor == rg-count
        # p1: replicas: ('T0', 1): [2,3]
        p1 = ct.partitions[('T0', 1)]
        # rg-imbalanced p1
        opt_cnt = 1    # 2/2

        cb._rebalance_partition(p1)

        # Verify partition is rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)

        # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
        # p1: replicas: ('T1',1): [0,1,2,4]
        p1 = ct.partitions[('T1', 1)]
        # Assert originally-imbalanced p1
        opt_cnt = 2    # 4/2
        cb._rebalance_partition(p1)

        # Verify partition is rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)

    def test_rebalance_partition_imbalanced_case2(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        ct = create_cluster_topology()
        cb = create_balancer(ct)
        # CASE 2: repl-factor % rg-count > 0
        # p1: replicas ('T3', 1): [0,1,4]
        p1 = ct.partitions[('T3', 1)]
        # rg-imbalanced p1
        opt_cnt = 1    # 3/2
        extra_cnt = 1  # 3%2
        cb._rebalance_partition(p1)

        # Verify partition is now rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)

    def test_rebalance_partition_balanced(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Test already balanced partitions in given example for different cases
        # Analyze Cases 1a, 1b
        ct = create_cluster_topology()
        cb = create_balancer(ct)
        # CASE 1: repl-factor % rg-count == 0
        # (1a): repl-factor == rg-count
        # p1: replicas: ('T0', 0): [1,2]
        p1 = ct.partitions[('T0', 0)]
        opt_cnt = 1    # 2/2
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)
        cb._rebalance_partition(p1)

        # Verify no change in replicas after rebalancing
        self.rg_rebalance_assert_no_change(cb, p1)

        # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
        # p1: replicas: ('T1',0): [0,1,2,3]
        p1 = ct.partitions[('T1', 0)]
        opt_cnt = 2    # 4/2
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)

        # Verify no change in replicas after rebalancing
        self.rg_rebalance_assert_no_change(cb, p1)

    def test_rebalance_replication_groups(
            self,
            create_balancer,
            create_cluster_topology,
            default_assignment,
    ):
        ct = create_cluster_topology()

        cb = create_balancer(ct)
        cb.rebalance_replication_groups()

        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )

        # Verify that rg-group-balanced
        assert net_imbal == 0

        # Verify that new-assignment is valid
        self.assert_valid(
            ct.assignment,
            default_assignment,
            ct.brokers.keys(),
        )

    def test_rebalance_replication_groups_balanced(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Replication-group is already balanced
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T0', 1), ['0', '3']),
                ((u'T2', 0), ['2']),
                ((u'T3', 0), ['0', '1', '2']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(5))

        cb = create_balancer(ct)
        cb.rebalance_replication_groups()

        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )

        # Verify that rg-group-balanced
        assert net_imbal == 0
        # Verify that new-assignment same as previous
        assert ct.assignment == assignment

    def test_rebalance_replication_groups_error(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T0', 1), ['0', '3']),
                ((u'T2', 0), ['2']),
                ((u'T3', 0), ['0', '1', '9']),  # broker 9 is not active
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(5))

        with pytest.raises(RebalanceError):
            cb = create_balancer(ct)
            cb.rebalance_replication_groups()

    def test__rebalance_groups_partition_cnt_case1(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # rg1 has 6 partitions
        # rg2 has 2 partitions
        # Both rg's are balanced(based on replica-count) initially
        # Result: rg's will be balanced for partition-count
        assignment = dict(
            [
                ((u'T1', 1), ['0', '1', '2']),
                ((u'T1', 0), ['1']),
                ((u'T3', 0), ['1']),
                ((u'T2', 0), ['0', '1', '3']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))
        cb = create_balancer(ct)

        # Re-balance replication-groups for partition-count
        cb._rebalance_groups_partition_cnt()

        # Verify both replication-groups have same partition-count
        assert len(ct.rgs['rg1'].partitions) == len(ct.rgs['rg2'].partitions)
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Verify minimum partition movements 2
        assert total_movements == 2
        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )
        # Verify replica-count imbalance remains unaltered
        assert net_imbal == 0

    def test__rebalance_groups_partition_cnt_case2(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # 1 over-balanced, 2 under-balanced replication-groups
        # rg1 has 4 partitions
        # rg2 has 1 partition
        # rg3 has 1 partition
        # All rg's are balanced(based on replica-count) initially
        # Result: rg's will be balanced for partition-count
        assignment = dict(
            [
                ((u'T1', 1), ['0', '2']),
                ((u'T3', 1), ['0']),
                ((u'T3', 0), ['0']),
                ((u'T2', 0), ['0', '5']),
            ]
        )
        brokers = {
            '0': mock.MagicMock(),
            '2': mock.MagicMock(),
            '5': mock.MagicMock(),
        }
        ct = create_cluster_topology(assignment, brokers)
        cb = create_balancer(ct)

        # Re-balance brokers
        cb._rebalance_groups_partition_cnt()

        # Verify all replication-groups have same partition-count
        assert len(ct.rgs['rg1'].partitions) == len(ct.rgs['rg2'].partitions)
        assert len(ct.rgs['rg1'].partitions) == len(ct.rgs['rg3'].partitions)
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Verify minimum partition movements 2
        assert total_movements == 2
        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )
        # Verify replica-count imbalance remains 0
        assert net_imbal == 0

    def test__rebalance_groups_partition_cnt_case3(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # 1 over-balanced, 1 under-balanced, 1 opt-balanced replication-group
        # rg1 has 3 partitions
        # rg2 has 2 partitions
        # rg3 has 1 partition
        # All rg's are balanced(based on replica-count) initially
        # Result: rg's will be balanced for partition-count
        assignment = dict(
            [
                ((u'T1', 1), ['0', '2']),
                ((u'T3', 1), ['2']),
                ((u'T3', 0), ['0']),
                ((u'T2', 0), ['0', '5']),
            ]
        )
        brokers = {
            '0': mock.MagicMock(),
            '2': mock.MagicMock(),
            '5': mock.MagicMock(),
        }
        ct = create_cluster_topology(assignment, brokers)
        cb = create_balancer(ct)

        # Re-balance brokers across replication-groups
        cb._rebalance_groups_partition_cnt()

        # Verify all replication-groups have same partition-count
        assert len(ct.rgs['rg1'].partitions) == len(ct.rgs['rg2'].partitions)
        assert len(ct.rgs['rg1'].partitions) == len(ct.rgs['rg3'].partitions)
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Verify minimum partition movements
        assert total_movements == 1
        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )
        # Verify replica-count imbalance remains 0
        assert net_imbal == 0

    def test__rebalance_groups_partition_cnt_case4(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # rg1 has 4 partitions
        # rg2 has 2 partitions
        # Both rg's are balanced(based on replica-count) initially
        # Result: rg's couldn't be balanced partition-count since
        # no available broker without partition movement
        assignment = dict(
            [
                ((u'T1', 1), ['0', '1', '2']),
                ((u'T2', 0), ['0', '1', '2']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        cb = create_balancer(ct)

        # Re-balance replication-groups for partition-count
        cb._rebalance_groups_partition_cnt()

        # Verify no change in assignment
        assert ct.assignment == assignment

    # Tests for leader-balancing
    def test_rebalance_leaders_balanced_case1(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Already balanced-assignment with evenly-distributed
        # (broker-id: leader-count): {0: 1, 1:1, 2:1}
        # opt-count: 3/3 = 1, extra-count: 3%3 = 0
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
                ((u'T1', 0), ['0', '2']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        orig_assignment = ct.assignment

        cb = create_balancer(ct)
        cb.rebalance_leaders()
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())

        # No changed in already-balanced assignment
        assert orig_assignment == ct.assignment
        # Assert leader-balanced
        assert net_imbal == 0

    def test_rebalance_leaders_balanced_case2(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Already balanced-assignment NOT evenly-distributed
        # (broker-id: leader-count): {0: 1, 1:1, 2:1}
        # opt-count: 2/3 = 0, extra-count: 2%3 = 2
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        orig_assignment = ct.assignment

        cb = create_balancer(ct)
        cb.rebalance_leaders()
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())

        # No changed in already-balanced assignment
        assert orig_assignment == ct.assignment
        # Assert leader-balanced
        assert net_imbal == 0

    def test_rebalance_leaders_unbalanced_case1(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Balance leader-imbalance successfully
        # (broker-id: leader-count): {0: 0, 1:2, 2:1}
        # Net-leader-imbalance: 1
        # opt-count: 3/3 = 1, extra-count: 3%3 = 0
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
                ((u'T1', 0), ['1', '0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        orig_assignment = ct.assignment

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify if valid-leader assignment
        self.assert_leader_valid(orig_assignment, ct.assignment)
        # New-leader imbalance-count be less than previous imbal count
        _, new_leader_imbal, new_leaders_per_broker = \
            get_leader_imbalance_stats(ct.brokers.values())
        # Verify leader-balanced
        assert new_leader_imbal == 0
        # Verify partitions-changed assignment
        assert new_leaders_per_broker['0'] == 1
        assert new_leaders_per_broker['1'] == 1
        assert new_leaders_per_broker['2'] == 1

    def test_rebalance_leaders_unbalanced_case2(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # (Broker: leader-count): {0: 2, 1: 1, 2:0}
        # opt-count: 3/3 = 1, extra-count = 0
        # Leader-imbalance-value: 1
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T1', 0), ['0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2a(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # (Broker: leader-count): {0: 2, 1: 1, 2:0, 3:1}
        # opt-count: 3/4 = 1, extra-count = 3
        # Leader-imbalance-value: 1
        # imbalanced-broker: 0,2; balanced-brokers: 1,3
        assignment = dict(
            [
                ((u'T0', 0), ['3', '2']),
                ((u'T0', 1), ['1', '3']),
                ((u'T1', 1), ['0', '1']),
                ((u'T1', 0), ['0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0
        # Verify that (T0, 1) also swapped even if 1 and 3 were balanced
        # Rebalancing through non-followers
        replica_ids = [b.id for b in ct.partitions[('T0', 1)].replicas]
        assert replica_ids == ['3', '1']

    def test_rebalance_leaders_unbalanced_case2b(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['3', '2']),
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2c(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Broker-2 imbalance value: 2 with different brokers
        # Broker-2 requests leadership from multiple brokers (0, 1) once
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['0']),
                ((u'T2', 1), ['0']),
                ((u'T3', 0), ['3', '2']),
                ((u'T3', 1), ['1', '3']),
                ((u'T4', 0), ['1']),
                ((u'T4', 2), ['3']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2d(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Broker-2 imbalanced with same brokers
        # Broker-2 requests leadership from same broker-1 twice
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T1', 2), ['0']),
                ((u'T1', 3), ['1', '2']),
                ((u'T1', 4), ['0', '1']),
                ((u'T1', 5), ['0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2e(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Imbalance-val 2
        # Multiple imbalanced brokers (2, 5) gets non-follower balanced
        # from multiple brokers (1,4)
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['0']),
                ((u'T3', 0), ['4', '5']),
                ((u'T3', 1), ['3', '4']),
                ((u'T4', 0), ['3']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(6))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case3(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Imbalanced 0 and 2. No re-balance possible.
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0']),
                ((u'T2', 0), ['0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify still leader-imbalanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 1
        # No change in assignment
        assert sorted(ct.assignment) == sorted(assignment)

    def test_rebalance_leaders_unbalanced_case4(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Imbalanced assignment
        # Partial leader-imbalance possible
        # (Broker: leader-count): {0: 3, 1: 1, 2:0}
        # opt-count: 5/3 = 1, extra-count = 2
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['0', '2']),
                ((u'T1', 0), ['0']),
                ((u'T1', 1), ['0']),
                ((u'T1', 2), ['0']),
            ]
        )

        ct = create_cluster_topology(assignment, broker_range(3))
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        _, new_net_imbal, new_leaders_per_broker = get_leader_imbalance_stats(
            ct.brokers.values(),
        )
        # Verify that net-imbalance has reduced but not zero
        assert new_net_imbal > 0 and new_net_imbal < net_imbal
        # Verify the changes in leaders-per-broker count
        assert new_leaders_per_broker['2'] == 1
        assert new_leaders_per_broker['1'] == 1
        assert new_leaders_per_broker['0'] == 3

    def test_rebalance_leaders_unbalanced_case2f(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['2', '0']),
                ((u'T1', 0), ['2', '0']),
                ((u'T1', 1), ['0']),
                ((u'T2', 0), ['1']),
                ((u'T2', 1), ['2']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case5(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # Special case, wherein none under-balanced
        # but 0 is overbalanced
        assignment = dict(
            [
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['0']),
                ((u'T2', 1), ['0']),
                ((u'T3', 0), ['2', '3']),
                ((u'T3', 1), ['3', '1']),
                ((u'T4', 0), ['1']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0
