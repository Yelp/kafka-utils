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
from __future__ import absolute_import
from __future__ import print_function

from argparse import Namespace

import mock
import pytest
import six

from .helper import broker_range
from kafka_utils.kafka_cluster_manager.cluster_info.error import RebalanceError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .partition_count_balancer import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import calculate_partition_movement
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_broker_leader_counts
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_net_imbalance
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_replication_group_imbalance_stats


class TestPartitionCountBalancer(object):

    @pytest.fixture
    def create_balancer(self):
        def build_balancer(cluster_topology, **kwargs):
            args = mock.Mock(spec=Namespace)
            args.balancer_args = []
            args.configure_mock(**kwargs)
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
        for t_p, new_replicas in six.iteritems(new_assignment):
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
        for partition, orig_replicas in six.iteritems(orig_assignment):
            assert set(orig_replicas) == set(new_assignment[partition])

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
            list(ct.rgs.values()),
            list(ct.partitions.values()),
        )

        # Verify that rg-group-balanced
        assert net_imbal == 0

        # Verify that new-assignment is valid
        self.assert_valid(
            ct.assignment,
            default_assignment,
            list(ct.brokers.keys()),
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
            list(ct.rgs.values()),
            list(ct.partitions.values()),
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
            list(ct.rgs.values()),
            list(ct.partitions.values()),
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
            list(ct.rgs.values()),
            list(ct.partitions.values()),
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
            list(ct.rgs.values()),
            list(ct.partitions.values()),
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

    def test__rebalance_groups_partition_cnt_case5(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # rg1 has 4 partitions
        # rg2 has 2 partitions
        # rg3 has 2 partitions
        # Result: rg's will be balanced for partition-count
        # All rg's will be balanced with just 1 partition-movement
        brokers = {
            "0": {"host": "host1"},
            "1": {"host": "host2"},
            "2": {"host": "host3"},
            "3": {"host": "host4"},
            "5": {"host": "host5"},
        }
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T1', 0), ['1', '3']),
                ((u'T2', 0), ['0', '5']),
                ((u'T3', 0), ['1', '5']),
            ]
        )
        ct = create_cluster_topology(assignment, brokers)
        cb = create_balancer(ct)

        # Re-balance replication-groups for partition-count
        cb._rebalance_groups_partition_cnt()

        # Assert partition is moved from rg1 only
        assert len(ct.rgs['rg1'].partitions) == 3
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Verify minimum partition movements 1
        assert total_movements == 1
        net_imbal, _ = get_replication_group_imbalance_stats(
            list(ct.rgs.values()),
            list(ct.partitions.values()),
        )
        # Verify replica-count imbalance remains unaltered
        assert net_imbal == 0

    def test__rebalance_groups_partition_cnt_case6(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        # rg1 has 5 partitions
        # rg2 has 1 partitions
        # rg3 has 1 partitions
        # Result: rg's will be balanced for partition-count
        # All rg's will be balanced with 2 partition-movements
        # This test case covers the aspect that even if the partition
        # count difference b/w the replication-groups is > 1,
        # we still move onto next replication-group if either of the
        # replication-groups reaches the optimal partition-count.
        brokers = {
            "0": {"host": "host1"},
            "1": {"host": "host2"},
            "2": {"host": "host3"},
            "3": {"host": "host4"},
            "5": {"host": "host5"},
        }
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T1', 0), ['1', '0']),
                ((u'T2', 0), ['0', '5']),
                ((u'T3', 0), ['1']),
            ]
        )
        ct = create_cluster_topology(assignment, brokers)
        cb = create_balancer(ct)

        # Re-balance replication-groups for partition-count
        cb._rebalance_groups_partition_cnt()

        # Assert final partition counts in replication-groups
        assert len(ct.rgs['rg1'].partitions) == 3
        assert len(ct.rgs['rg2'].partitions) == 2
        assert len(ct.rgs['rg3'].partitions) == 2
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Verify minimum partition movements 2
        assert total_movements == 2

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
        net_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )

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
        net_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )

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
        new_leaders_per_broker = {
            broker.id: broker.count_preferred_replica()
            for broker in six.itervalues(ct.brokers)
        }
        new_leader_imbal = get_net_imbalance(list(new_leaders_per_broker.values()))
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        net_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )

        cb = create_balancer(ct)
        cb.rebalance_leaders()

        new_leaders_per_broker = {
            broker.id: broker.count_preferred_replica()
            for broker in six.itervalues(ct.brokers)
        }
        new_net_imbal = get_net_imbalance(list(new_leaders_per_broker.values()))
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
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
        leader_imbal = get_net_imbalance(
            get_broker_leader_counts(list(ct.brokers.values())),
        )
        assert leader_imbal == 0

    # Revoke leadership tests
    def test_revoke_leadership_no_change(
        self,
        create_balancer,
        create_cluster_topology,
    ):
        # No leadership changes to be made to broker 2
        assignment = dict(
            [
                ((u'T0', 0), ['1', '0']),
                ((u'T1', 0), ['0', '1']),
                ((u'T1', 1), ['0']),
                ((u'T2', 0), ['1']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        cb = create_balancer(ct)
        cb.revoke_leadership(['2'])

        # Since Broker '2' wasn't leader of any partition
        # so there is no change in assignment
        assert ct.assignment == assignment

    def test_revoke_leadership_single_broker(
        self,
        create_balancer,
        create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['2', '0']),
                ((u'T1', 0), ['2', '1']),
                ((u'T1', 1), ['0', '2']),
                ((u'T2', 0), ['1', '0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(3))
        cb = create_balancer(ct)
        cb.revoke_leadership(['2'])

        new_leaders_per_broker = {
            broker.id: broker.count_preferred_replica()
            for broker in six.itervalues(ct.brokers)
        }
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Get net imbalance statistics excluding brokers to be revoked
        # leadership from
        brokers = [
            b for b in six.itervalues(ct.brokers)
            if b.id not in ['2', '3']
        ]
        new_net_imbal = get_net_imbalance(get_broker_leader_counts(brokers))
        # Verify that broker '2' is not leader of any partition
        assert new_leaders_per_broker['2'] == 0
        assert new_leaders_per_broker['1'] == 2
        assert new_leaders_per_broker['0'] == 2
        # Assert no partition movements
        assert total_movements == 0
        # Assert remaining brokers are balanced with leader count
        assert new_net_imbal == 0

    def test_revoke_leadership_multiple_brokers(
        self,
        create_balancer,
        create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['2', '0']),
                ((u'T1', 0), ['2', '1']),
                ((u'T1', 1), ['0', '2']),
                ((u'T2', 0), ['0', '3']),
                ((u'T3', 0), ['3', '1']),
                ((u'T3', 1), ['3', '1']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))
        cb = create_balancer(ct)
        cb.revoke_leadership(['2', '3'])

        new_leaders_per_broker = {
            broker.id: broker.count_preferred_replica()
            for broker in six.itervalues(ct.brokers)
        }
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        # Get net imbalance statistics excluding brokers to be revoked
        # leadership from
        brokers = [
            b for b in six.itervalues(ct.brokers)
            if b.id not in ['2', '3']
        ]
        new_net_imbal = get_net_imbalance(get_broker_leader_counts(brokers))
        # Verify that broker '2' and '3' is not leader of any partition
        assert new_leaders_per_broker['0'] == 3
        assert new_leaders_per_broker['1'] == 3
        assert new_leaders_per_broker['2'] == 0
        assert new_leaders_per_broker['3'] == 0
        # Assert no partition movements
        assert total_movements == 0
        # Assert remaining brokers are balanced with leader count
        assert new_net_imbal == 0

    def test_revoke_leadership_not_possible(
        self,
        create_balancer,
        create_cluster_topology,
    ):
        assignment = dict(
            [
                ((u'T0', 0), ['2', '3']),
                ((u'T1', 0), ['3']),
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['1', '0']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))
        cb = create_balancer(ct)
        cb.revoke_leadership(['2', '3'])

        new_leaders_per_broker = {
            broker.id: broker.count_preferred_replica()
            for broker in six.itervalues(ct.brokers)
        }
        # Broker '2' and '3' are to be revoked from leadership
        # But (u'T0, 0) has replicas '2' and '3'
        # and (u'T1', 0) has only single replica
        # Assert no leadership changes
        assert new_leaders_per_broker['0'] == 1
        assert new_leaders_per_broker['1'] == 1
        assert new_leaders_per_broker['2'] == 1
        assert new_leaders_per_broker['3'] == 1
        # Assert no partition movements
        _, total_movements = \
            calculate_partition_movement(assignment, ct.assignment)
        assert total_movements == 0
