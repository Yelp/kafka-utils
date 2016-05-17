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

from kafka_utils.kafka_cluster_manager.cluster_info \
    .cluster_topology import ClusterTopology
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import BrokerDecommissionError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import InvalidBrokerIdError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import InvalidPartitionError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import RebalanceError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import calculate_partition_movement
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_leader_imbalance_stats
from kafka_utils.kafka_cluster_manager.cluster_info \
    .stats import get_replication_group_imbalance_stats


class TestClusterTopology(object):
    # replication-group to broker map
    # rg1: 0, 1, 4; rg2: 2, 3; rg3: 5; rg4: 6;
    broker_rg = {
        '0': 'rg1', '1': 'rg1', '2': 'rg2', '3': 'rg2',
        '4': 'rg1', '5': 'rg3', '6': 'rg4',
    }
    topic_ids = ['T0', 'T1', 'T2', 'T3']
    brokers = {
        '0': {'host': 'host1'},
        '1': {'host': 'host2'},
        '2': {'host': 'host3'},
        '3': {'host': 'host4'},
        '4': {'host': 'host5'},
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
    _initial_assignment = dict(
        [
            ((u'T0', 0), ['1', '2']),
            ((u'T0', 1), ['2', '3']),
            ((u'T1', 0), ['0', '1', '2', '3']),
            ((u'T1', 1), ['0', '1', '2', '4']),
            ((u'T2', 0), ['2']),
            ((u'T3', 0), ['0', '1', '2']),
            ((u'T3', 1), ['0', '1', '4']),
        ]
    )

    def get_replication_group_id(self, broker):
        try:
            return self.broker_rg[broker.id]
        except KeyError:  # inactive brokers
            return None

    def build_cluster_topology(self, assignment=None, brokers=None):
        """Create cluster topology from given assignment."""
        if not assignment:
            assignment = self._initial_assignment
        if not brokers:
            brokers = self.brokers
        return ClusterTopology(assignment, brokers, self.get_replication_group_id)

    def test_cluster_topology_inactive_brokers(self):
        assignment = {
            (u'T0', 0): ['0', '1'],
            (u'T0', 1): ['8', '9'],  # 8 and 9 are not in active brokers
        }
        brokers = {
            '0': {'host': 'host0'},
            '1': {'host': 'host1'},
        }

        def extract_group(broker):
            # group 0 for broker 0
            # group 1 for broker 1
            # None for inactive brokers
            if broker in brokers:
                return broker.id
            return None

        ct = ClusterTopology(assignment, brokers, extract_group)
        assert ct.brokers['8'].inactive
        assert ct.brokers['9'].inactive
        assert None in ct.rgs

    def test_broker_decommission(self):
        assignment = {
            (u'T0', 0): ['0', '2'],
            (u'T0', 1): ['0', '3'],
            (u'T1', 0): ['0', '5'],
        }
        ct = self.build_cluster_topology(assignment)
        partitions_count = len(ct.partitions)

        # should move all partitions from broker 0 to either 1 or 4 because they
        # are in the same replication group and empty.
        ct.decommission_brokers(['0'])

        # Here we just care that broker 0 is empty and partitions count didn't
        # change
        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()

    def test_broker_decommission_many(self):
        assignment = {
            (u'T0', 0): ['0', '2'],
            (u'T0', 1): ['0', '3'],
            (u'T1', 0): ['0', '5'],
            (u'T1', 1): ['1', '5'],
            (u'T1', 2): ['1', '5'],
            (u'T2', 0): ['0', '3'],
        }
        ct = self.build_cluster_topology(assignment)
        partitions_count = len(ct.partitions)

        ct.decommission_brokers(['0', '1', '3'])

        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()
        assert ct.brokers['1'].empty()
        assert ct.brokers['3'].empty()
        # All partitions from 0 and 1 should now be in 4
        assert len(ct.brokers['4'].partitions) == 6
        # All partitions from 3 should now be in 2
        assert len(ct.brokers['2'].partitions) == 3

    def test_broker_decommission_failover(self):
        assignment = {
            (u'T0', 0): ['0', '1', '2'],
            (u'T0', 1): ['0', '1', '2'],
            (u'T1', 0): ['0', '1'],
            (u'T1', 1): ['1', '5'],
            (u'T1', 2): ['1', '5'],
            (u'T2', 0): ['0', '3'],
        }
        ct = self.build_cluster_topology(assignment)
        partitions_count = len(ct.partitions)

        ct.decommission_brokers(['0'])

        # Partition T00, T01, and T10 should move to 5 and 6
        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()

    def test_broker_decommission_error(self):
        assignment = {
            (u'T1', 0): ['0', '1', '2', '3', '4'],
            (u'T1', 1): ['0', '1', '2', '4'],
            (u'T2', 1): ['0', '1', '2', '4'],
        }
        ct = self.build_cluster_topology(assignment)

        with pytest.raises(BrokerDecommissionError):
            ct.decommission_brokers(['0'])

    def test_rebalance_replication_groups(self):
        ct = self.build_cluster_topology()
        ct.rebalance_replication_groups()
        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )

        # Verify that rg-group-balanced
        assert net_imbal == 0

        # Verify that new-assignment is valid
        self.assert_valid(
            ct.assignment,
            self._initial_assignment,
            ct.brokers.keys(),
        )

    def test_rebalance_replication_groups_balanced(self):
        # Replication-group is already balanced
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T0', 1), ['0', '3']),
                ((u'T2', 0), ['2']),
                ((u'T3', 0), ['0', '1', '2']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(5))
        ct.rebalance_replication_groups()
        net_imbal, _ = get_replication_group_imbalance_stats(
            ct.rgs.values(),
            ct.partitions.values(),
        )

        # Verify that rg-group-balanced
        assert net_imbal == 0
        # Verify that new-assignment same as previous
        assert ct.assignment == assignment

    def test_rebalance_replication_groups_error(self):
        assignment = dict(
            [
                ((u'T0', 0), ['0', '2']),
                ((u'T0', 1), ['0', '3']),
                ((u'T2', 0), ['2']),
                ((u'T3', 0), ['0', '1', '9']),  # broker 9 is not active
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(5))

        with pytest.raises(RebalanceError):
            ct.rebalance_replication_groups()

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
        p1 = ((u'T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
        assignment = dict([p1])
        ct = self.build_cluster_topology(assignment, self.srange(7))
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
        p1_info = ((u'T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
        assignment = dict([p1_info])
        ct = self.build_cluster_topology(assignment, self.srange(7))
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
        # No eligible dest-group is there where partition can be sent to
        assert rg_dest is None

    def test_rebalance_partition_imbalanced_case1(self):
        # Test imbalanced partitions for below cases
        # Note: In initial-assignment, all partitions with id-1 are 'imbalanced'
        ct = self.build_cluster_topology()
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
        ct = self.build_cluster_topology()
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
        ct = self.build_cluster_topology()
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
        ct = self.build_cluster_topology()
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
        return {str(x): {"host": "host%s" % x} for x in range(n)}

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
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
                ((u'T1', 0), ['0', '2']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(3))
        orig_assignment = ct.assignment
        ct.rebalance_leaders()
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())

        # No changed in already-balanced assignment
        assert orig_assignment == ct.assignment
        # Assert leader-balanced
        assert net_imbal == 0

    def test_rebalance_leaders_balanced_case2(self):
        # Already balanced-assignment NOT evenly-distributed
        # (broker-id: leader-count): {0: 1, 1:1, 2:1}
        # opt-count: 2/3 = 0, extra-count: 2%3 = 2
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(3))
        orig_assignment = ct.assignment
        ct.rebalance_leaders()
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())

        # No changed in already-balanced assignment
        assert orig_assignment == ct.assignment
        # Assert leader-balanced
        assert net_imbal == 0

    def test_rebalance_leaders_unbalanced_case1(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(3))
        orig_assignment = ct.assignment
        ct.rebalance_leaders()

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

    def test_rebalance_leaders_unbalanced_case2(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(3))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2a(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.rebalance_leaders()

        # Verify balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0
        # Verify that (T0, 1) also swapped even if 1 and 3 were balanced
        # Rebalancing through non-followers
        replica_ids = [b.id for b in ct.partitions[('T0', 1)].replicas]
        assert replica_ids == ['3', '1']

    def test_rebalance_leaders_unbalanced_case2b(self):
        assignment = dict(
            [
                ((u'T0', 0), ['3', '2']),
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '1']),
                ((u'T2', 0), ['0']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2c(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2d(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(3))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case2e(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(6))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case3(self):
        # Imbalanced 0 and 2. No re-balance possible.
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0']),
                ((u'T2', 0), ['0']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(3))
        ct.rebalance_leaders()

        # Verify still leader-imbalanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 1
        # No change in assignment
        assert sorted(ct.assignment) == sorted(assignment)

    def test_rebalance_leaders_unbalanced_case4(self):
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

        ct = self.build_cluster_topology(assignment, self.srange(3))
        _, net_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        ct.rebalance_leaders()
        _, new_net_imbal, new_leaders_per_broker = get_leader_imbalance_stats(
            ct.brokers.values(),
        )
        # Verify that net-imbalance has reduced but not zero
        assert new_net_imbal > 0 and new_net_imbal < net_imbal
        # Verify the changes in leaders-per-broker count
        assert new_leaders_per_broker['2'] == 1
        assert new_leaders_per_broker['1'] == 1
        assert new_leaders_per_broker['0'] == 3

    def test_rebalance_leaders_unbalanced_case2f(self):
        assignment = dict(
            [
                ((u'T0', 0), ['2', '0']),
                ((u'T1', 0), ['2', '0']),
                ((u'T1', 1), ['0']),
                ((u'T2', 0), ['1']),
                ((u'T2', 1), ['2']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(3))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test_rebalance_leaders_unbalanced_case5(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.rebalance_leaders()

        # Verify leader-balanced
        _, leader_imbal, _ = get_leader_imbalance_stats(ct.brokers.values())
        assert leader_imbal == 0

    def test__rebalance_groups_partition_cnt_case1(self):
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
        ct = self.build_cluster_topology(assignment, self.srange(4))
        # Re-balance replication-groups for partition-count
        ct._rebalance_groups_partition_cnt()

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

    def test__rebalance_groups_partition_cnt_case2(self):
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
        ct = self.build_cluster_topology(assignment, brokers)
        # Re-balance brokers
        ct._rebalance_groups_partition_cnt()

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

    def test__rebalance_groups_partition_cnt_case3(self):
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
        ct = self.build_cluster_topology(assignment, brokers)
        # Re-balance brokers across replication-groups
        ct._rebalance_groups_partition_cnt()

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

    def test_update_cluster_topology_invalid_broker(self):
        assignment = dict([((u'T0', 0), ['1', '2'])])
        new_assignment = dict([((u'T0', 0), ['1', '3'])])
        ct = self.build_cluster_topology(assignment, self.srange(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.update_cluster_topology(new_assignment)

    def test_update_cluster_topology_invalid_partition(self):
        assignment = dict([((u'T0', 0), ['1', '2'])])
        new_assignment = dict([((u'invalid_topic', 0), ['1', '0'])])
        ct = self.build_cluster_topology(assignment, self.srange(3))

        with pytest.raises(InvalidPartitionError):
            ct.update_cluster_topology(new_assignment)

    def test_update_cluster_topology(self):
        assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['2', '0']),
                ((u'T1', 0), ['0', '2']),
            ]
        )
        new_assignment = dict(
            [
                ((u'T0', 0), ['1', '2']),
                ((u'T0', 1), ['1', '2']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(3))

        ct.update_cluster_topology(new_assignment)

        # Verify updates of partition and broker objects
        r_T0_0 = [b.id for b in ct.partitions[(u'T0', 0)].replicas]
        r_T0_1 = [b.id for b in ct.partitions[(u'T0', 1)].replicas]
        r_T1_0 = [b.id for b in ct.partitions[(u'T1', 0)].replicas]
        assert r_T0_0 == ['1', '2']
        assert r_T0_1 == ['1', '2']
        assert r_T1_0 == ['0', '2']

        # Assert partitions of brokers get updated
        assert ct.brokers['0'].partitions == set([
            ct.partitions[(u'T1', 0)],
        ])

        assert ct.brokers['1'].partitions == set([
            ct.partitions[(u'T0', 0)],
            ct.partitions[(u'T0', 1)],
        ])

        assert ct.brokers['2'].partitions == set([
            ct.partitions[(u'T0', 0)],
            ct.partitions[(u'T0', 1)],
            ct.partitions[(u'T1', 0)],
        ])

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
            set(orig_replicas) == set(new_assignment[partition])

    def test_replace_broker_leader(self):
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '2']),
                ((u'T2', 0), ['1']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.replace_broker('1', '3')

        assert ct.brokers['1'].partitions == set([])
        assert ct.brokers['3'].partitions == set([
            ct.partitions[(u'T1', 0)],
            ct.partitions[(u'T2', 0)],
        ])
        assert ct.partitions[(u'T1', 0)].replicas == [
            ct.brokers['3'],
            ct.brokers['2'],
        ]

    def test_replace_broker_non_leader(self):
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T2', 0), ['1']),
            ]
        )
        ct = self.build_cluster_topology(assignment, self.srange(4))
        ct.replace_broker('2', '0')

        assert ct.brokers['2'].partitions == set([])
        assert ct.brokers['0'].partitions == set([ct.partitions[(u'T1', 0)]])
        assert ct.partitions[(u'T1', 0)].replicas == [
            ct.brokers['1'],
            ct.brokers['0'],
        ]

    def test_replace_broker_invalid_source_broker(self):
        assignment = dict([((u'T1', 0), ['0', '1'])])
        ct = self.build_cluster_topology(assignment, self.srange(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.replace_broker('444', '2')

    def test_replace_broker_invalid_destination_broker(self):
        assignment = dict([((u'T1', 0), ['0', '1'])])
        ct = self.build_cluster_topology(assignment, self.srange(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.replace_broker('0', '444')
