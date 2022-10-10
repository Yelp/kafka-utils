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
from argparse import Namespace
from unittest import mock

import pytest

from .helper import broker_range
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import BrokerDecommissionError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .genetic_balancer import GeneticBalancer
from kafka_utils.kafka_cluster_manager.cluster_info \
    .partition_count_balancer import PartitionCountBalancer


class TestClusterBalancer:

    @pytest.fixture(params=[PartitionCountBalancer, GeneticBalancer])
    def create_balancer(self, request):
        def build_balancer(cluster_topology, **kwargs):
            args = mock.Mock(spec=Namespace)
            args.max_partition_movements = None
            args.max_movement_size = None
            args.max_leader_changes = None
            args.replication_groups = True
            args.brokers = True
            args.leaders = True
            args.balancer_args = []
            args.configure_mock(**kwargs)
            return request.param(cluster_topology, args)
        return build_balancer

    def assert_rg_balanced_partition(self, ct, partition, opt_cnt, extra_cnt=0):
        """Verify that the partition replicas are balanced across
        replication-groups.
        """
        for rg in ct.rgs.values():
            replica_cnt_rg = rg.count_replica(partition)

            # Verify evenly-balanced partition
            assert replica_cnt_rg == opt_cnt or\
                replica_cnt_rg == opt_cnt + extra_cnt

    def test_broker_decommission(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            ('T0', 0): ['0', '2'],
            ('T0', 1): ['0', '3'],
            ('T1', 0): ['0', '5'],
        }
        ct = create_cluster_topology(assignment)
        partitions_count = len(ct.partitions)

        # should move all partitions from broker 0 to either 1 or 4 because they
        # are in the same replication group and empty.
        cb = create_balancer(ct)
        cb.decommission_brokers(['0'])

        # Here we just care that broker 0 is empty and partitions count didn't
        # change
        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()

    def test_broker_decommission_many(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            ('T0', 0): ['0', '2'],
            ('T0', 1): ['0', '3'],
            ('T1', 0): ['0', '5'],
            ('T1', 1): ['1', '5'],
            ('T1', 2): ['1', '5'],
            ('T2', 0): ['0', '3'],
        }
        ct = create_cluster_topology(assignment)
        partitions_count = len(ct.partitions)

        cb = create_balancer(ct)
        cb.decommission_brokers(['0', '1', '3'])

        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()
        assert ct.brokers['1'].empty()
        assert ct.brokers['3'].empty()

    def test_broker_decommission_force(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            ('T0', 0): ['0', '1', '2'],
            ('T0', 1): ['0', '1', '2'],
            ('T1', 0): ['0', '1'],
            ('T1', 1): ['1', '4'],
            ('T1', 2): ['1', '4'],
            ('T2', 0): ['0', '3'],
        }
        # r1 b0 t00, t01, t10, t20
        # r1 b1 t00, t01, t10, t11, t12
        # r2 b2 t00, t01
        # r2 b3 t20
        # r3 b4 t11, t12
        brokers_rg = {'0': 'rg1', '1': 'rg1', '2': 'rg2', '3': 'rg2', '4': 'r3'}
        ct = create_cluster_topology(
            assignment,
            brokers_rg,
            lambda x: x.metadata,  # The value of the broker dict is the metadata attribute
        )
        partitions_count = len(ct.partitions)

        cb = create_balancer(ct)
        cb.decommission_brokers(['0'])

        assert len(ct.partitions) == partitions_count
        assert ct.brokers['0'].empty()

    def test_broker_decommission_empty_replication_group(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            ('T0', 0): ['0', '1', '2'],
            ('T0', 1): ['0', '1', '2'],
            ('T1', 0): ['0', '1'],
            ('T1', 1): ['1', '2'],
            ('T1', 2): ['1', '2'],
            ('T2', 0): ['0', '2'],
        }
        # r0 b0 t00, t01, t10, t20
        # r1 b1 t00, t01, t10, t11, t12
        # __ b2 t00, t01, t11, t12, t20 let's assume b2 is down and there is no
        # metadata for it (it was in r2 before the failure)
        # r2 b3 empty this broker came up to replace b2
        brokers_rg = {'0': 'rg0', '1': 'rg1', '3': 'rg2'}  # NOTE: b2 is not in this list

        ct = create_cluster_topology(
            assignment,
            brokers_rg,
            lambda x: x.metadata,  # The value of the broker dict is the metadata attribute
        )
        partitions_count = len(ct.partitions)

        cb = create_balancer(ct)
        cb.decommission_brokers(['2'])

        assert len(ct.partitions) == partitions_count
        assert ct.brokers['2'].empty()

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
        p1_info = (('T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
        assignment = dict([p1_info])
        ct = create_cluster_topology(assignment, broker_range(7))
        p1 = ct.partitions[p1_info[0]]
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
        p1_info = (('T0', 0), ['0', '1', '2', '3', '4', '5', '6'])
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

    def test_rebalance_partition_replicas_imbalanced_case1(
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

        cb._rebalance_partition_replicas(p1)

        # Verify partition is rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)

        # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
        # p1: replicas: ('T1',1): [0,1,2,4]
        p1 = ct.partitions[('T1', 1)]
        # Assert originally-imbalanced p1
        opt_cnt = 2    # 4/2
        cb._rebalance_partition_replicas(p1)

        # Verify partition is rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt)

    def test_rebalance_partition_replicas_imbalanced_case2(
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
        cb._rebalance_partition_replicas(p1)

        # Verify partition is now rg-balanced
        self.assert_rg_balanced_partition(ct, p1, opt_cnt, extra_cnt)

    def test_rebalance_partition_replicas_balanced(
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
        cb._rebalance_partition_replicas(p1)

        # Verify no change in replicas after rebalancing
        p1_old_replicas = p1.replicas
        cb._rebalance_partition_replicas(p1)
        assert p1_old_replicas == p1.replicas

        # (1b):  repl-count % rg-count == 0 and repl-count > rg-count
        # p2: replicas: ('T1',0): [0,1,2,3]
        p2 = ct.partitions[('T1', 0)]
        opt_cnt = 2    # 4/2
        self.assert_rg_balanced_partition(ct, p2, opt_cnt)

        # Verify no change in replicas after rebalancing
        p2_old_replicas = p2.replicas
        cb._rebalance_partition_replicas(p2)
        assert p2_old_replicas == p2.replicas

    def test_broker_decommission_error(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            ('T1', 0): ['0', '1', '2', '3', '4'],
            ('T1', 1): ['0', '1', '2', '4'],
            ('T2', 1): ['0', '1', '2', '4'],
        }
        ct = create_cluster_topology(assignment)

        with pytest.raises(BrokerDecommissionError):
            cb = create_balancer(ct)
            cb.decommission_brokers(['0'])

    def test_add_replica(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict([(('T1', 0), ['1', '3'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[('T1', 0)]

        cb = create_balancer(ct)
        cb.add_replica(partition.name, count=3)

        assert partition.replication_factor == 5
        assert sum(rg.count_replica(partition) for rg in ct.rgs.values()) == 5

    def test_remove_replica(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict([(('T1', 0), ['0', '1', '2', '3', '5'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[('T1', 0)]
        osr_broker_ids = ['1', '2']

        cb = create_balancer(ct)
        cb.remove_replica(partition.name, osr_broker_ids, count=3)

        assert partition.replication_factor == 2
        assert sum(rg.count_replica(partition) for rg in ct.rgs.values()) == 2

    def test_remove_replica_prioritize_osr(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict([(('T1', 0), ['0', '1', '2', '3', '5'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[('T1', 0)]
        osr_broker_ids = ['1', '2']

        cb = create_balancer(ct)
        cb.remove_replica(partition.name, osr_broker_ids, count=2)

        assert {b.id for b in partition.replicas} == {'0', '3', '5'}
