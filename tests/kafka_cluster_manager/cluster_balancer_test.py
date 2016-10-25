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
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import BrokerDecommissionError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .partition_count_balancer import PartitionCountBalancer


class TestClusterBalancer(object):

    @pytest.fixture(params=[PartitionCountBalancer])
    def create_balancer(self, request):
        def build_balancer(cluster_topology, **kwargs):
            args = mock.Mock(**kwargs)
            return request.param(cluster_topology, args)
        return build_balancer

    def test_broker_decommission(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            (u'T0', 0): ['0', '2'],
            (u'T0', 1): ['0', '3'],
            (u'T1', 0): ['0', '5'],
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
            (u'T0', 0): ['0', '2'],
            (u'T0', 1): ['0', '3'],
            (u'T1', 0): ['0', '5'],
            (u'T1', 1): ['1', '5'],
            (u'T1', 2): ['1', '5'],
            (u'T2', 0): ['0', '3'],
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
            (u'T0', 0): ['0', '1', '2'],
            (u'T0', 1): ['0', '1', '2'],
            (u'T1', 0): ['0', '1'],
            (u'T1', 1): ['1', '4'],
            (u'T1', 2): ['1', '4'],
            (u'T2', 0): ['0', '3'],
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
            (u'T0', 0): ['0', '1', '2'],
            (u'T0', 1): ['0', '1', '2'],
            (u'T1', 0): ['0', '1'],
            (u'T1', 1): ['1', '2'],
            (u'T1', 2): ['1', '2'],
            (u'T2', 0): ['0', '2'],
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

    def test_broker_decommission_error(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = {
            (u'T1', 0): ['0', '1', '2', '3', '4'],
            (u'T1', 1): ['0', '1', '2', '4'],
            (u'T2', 1): ['0', '1', '2', '4'],
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
        assignment = dict([((u'T1', 0), ['1', '3'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[(u'T1', 0)]

        cb = create_balancer(ct)
        cb.add_replica(partition.name, count=3)

        assert partition.replication_factor == 5
        assert sum(rg.count_replica(partition) for rg in ct.rgs.values()) == 5

    def test_remove_replica(
            self,
            create_balancer,
            create_cluster_topology,
    ):
        assignment = dict([((u'T1', 0), ['0', '1', '2', '3', '5'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[(u'T1', 0)]
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
        assignment = dict([((u'T1', 0), ['0', '1', '2', '3', '5'])])
        ct = create_cluster_topology(assignment, broker_range(6))
        partition = ct.partitions[(u'T1', 0)]
        osr_broker_ids = ['1', '2']

        cb = create_balancer(ct)
        cb.remove_replica(partition.name, osr_broker_ids, count=2)

        assert set(b.id for b in partition.replicas) == set(['0', '3', '5'])
