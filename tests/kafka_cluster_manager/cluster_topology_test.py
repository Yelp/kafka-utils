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
import pytest

from .helper import broker_range
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import InvalidBrokerIdError
from kafka_utils.kafka_cluster_manager.cluster_info \
    .error import InvalidPartitionError


class TestClusterTopology(object):

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

    def test_replace_broker_leader(self, create_cluster_topology):
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T1', 1), ['0', '2']),
                ((u'T2', 0), ['1']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))

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

    def test_replace_broker_non_leader(self, create_cluster_topology):
        assignment = dict(
            [
                ((u'T1', 0), ['1', '2']),
                ((u'T2', 0), ['1']),
            ]
        )
        ct = create_cluster_topology(assignment, broker_range(4))
        ct.replace_broker('2', '0')

        assert ct.brokers['2'].partitions == set([])
        assert ct.brokers['0'].partitions == set([ct.partitions[(u'T1', 0)]])
        assert ct.partitions[(u'T1', 0)].replicas == [
            ct.brokers['1'],
            ct.brokers['0'],
        ]

    def test_replace_broker_invalid_source_broker(self, create_cluster_topology):
        assignment = dict([((u'T1', 0), ['0', '1'])])
        ct = create_cluster_topology(assignment, broker_range(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.replace_broker('444', '2')

    def test_replace_broker_invalid_destination_broker(self, create_cluster_topology):
        assignment = dict([((u'T1', 0), ['0', '1'])])
        ct = create_cluster_topology(assignment, broker_range(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.replace_broker('0', '444')

    def test_cluster_topology_inactive_brokers(
            self,
            create_cluster_topology,
    ):
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

        ct = create_cluster_topology(assignment, brokers, extract_group)
        assert ct.brokers['8'].inactive
        assert ct.brokers['9'].inactive
        assert None in ct.rgs

    def test_update_cluster_topology_invalid_broker(
            self,
            create_cluster_topology,
    ):
        assignment = dict([((u'T0', 0), ['1', '2'])])
        new_assignment = dict([((u'T0', 0), ['1', '3'])])
        ct = create_cluster_topology(assignment, broker_range(3))

        with pytest.raises(InvalidBrokerIdError):
            ct.update_cluster_topology(new_assignment)

    def test_update_cluster_topology_invalid_partition(
            self,
            create_cluster_topology,
    ):
        assignment = dict([((u'T0', 0), ['1', '2'])])
        new_assignment = dict([((u'invalid_topic', 0), ['1', '0'])])
        ct = create_cluster_topology(assignment, broker_range(3))

        with pytest.raises(InvalidPartitionError):
            ct.update_cluster_topology(new_assignment)

    def test_update_cluster_topology(
            self,
            create_cluster_topology,
    ):
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
        ct = create_cluster_topology(assignment, broker_range(3))

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
