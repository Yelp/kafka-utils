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
from mock import Mock
from mock import sentinel

from .helper import create_and_attach_partition
from .helper import create_broker
from kafka_utils.kafka_cluster_manager.cluster_info.broker import Broker
from kafka_utils.kafka_cluster_manager.cluster_info.error import \
    EmptyReplicationGroupError
from kafka_utils.kafka_cluster_manager.cluster_info.error import \
    NotEligibleGroupError
from kafka_utils.kafka_cluster_manager.cluster_info.rg import \
    ReplicationGroup
from kafka_utils.kafka_cluster_manager.cluster_info.topic import Topic


@pytest.fixture
def rg_unbalanced(create_partition):
    # Broker Topics:Partition
    #   1    topic1:0, topic1:1, topic4:0
    #   2    topic1:2, topic1:3, topic2:0, topic2:1, topic3:0
    #   3    topic4:0
    # total partitions: 9
    p10 = create_partition('topic1', 0)
    p11 = create_partition('topic1', 1)
    p12 = create_partition('topic1', 2)
    p13 = create_partition('topic1', 3)
    p20 = create_partition('topic2', 0)
    p21 = create_partition('topic2', 1)
    p30 = create_partition('topic3', 0)
    p40 = create_partition('topic4', 0, replication_factor=2)
    p50 = create_partition('topic5', 0)
    b1 = create_broker('b1', [p10, p12, p13, p21])
    b2 = create_broker('b2', [p11, p20, p30, p40])
    b3 = create_broker('b3', [p40])
    b4 = create_broker('b4', [p50])
    return ReplicationGroup('test_rg', set([b1, b2, b3, b4]))


@pytest.fixture
def rg_balanced(create_partition):
    # Broker Topics:Partition
    #   1    topic1:0, topic1:1, topic2:0, topic4:0
    #   2    topic1:2, topic1:3, topic2:1
    #   3    topic2:1, topic3:0, topic4:0
    # total partitions: 9
    p10 = create_partition('topic1', 0)
    p11 = create_partition('topic1', 1)
    p12 = create_partition('topic1', 2)
    p13 = create_partition('topic1', 3)
    p20 = create_partition('topic2', 0)
    p21 = create_partition('topic2', 1)
    p30 = create_partition('topic3', 0)
    p40 = create_partition('topic4', 0, replication_factor=2)
    b1 = create_broker('b1', [p10, p11, p20, p40])
    b2 = create_broker('b2', [p12, p13, p21])
    b3 = create_broker('b3', [p21, p30, p40])
    return ReplicationGroup('test_rg', set([b1, b2, b3]))


def assert_rg_balanced(rg):
    expected_count = len(rg.partitions) // len(rg.brokers)
    for broker in rg.brokers:
        if not broker.decommissioned:
            assert len(broker.partitions) in (expected_count, expected_count + 1)


class TestReplicationGroup(object):

    def test_rebalance_brokers(self, rg_unbalanced):
        orig_partitions = rg_unbalanced.partitions

        rg_unbalanced.rebalance_brokers()

        # No partitions are missing
        assert sorted(orig_partitions) == sorted(rg_unbalanced.partitions)
        assert_rg_balanced(rg_unbalanced)

    def test_rebalance_brokers_one_inactive(
        self,
        rg_unbalanced,
        create_partition,
    ):
        expected_count = \
            len(rg_unbalanced.partitions) // len(rg_unbalanced.brokers)
        p51 = create_partition('topic5', 1)
        b4 = create_broker('b4', [p51])
        b4.mark_inactive()
        rg_unbalanced.add_broker(b4)
        orig_partitions = rg_unbalanced.partitions

        rg_unbalanced.rebalance_brokers()

        # No partitions are missing
        assert sorted(orig_partitions) == sorted(rg_unbalanced.partitions)
        # b4 has not changed
        assert b4.partitions == set([p51])
        for broker in rg_unbalanced.brokers:
            if not broker.decommissioned and not broker.inactive:
                assert len(broker.partitions) in (
                    expected_count,
                    expected_count + 1
                )

    def test_rebalance_brokers_balanced(self, rg_balanced):
        expected = {b: b.partitions for b in rg_balanced.brokers}

        rg_balanced.rebalance_brokers()

        # there shouldn't be any partition movement
        assert expected == {b: b.partitions for b in rg_balanced.brokers}
        assert_rg_balanced(rg_balanced)

    def test_rebalance_decommissioned_broker(
        self,
        rg_balanced,
        create_partition,
    ):
        # rg_balanced is supposed to have 3 brokers with 3 partitions each. We
        # add another broker with 3 more partitions
        broker_count = len(rg_balanced.brokers)
        p50 = create_partition('topic5', 0)
        p51 = create_partition('topic5', 1)
        p52 = create_partition('topic5', 2)
        broker = create_broker('broker4', [p50, p51, p52])
        rg_balanced.add_broker(broker)

        broker.mark_decommissioned()
        orig_partitions = rg_balanced.partitions

        rg_balanced.rebalance_brokers()

        assert broker.empty() is True
        assert sorted(orig_partitions) == sorted(rg_balanced.partitions)
        expected_count = len(rg_balanced.partitions) // broker_count
        for broker in rg_balanced.brokers:
            if not broker.decommissioned:
                assert len(broker.partitions) in (
                    expected_count,
                    expected_count + 1,
                )

    def test_rebalance_new_empty_broker(self, rg_balanced):
        broker = Broker('4')
        rg_balanced.add_broker(broker)
        orig_partitions = rg_balanced.partitions

        rg_balanced.rebalance_brokers()

        assert sorted(orig_partitions) == sorted(rg_balanced.partitions)
        assert_rg_balanced(rg_balanced)

    def test_rebalance_balanced_inactive_broker(self, rg_balanced):
        expected = {b: b.partitions for b in rg_balanced.brokers}
        list(rg_balanced.brokers)[0].mark_inactive()

        rg_balanced.rebalance_brokers()

        assert expected == {b: b.partitions for b in rg_balanced.brokers}
        assert_rg_balanced(rg_balanced)

    def test_decommission_no_remaining_brokers(self, create_partition):
        p10 = create_partition('topic1', 0)
        p11 = create_partition('topic1', 1)
        p20 = create_partition('topic2', 0)
        b1 = create_broker('b1', [p10, p11, p20])
        b2 = create_broker('b2', [])
        b2.mark_inactive()
        rg = ReplicationGroup('rg', set([b1, b2]))
        b1.mark_decommissioned()

        # Two brokers b1 decommissioned b2 inactive
        with pytest.raises(EmptyReplicationGroupError):
            rg.rebalance_brokers()

    def test_decommission_not_enough_replicas(self, create_partition):
        p10 = create_partition('topic1', 0)
        p11 = create_partition('topic1', 1)
        p20 = create_partition('topic2', 0, replication_factor=2)
        b1 = create_broker('b1', [p10, p11, p20])
        b2 = create_broker('b1', [p20])
        rg = ReplicationGroup('rg', set([b1, b2]))
        b2.mark_decommissioned()

        rg.rebalance_brokers()

        assert not b2.empty()

    def test_rebalance_empty_replication_group(self):
        rg = ReplicationGroup('empty_rg')

        with pytest.raises(EmptyReplicationGroupError):
            rg.rebalance_brokers()

    def test_decommission_an_inactive_broker(
        self,
        rg_balanced,
        create_partition,
    ):
        # rg_balanced is supposed to have 3 brokers with 3 partitions each. We
        # add another broker with 3 more partitions
        broker_count = len(rg_balanced.brokers)
        p50 = create_partition('topic5', 0)
        p51 = create_partition('topic5', 1)
        p52 = create_partition('topic5', 2)
        broker = create_broker('broker4', [p50, p51, p52])
        rg_balanced.add_broker(broker)

        broker.mark_decommissioned()
        broker.mark_inactive()
        orig_partitions = rg_balanced.partitions

        rg_balanced.rebalance_brokers()

        assert broker.empty() is True
        assert sorted(orig_partitions) == sorted(rg_balanced.partitions)
        expected_count = len(rg_balanced.partitions) // broker_count
        for broker in rg_balanced.brokers:
            if not broker.decommissioned:
                assert len(broker.partitions) in (
                    expected_count,
                    expected_count + 1,
                )

    def test_add_broker_empty(self):
        rg = ReplicationGroup('test_rg', None)
        rg.add_broker(sentinel.broker)

        assert set([sentinel.broker]) == rg.brokers

    def test_invalid_brokers_type_list(self):
        with pytest.raises(TypeError):
            ReplicationGroup('test_rg', [sentinel.broker1, sentinel.broker2])

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
        mock_brokers = set([
            Mock(spec=Broker, partitions=set([sentinel.p1, sentinel.p2])),
            Mock(spec=Broker, partitions=set([sentinel.p3, sentinel.p1])),
        ])
        rg = ReplicationGroup('test_rg', mock_brokers)
        expected = [
            sentinel.p1,
            sentinel.p2,
            sentinel.p3,
            sentinel.p1,
        ]

        assert sorted(expected) == sorted(rg.partitions)

    def test_acquire_partition(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p12 = create_partition('t1', 2)
        p13 = create_partition('t1', 3)
        p20 = create_partition('t2', 0)
        p21 = create_partition('t2', 1)
        b1 = create_broker('b1', [p10, p11, p20])
        b2 = create_broker('b2', [p12, p21])
        rg = ReplicationGroup('test_rg', set([b1, b2]))
        b3 = create_broker('b3', [p13])

        rg.acquire_partition(p13, b3)

        assert b3.empty()
        assert p13 in b2.partitions

    def test_acquire_partition_error(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p20 = create_partition('t2', 0)
        b1 = create_broker('b1', [p10, p11, p20])
        rg = ReplicationGroup('test_rg', set([b1]))
        b3 = create_broker('b3', [p11])

        # Not eligible because broker b3 has already a replica of partition p11
        with pytest.raises(NotEligibleGroupError):
            rg.acquire_partition(p11, b3)

    def test__elect_source_broker(self, create_partition):
        p10 = create_partition('t1', 0)
        p20 = create_partition('t2', 0)
        p11 = create_partition('t1', 1)
        b1 = create_broker('b1', [p10, p11])  # b1 -> t1: {0,1}, t2: 0
        p30 = create_partition('t3', 0)
        b2 = create_broker('b2', [p10, p20, p30])  # b2 -> t1: 0, t2: 1, t3: 0
        rg = ReplicationGroup('test_rg', set([b1, b2]))

        # b1 has 2 partitions (p1 and p3) for same topic t1
        # b2 has only 1 partition (p1) for topic t1
        # source broker should be b1 to reduce the number of partitions of the
        # same topic
        victim_partition = p10
        actual = rg._elect_source_broker(victim_partition)
        assert actual == b1

    def test__elect_dest_broker(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p20 = create_partition('t2', 0)
        p30 = create_partition('t3', 0)
        b1 = create_broker('b1', [p10, p20, p11])  # b1 -> t1: {0,1}, t2: 0
        b2 = create_broker('b2', [p10, p30])  # b2 -> t1: 0, t3: 0
        rg = ReplicationGroup('test_rg', set([b1, b2]))

        # Since p30 is already in b2 so the preferred destination will be b1
        # although b2 has less partitions.
        victim_partition = p30
        actual = rg._elect_dest_broker(victim_partition)
        assert actual == b1

    def test__elect_dest_broker_prefer_less_siblings(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p20 = create_partition('t2', 0)
        b1 = create_broker('b1', [p10, p11, p20])  # b1 -> t1: {0,1}, t2: 0
        p30 = create_partition('t3', 0)
        p31 = create_partition('t3', 1)
        p32 = create_partition('t3', 2)
        b2 = create_broker('b2', [p10, p30, p31, p32])  # b2 -> t1: 0, t3: 0
        rg = ReplicationGroup('test_rg', set([b1, b2]))

        # Since t1 has two partitions in b1 but only one in b2,
        # the preferred destination should be b2
        victim_partition_p12 = create_partition('t1', 2)
        actual = rg._elect_dest_broker(victim_partition_p12)
        assert actual == b2

    def test__elect_dest_broker_partition_conflict(self, create_partition):
        p1 = create_partition('t1', 0)
        p2 = create_partition('t2', 0)
        p3 = create_partition('t1', 1)
        b1 = create_broker('b1', [p1, p2, p3])  # b1 -> t1: {0,1}, t2: 0
        rg = ReplicationGroup('test_rg', set([b1]))
        # p1 already exists in b1
        # This should never happen and we expect the application to fail badly
        victim_partition = p1
        assert None is rg._elect_dest_broker(victim_partition)

    def test_count_replica(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p12 = create_partition('t1', 2)
        p13 = create_partition('t1', 3)
        b1 = create_broker('b1', [p10, p11, p12])
        b2 = create_broker('b2', [p10, p13])
        rg = ReplicationGroup('test_rg', set([b1, b2]))

        assert rg.count_replica(p10) == 2
        assert rg.count_replica(p11) == 1
        assert rg.count_replica(p12) == 1
        assert rg.count_replica(p13) == 1
        assert rg.count_replica(create_partition('t1', 4)) == 0

    def test__select_broker_pair(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p20 = create_partition('t2', 0)
        b0 = create_broker('b0', [p10, p20, p11])
        b1 = create_broker('b1', [p10, p20])
        b2 = create_broker('b2', [p20, p11])
        b3 = create_broker('b3', [p10, p20, p11])
        rg_source = ReplicationGroup('rg1', set([b0, b1, b2, b3]))
        b4 = create_broker('b4', [p20, p11])
        b5 = create_broker('b5', [p20])
        b6 = create_broker('b6', [p10])
        b7 = create_broker('b7', [p11])
        rg_dest = ReplicationGroup('rg2', set([b4, b5, b6, b7]))

        # Select best-suitable brokers for moving partition 'p10'
        broker_source, broker_dest = rg_source._select_broker_pair(rg_dest, p10)

        # source-broker can't be b2 since it doesn't have p10
        # source-broker shouldn't be b1 since it has lesser partitions
        # than b3 and b0
        assert broker_source in [b0, b3]
        # dest-broker shouldn't be b4 since it has more partitions than b4
        # dest-broker can't be b6 since it has already partition p10
        assert broker_dest in [b5, b7]

    def test_move_partition(self, create_partition):
        p10 = create_partition('t1', 0)
        p11 = create_partition('t1', 1)
        p20 = create_partition('t2', 0)
        b1 = create_broker('b1', [p10, p20, p11])
        b2 = create_broker('b2', [p10, p11])
        # 2 p1 replicas are in rg_source
        rg_source = ReplicationGroup('rg1', set([b1, b2]))
        b3 = create_broker('b3', [p10, p11])
        b4 = create_broker('b4', [p20])
        # 1 p1 replica is in rg_dest
        rg_dest = ReplicationGroup('rg2', set([b3, b4]))

        # Move partition p1 from rg1 to rg2
        rg_source.move_partition(rg_dest, p10)

        # partition-count of p1 for rg1 should be 1
        assert rg_source.count_replica(p10) == 1
        # partition-count of p1 for rg2 should be 2
        assert rg_dest.count_replica(p10) == 2
        # Rest of the partitions are untouched
        assert rg_source.count_replica(p20) == 1
        assert rg_dest.count_replica(p20) == 1
        assert rg_source.count_replica(p11) == 2
        assert rg_dest.count_replica(p11) == 1

    def test__get_target_brokers_1(self, create_partition):
        p10 = create_partition('topic1', 0, replication_factor=2)
        p11 = create_partition('topic1', 1, replication_factor=2)
        p20 = create_partition('topic2', 0, replication_factor=2)
        p30 = create_partition('topic3', 0)
        b1 = create_broker('b1', [p10, p11, p20, p30])
        b2 = create_broker('b2', [p10, p20])
        rg = ReplicationGroup('rg', set([b1, b2]))

        over_loaded = [b1]
        under_loaded = [b2]
        target = rg._get_target_brokers(
            over_loaded,
            under_loaded,
            rg.generate_sibling_distance(),
        )

        # p30 is the best fit because topic3 doesn't have any partition in
        # broker 2
        assert target == (b1, b2, p30) or target == (b1, b2, p11)

    def test_get_target_brokers_2(self, create_partition):
        p10 = create_partition('topic1', 0, replication_factor=2)
        p11 = create_partition('topic1', 1, replication_factor=2)
        p20 = create_partition('topic2', 0, replication_factor=1)
        p21 = create_partition('topic2', 1, replication_factor=1)
        p30 = create_partition('topic3', 0)
        b1 = create_broker('b1', [p10, p11, p20])
        b2 = create_broker('b2', [p10, p30])
        b3 = create_broker('b3', [p21])
        rg = ReplicationGroup('rg', set([b1, b2, b3]))

        over_loaded = [b1]
        under_loaded = [b2, b3]

        target = rg._get_target_brokers(
            over_loaded,
            under_loaded,
            rg.generate_sibling_distance(),
        )

        # b3 is the best broker fit because of number of partition
        # Both p10 and p11 are a good fit. p20 can't be considered because it is
        # already in b3.
        assert target == (b1, b3, p10) or target == (b1, b3, p11)

    def test_get_target_brokers_3(self, create_partition):
        p10 = create_partition('topic1', 0)
        p11 = create_partition('topic1', 1)
        p12 = create_partition('topic1', 2)
        p20 = create_partition('topic2', 0)
        p21 = create_partition('topic2', 1)
        p30 = create_partition('topic3', 0)
        p31 = create_partition('topic3', 1)
        b1 = create_broker('b1', [p10, p11, p20])
        b2 = create_broker('b2', [p12, p21, p30, p31])
        b3 = create_broker('b3', [])
        rg = ReplicationGroup('rg', set([b1, b2, b3]))

        over_loaded = [b1, b2]
        under_loaded = [b3]

        target = rg._get_target_brokers(
            over_loaded,
            under_loaded,
            rg.generate_sibling_distance(),
        )

        # b3 is the best broker fit because of number of partition
        # Both p10 and p11 are a good fit. p20 can't be considered because it is
        # already in b3.
        assert target == (b2, b3, p30) or target == (b2, b3, p31)

    def test_generate_sibling_count(self):
        t1 = Topic('topic1', 2)
        t2 = Topic('topic2', 2)
        t3 = Topic('topic3', 1)
        p10 = create_and_attach_partition(t1, 0)
        p11 = create_and_attach_partition(t1, 1)
        p12 = create_and_attach_partition(t1, 2)
        p20 = create_and_attach_partition(t2, 0)
        p21 = create_and_attach_partition(t2, 1)
        p22 = create_and_attach_partition(t2, 2)
        p30 = create_and_attach_partition(t3, 0)
        p31 = create_and_attach_partition(t3, 1)
        b1 = create_broker('b1', [p10, p11, p20, p21, p30, p31])
        b2 = create_broker('b2', [p12, p21, p22])
        b3 = create_broker('b3', [p10, p11, p22])
        rg = ReplicationGroup('rg', set([b1, b2, b3]))

        expected = {
            b1: {b2: {t1: 1, t2: 0}, b3: {t1: 0, t2: 1}},
            b2: {b1: {t1: -1, t2: 0, t3: -2}, b3: {t1: -1, t2: 1}},
            b3: {b1: {t1: 0, t2: -1, t3: -2}, b2: {t1: 1, t2: -1}},
        }
        actual = rg.generate_sibling_distance()

        assert dict(actual) == expected

    def test_update_sibling_count(self):
        t1 = Topic('topic1', 2)
        t2 = Topic('topic2', 2)
        t3 = Topic('topic3', 1)
        p10 = create_and_attach_partition(t1, 0)
        p11 = create_and_attach_partition(t1, 1)
        p12 = create_and_attach_partition(t1, 2)
        p20 = create_and_attach_partition(t2, 0)
        p21 = create_and_attach_partition(t2, 1)
        p22 = create_and_attach_partition(t2, 2)
        p30 = create_and_attach_partition(t3, 0)
        p31 = create_and_attach_partition(t3, 1)
        b1 = create_broker('b1', [p10, p11, p20, p21, p30, p31])
        b2 = create_broker('b2', [p12, p21, p22])
        b3 = create_broker('b3', [p10, p11, p22])
        rg = ReplicationGroup('rg', set([b1, b2, b3]))
        sibling_distance = {
            b2: {
                b1: {t1: -1, t2: 0, t3: -2},
                b3: {t1: 1, t2: -1, t3: -2}
            },
        }
        # Move a p10 from b1 to b2
        b1.move_partition(p10, b2)

        # NOTE: b2: b1: t1: -1 -> 1 and b2: b3: t1: 1 -> 0
        expected = {
            b2: {
                b1: {t1: 1, t2: 0, t3: -2},
                b3: {t1: 0, t2: -1, t3: -2},
            },
        }
        actual = rg.update_sibling_distance(sibling_distance, b2, t1)

        assert dict(actual) == expected
