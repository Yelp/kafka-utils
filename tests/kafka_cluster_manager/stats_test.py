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
from math import sqrt

import kafka_utils.kafka_cluster_manager.cluster_info.stats as stats


def test_mean():
    assert stats.mean([1, 2, 3, 4, 5]) == 3


def test_variance():
    assert stats.variance([1, 2, 3, 4, 5]) == 2


def test_stdevp():
    assert stats.stdevp([1, 2, 3, 4, 5]) == sqrt(2)


def test_coefficient_of_variation():
    assert stats.coefficient_of_variation([1, 2, 3, 4, 5]) == sqrt(2) / 3


def test_get_net_imbalance_balanced_equal():
    assert stats.get_net_imbalance([3, 3, 3, 3, 3]) == 0


def test_get_net_imbalance_balanced_unequal():
    assert stats.get_net_imbalance([3, 4, 3, 4, 3]) == 0


def test_get_net_imbalance_imbalanced_equal():
    assert stats.get_net_imbalance([3, 2, 3, 4, 3]) == 1


def test_get_net_imbalance_imbalanced_unequal():
    assert stats.get_net_imbalance([3, 2, 4, 4, 4]) == 1


def test_get_extra_element_count_optimal_none_allowed():
    assert stats.get_extra_element_count(2, 2, 0) == (0, 0)


def test_get_extra_element_count_optimal_one_allowed():
    assert stats.get_extra_element_count(2, 2, 1) == (0, 1)


def test_get_extra_element_count_extra_none_allowed():
    assert stats.get_extra_element_count(3, 2, 0) == (1, 0)


def test_get_extra_element_count_extra_one_allowed():
    assert stats.get_extra_element_count(3, 2, 1) == (0, 0)


def test_get_extra_element_count_multiple_extra_one_allowed():
    assert stats.get_extra_element_count(4, 2, 1) == (1, 0)


def test_get_replication_group_imbalance_stats(create_cluster_topology):
    ct = create_cluster_topology()
    rgs = [ct.rgs['rg1'], ct.rgs['rg2']]
    partitions = list(ct.partitions.values())

    net_imbalance, extra_replica_cnt_per_rg = \
        stats.get_replication_group_imbalance_stats(rgs, partitions)

    assert extra_replica_cnt_per_rg['rg1'] == 1
    assert extra_replica_cnt_per_rg['rg2'] == 1
    assert net_imbalance == 2


def test_get_broker_partition_counts(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)

    counts = stats.get_broker_partition_counts(brokers)

    assert counts == [4, 5, 6, 3, 1]


def test_get_broker_weights(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)

    weights = stats.get_broker_weights(brokers)

    assert weights == [24.0, 26.0, 27.0, 12.0, 8.0]


def test_get_broker_leader_counts(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)

    counts = stats.get_broker_leader_counts(brokers)

    assert counts == [4, 1, 2, 0, 0]


def test_get_broker_leader_weights(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)

    weights = stats.get_broker_leader_weights(brokers)

    assert weights == [24.0, 2.0, 9.0, 0.0, 0.0]


def test_get_topic_imbalance_stats(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)
    topics = list(ct.topics.values())

    net_imbalance, extra_partition_cnt_per_broker = \
        stats.get_topic_imbalance_stats(brokers, topics)

    expected_extra = {'0': 0, '1': 1, '2': 1, '3': 1, '4': 0}

    assert net_imbalance == 3
    assert extra_partition_cnt_per_broker == expected_extra


def test_get_weighted_topic_imbalance_stats(create_cluster_topology):
    ct = create_cluster_topology()
    brokers = sorted(list(ct.brokers.values()), key=lambda b: b.id)
    topics = list(ct.topics.values())

    total_imbalance, weighted_imbalance_per_broker = \
        stats.get_weighted_topic_imbalance_stats(brokers, topics)

    # 97 is the total weight of the cluster.
    expected = {
        '0': 0,
        '1': 45 / 97,
        '2': 10 / 97,
        '3': 36 / 97,
        '4': 0,
    }
    expected_imbalance = 91 / 97

    assert abs(total_imbalance - expected_imbalance) < 1e-05
    assert weighted_imbalance_per_broker == expected


def test_get_partition_movement_stats(create_cluster_topology):
    ct = create_cluster_topology()
    base_assignment = ct.assignment

    # Move (T0,0) 2 -> 4
    # Move (T0,1) 3 -> 1
    # Move (T3,0) 1 -> 3
    # Move (T3,0) 2 -> 4
    # Change leader (T1, 0) 0 -> 1
    new_assignment = {
        ('T0', 0): ['1', '4'],
        ('T0', 1): ['2', '1'],
        ('T1', 0): ['1', '0', '2', '3'],
        ('T1', 1): ['0', '1', '2', '3'],
        ('T2', 0): ['2'],
        ('T3', 0): ['0', '3', '4'],
        ('T3', 1): ['0', '1', '4'],
    }
    ct.update_cluster_topology(new_assignment)

    movement_count, movement_size, leader_changes = \
        stats.get_partition_movement_stats(ct, base_assignment)

    assert movement_count == 4
    assert movement_size == 23.0
    assert leader_changes == 1
