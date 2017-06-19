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
"""This files contains supporting api's required to evaluate stats of the
cluster at any given time.
"""
from __future__ import absolute_import
from __future__ import division

from collections import defaultdict
from math import sqrt

import six

from .util import compute_optimum


def mean(data):
    """Return the mean of a sequence of numbers."""
    return sum(data) / len(data)


def variance(data, data_mean=None):
    """Return variance of a sequence of numbers.
    :param data_mean: Precomputed mean of the sequence.
    """
    data_mean = data_mean or mean(data)
    return sum((x - data_mean) ** 2 for x in data) / len(data)


def standard_deviation(data, data_mean=None, data_variance=None):
    """Return standard deviation of a sequence of numbers.
    :param data_mean: Precomputed mean of the sequence.
    :param data_variance: Precomputed variance of the sequence.
    """
    data_variance = data_variance or variance(data, data_mean)
    return sqrt(data_variance)


def coefficient_of_variation(data, data_mean=None, data_stdev=None):
    """Return the coefficient of variation (CV) of a sequence of numbers.
    :param data_mean: Precomputed mean of the sequence.
    :param data_standard_deviation: Precomputed standard_deviation of the
        sequence.
    """
    data_mean = data_mean or mean(data)
    data_stdev = data_stdev or standard_deviation(data, data_mean)
    if data_mean == 0:
        return float("inf") if data_stdev != 0 else 0
    else:
        return data_stdev / data_mean


def get_net_imbalance(count_per_broker):
    """Calculate and return net imbalance based on given count of
    partitions or leaders per broker.

    Net-imbalance in case of partitions implies total number of
    extra partitions from optimal count over all brokers.
    This is also implies, the minimum number of partition movements
    required for overall balancing.

    For leaders, net imbalance implies total number of extra brokers
    as leaders from optimal count.
    """
    net_imbalance = 0
    opt_count, extra_allowed = \
        compute_optimum(len(count_per_broker), sum(count_per_broker))
    for count in count_per_broker:
        extra_cnt, extra_allowed = \
            get_extra_element_count(count, opt_count, extra_allowed)
        net_imbalance += extra_cnt
    return net_imbalance


def get_extra_element_count(curr_count, opt_count, extra_allowed_cnt):
    """Evaluate and return extra same element count based on given values.

    :key-term:
    group:  In here group can be any base where elements are place
            i.e. replication-group while placing replicas (elements)
            or  brokers while placing partitions (elements).
    element:  Generic term for units which are optimally placed over group.

    :params:
    curr_count: Given count
    opt_count:  Optimal count for each group.
    extra_allowed_cnt:  Count of groups which can have 1 extra element
                   _    on each group.
    """
    if curr_count > opt_count:
        # We still can allow 1 extra count
        if extra_allowed_cnt > 0:
            extra_allowed_cnt -= 1
            extra_cnt = curr_count - opt_count - 1
        else:
            extra_cnt = curr_count - opt_count
    else:
        extra_cnt = 0
    return extra_cnt, extra_allowed_cnt


# Get imbalance stats
def get_replication_group_imbalance_stats(rgs, partitions):
    """Calculate extra replica count replica count over each replication-group
    and net extra-same-replica count.
    """
    tot_rgs = len(rgs)
    extra_replica_cnt_per_rg = defaultdict(int)
    for partition in partitions:
        # Get optimal replica-count for each partition
        opt_replica_cnt, extra_replicas_allowed = \
            compute_optimum(tot_rgs, partition.replication_factor)

        # Extra replica count for each rg
        for rg in rgs:
            replica_cnt_rg = rg.count_replica(partition)
            extra_replica_cnt, extra_replicas_allowed = \
                get_extra_element_count(
                    replica_cnt_rg,
                    opt_replica_cnt,
                    extra_replicas_allowed,
                )
            extra_replica_cnt_per_rg[rg.id] += extra_replica_cnt

    # Evaluate net imbalance across all replication-groups
    net_imbalance = sum(extra_replica_cnt_per_rg.values())
    return net_imbalance, extra_replica_cnt_per_rg


def get_broker_partition_counts(brokers):
    """Get a list containing the number of partitions on each broker"""
    return [len(broker.partitions) for broker in brokers]


def get_broker_weights(brokers):
    """Get a list containing the weight of each broker"""
    return [broker.weight for broker in brokers]


def get_broker_leader_counts(brokers):
    """Get a list containing the number of leaders of each broker"""
    return [broker.count_preferred_replica() for broker in brokers]


def get_broker_leader_weights(brokers):
    """Get a list containing the weight from leaders on each broker"""
    return [broker.leader_weight for broker in brokers]


def get_topic_imbalance_stats(brokers, topics):
    """Return count of topics and partitions on each broker having multiple
    partitions of same topic.

    :rtype dict(broker_id: same-topic-partition count)
    Example:
    Total-brokers (b1, b2): 2
    Total-partitions of topic t1: 5
    (b1 has 4 partitions), (b2 has 1 partition)
    opt-count: 5/2 = 2
    extra-count: 5%2 = 1
    i.e. 1 broker can have 2 + 1 = 3 partitions
    and rest of brokers can have 2 partitions for given topic
    Extra-partition or imbalance:
    b1: current-partitions - optimal-count = 4 - 2 - 1(extra allowed) = 1
    Net-imbalance = 1
    """
    extra_partition_cnt_per_broker = defaultdict(int)
    tot_brokers = len(brokers)
    # Sort the brokers so that the iteration order is deterministic.
    sorted_brokers = sorted(brokers, key=lambda b: b.id)
    for topic in topics:
        # Optimal partition-count per topic per broker
        total_partition_replicas = \
            len(topic.partitions) * topic.replication_factor
        opt_partition_cnt, extra_partitions_allowed = \
            compute_optimum(tot_brokers, total_partition_replicas)
        # Get extra-partition count per broker for each topic
        for broker in sorted_brokers:
            partition_cnt_broker = broker.count_partitions(topic)
            extra_partitions, extra_partitions_allowed = \
                get_extra_element_count(
                    partition_cnt_broker,
                    opt_partition_cnt,
                    extra_partitions_allowed,
                )
            extra_partition_cnt_per_broker[broker.id] += extra_partitions

    # Net extra partitions over all brokers
    net_imbalance = sum(six.itervalues(extra_partition_cnt_per_broker))
    return net_imbalance, extra_partition_cnt_per_broker


def get_weighted_topic_imbalance_stats(brokers, topics):
    weighted_imbalance_per_broker = defaultdict(float)
    tot_brokers = len(brokers)
    # Sort the brokers so that the iteration order is deterministic.
    sorted_brokers = sorted(brokers, key=lambda b: b.id)
    total_weight = sum(topic.weight for topic in topics)
    for topic in topics:
        total_partition_replicas = sum(
            partition.replication_factor for partition in topic.partitions
        )

        opt_partition_cnt, extra_partitions_allowed = \
            compute_optimum(tot_brokers, total_partition_replicas)

        for broker in sorted_brokers:
            partition_cnt_broker = broker.count_partitions(topic)
            extra_partitions, extra_partitions_allowed = \
                get_extra_element_count(
                    partition_cnt_broker,
                    opt_partition_cnt,
                    extra_partitions_allowed,
                )
            weighted_imbalance_per_broker[broker.id] += \
                extra_partitions * topic.weight / total_weight

    total_imbalance = sum(six.itervalues(weighted_imbalance_per_broker))
    return total_imbalance, weighted_imbalance_per_broker


def get_partition_movement_stats(ct, prev_assignment):
    curr_assignment = ct.assignment
    movement_count = 0
    movement_size = 0.0
    leader_changes = 0
    for prev_partition, prev_replicas in six.iteritems(prev_assignment):
        curr_replicas = curr_assignment[prev_partition]
        diff = len(set(curr_replicas) - set(prev_replicas))
        movement_count += diff
        movement_size += diff * ct.partitions[prev_partition].size

        curr_leader = curr_replicas and curr_replicas[0] or None
        prev_leader = prev_replicas and prev_replicas[0] or None
        if curr_leader != prev_leader:
            leader_changes += 1

    return movement_count, movement_size, leader_changes


def calculate_partition_movement(prev_assignment, curr_assignment):
    """Calculate the partition movements from initial to current assignment.
    Algorithm:
        For each partition in initial assignment
            # If replica set different in current assignment:
                # Get Difference in sets
    :rtype: tuple
    dict((partition,  (from_broker_set, to_broker_set)), total_movements
    """
    total_movements = 0
    movements = {}
    for prev_partition, prev_replicas in six.iteritems(prev_assignment):
        curr_replicas = curr_assignment[prev_partition]
        diff = len(set(curr_replicas) - set(prev_replicas))
        if diff:
            total_movements += diff
            movements[prev_partition] = (
                (set(prev_replicas) - set(curr_replicas)),
                (set(curr_replicas) - set(prev_replicas)),
            )
    return movements, total_movements
