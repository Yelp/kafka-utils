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
from __future__ import division
from __future__ import print_function

import logging

import kafka_utils.kafka_cluster_manager.cluster_info.stats as stats
from kafka_utils.util.validation import assignment_to_plan
_log = logging.getLogger('kafka-cluster-manager')


def display_table(headers, table):
    """Print a formatted table.

    :param headers: A list of header objects that are displayed in the first
        row of the table.
    :param table: A list of lists where each sublist is a row of the table.
        The number of elements in each row should be equal to the number of
        headers.
    """
    assert all(len(row) == len(headers) for row in table)

    str_headers = [str(header) for header in headers]
    str_table = [[str(cell) for cell in row] for row in table]
    column_lengths = [
        max(len(header), *(len(row[i]) for row in str_table))
        for i, header in enumerate(str_headers)
    ]

    print(
        " | ".join(
            str(header).ljust(length)
            for header, length in zip(str_headers, column_lengths)
        )
    )
    print("-+-".join("-" * length for length in column_lengths))
    for row in str_table:
        print(
            " | ".join(
                str(cell).ljust(length)
                for cell, length in zip(row, column_lengths)
            )
        )


def display_replica_imbalance(rgs, partitions):
    """Display replica replication-group distribution imbalance statistics.

    :param rgs: List of the cluster's ReplicationGroups.
    :param partitions: List of the cluster's Partitions.
    """
    rg_ids = [rg.id for rg in rgs]
    rg_imbalance, rg_extra_replica_count = \
        stats.get_replication_group_imbalance_stats(rgs, partitions)
    display_table(
        ['Replication Group', 'Extra replica count'],
        zip(rg_ids, [rg_extra_replica_count[rg_id] for rg_id in rg_ids]),
    )
    print(
        '\n'
        'Total extra replica count: {imbalance}'
        .format(
            imbalance=rg_imbalance,
        )
    )


def display_partition_imbalance(brokers):
    """Display partition count and weight imbalance statistics.

    :param brokers: List of the cluster's Brokers.
    """
    broker_ids = [broker.id for broker in brokers]
    broker_counts = stats.get_broker_partition_counts(brokers)
    broker_weights = stats.get_broker_weights(brokers)
    display_table(
        ['Broker', 'Partition Count', 'Weight'],
        zip(broker_ids, broker_counts, broker_weights),
    )
    print(
        '\n'
        'Partition count imbalance: {net_imbalance}\n'
        'Broker weight mean: {weight_mean}\n'
        'Broker weight stdev: {weight_stdev}\n'
        'Broker weight cv: {weight_cv}'
        .format(
            net_imbalance=stats.get_net_imbalance(broker_counts),
            weight_mean=stats.mean(broker_weights),
            weight_stdev=stats.standard_deviation(broker_weights),
            weight_cv=stats.coefficient_of_variation(broker_weights),
        )
    )


def display_leader_imbalance(brokers):
    """Display leader count and weight imbalance statistics.

    :param brokers: List of the cluster's Brokers.
    """
    broker_ids = [broker.id for broker in brokers]
    broker_leader_counts = stats.get_broker_leader_counts(brokers)
    broker_leader_weights = stats.get_broker_leader_weights(brokers)
    display_table(
        ['Broker', 'Leader Count', 'Leader Weight'],
        zip(broker_ids, broker_leader_counts, broker_leader_weights),
    )
    print(
        '\n'
        'Leader count imbalance: {net_imbalance}\n'
        'Broker leader weight mean: {weight_mean}\n'
        'Broker leader weight stdev: {weight_stdev}\n'
        'Broker leader weight cv: {weight_cv}'
        .format(
            net_imbalance=stats.get_net_imbalance(broker_leader_counts),
            weight_mean=stats.mean(broker_leader_weights),
            weight_stdev=stats.standard_deviation(broker_leader_weights),
            weight_cv=stats.coefficient_of_variation(broker_leader_weights),
        )
    )


def display_topic_broker_imbalance(brokers, topics):
    """Display topic broker imbalance statistics.

    :param brokers: List of the cluster's Brokers.
    :param topics: List of the cluster's Topics.
    """
    broker_ids = [broker.id for broker in brokers]
    topic_imbalance, broker_topic_imbalance = \
        stats.get_topic_imbalance_stats(brokers, topics)
    weighted_topic_imbalance, weighted_broker_topic_imbalance = \
        stats.get_weighted_topic_imbalance_stats(brokers, topics)
    display_table(
        [
            'Broker',
            'Extra-Topic-Partition Count',
            'Weighted Topic Imbalance',
        ],
        zip(
            broker_ids,
            broker_topic_imbalance.values(),
            weighted_broker_topic_imbalance.values(),
        ),
    )
    print(
        '\n'
        'Topic partition imbalance count: {topic_imbalance}\n'
        'Weighted topic partition imbalance: {weighted_topic_imbalance}'
        .format(
            topic_imbalance=topic_imbalance,
            weighted_topic_imbalance=weighted_topic_imbalance,
        )
    )


def display_movements_stats(ct, base_assignment):
    """Display how the amount of movement between two assignments.

    :param ct: The cluster's ClusterTopology.
    :param base_assignment: The cluster assignment to compare against.
    """
    movement_count, movement_size, leader_changes = \
        stats.get_partition_movement_stats(ct, base_assignment)
    print(
        'Total partition movements: {movement_count}\n'
        'Total partition movement size: {movement_size}\n'
        'Total leader changes: {leader_changes}'
        .format(
            movement_count=movement_count,
            movement_size=movement_size,
            leader_changes=leader_changes,
        )
    )


def display_cluster_topology_stats(cluster_topology, base_assignment=None):
    brokers = cluster_topology.brokers.values()
    rgs = cluster_topology.rgs.values()
    topics = cluster_topology.topics.values()
    partitions = cluster_topology.partitions.values()

    display_replica_imbalance(rgs, partitions)
    print("")
    display_partition_imbalance(brokers)
    print("")
    display_leader_imbalance(brokers)
    print("")
    display_topic_broker_imbalance(brokers, topics)
    if base_assignment:
        print("")
        display_movements_stats(cluster_topology, base_assignment)


def display_cluster_topology(cluster_topology):
    print(assignment_to_plan(cluster_topology.assignment))


def display_assignment_changes(plan_details, to_log=True):
    """Display current and proposed changes in
    topic-partition to replica layout over brokers.
    """
    curr_plan_list, new_plan_list, total_changes = plan_details
    action_cnt = '\n[INFO] Total actions required {0}'.format(total_changes)
    _log_or_display(to_log, action_cnt)
    action_cnt = (
        '[INFO] Total actions that will be executed {0}'
        .format(len(new_plan_list))
    )
    _log_or_display(to_log, action_cnt)
    changes = ('[INFO] Proposed Changes in current cluster-layout:\n')
    _log_or_display(to_log, changes)

    tp_str = 'Topic - Partition'
    curr_repl_str = 'Previous-Assignment'
    new_rep_str = 'Proposed-Assignment'
    tp_list = [tp_repl[0] for tp_repl in curr_plan_list]

    # Display heading
    msg = '=' * 80
    _log_or_display(to_log, msg)
    row = (
        '{tp:^30s}: {curr_rep_str:^20s} ==> {new_rep_str:^20s}' .format(
            tp=tp_str,
            curr_rep_str=curr_repl_str,
            new_rep_str=new_rep_str,
        )
    )
    _log_or_display(to_log, row)
    msg = '=' * 80
    _log_or_display(to_log, msg)

    # Display each topic-partition list with changes
    tp_list_sorted = sorted(tp_list, key=lambda tp: (tp[0], tp[1]))
    for tp in tp_list_sorted:
        curr_repl = [
            tp_repl[1] for tp_repl in curr_plan_list if tp_repl[0] == tp
        ][0]
        proposed_repl = [
            tp_repl[1] for tp_repl in new_plan_list if tp_repl[0] == tp
        ][0]
        tp_str = '{topic} - {partition:<2d}'.format(topic=tp[0], partition=tp[1])
        row = (
            '{tp:<30s}: {curr_repl:<20s} ==> {proposed_repl:<20s}'.format(
                tp=tp_str,
                curr_repl=curr_repl,
                proposed_repl=proposed_repl,
            )
        )
        _log_or_display(to_log, row)


def _log_or_display(to_log, msg):
    """Log or display the information."""
    if to_log:
        _log.info(msg)
    else:
        print(msg)
