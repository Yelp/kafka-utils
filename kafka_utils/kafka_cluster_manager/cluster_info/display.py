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
from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any

import kafka_utils.kafka_cluster_manager.cluster_info.stats as stats
from kafka_utils.kafka_cluster_manager.cluster_info.broker import Broker
from kafka_utils.kafka_cluster_manager.cluster_info.cluster_topology \
    import ClusterTopology
from kafka_utils.util.validation import assignment_to_plan
_log = logging.getLogger('kafka-cluster-manager')


def display_table(headers: list[str], table: list[list[str]]) -> None:
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


def _display_table_title_multicolumn(title: str, key_name: str, keys: list[Any], names: list[Any], values: list[Any]) -> None:
    assert len(names) == len(values)
    if len(names) == 1:
        headers = [key_name, title]
    else:
        print(title)
        headers = [key_name] + names
    display_table(headers, list(zip(keys, *values)))


def display_replica_imbalance(cluster_topologies: dict[str, ClusterTopology]) -> None:
    """Display replica replication-group distribution imbalance statistics.

    :param cluster_topologies: A dictionary mapping a string name to a
        ClusterTopology object.
    """
    assert cluster_topologies

    rg_ids = list(next(iter(cluster_topologies.values())).rgs.keys())
    assert all(
        set(rg_ids) == set(cluster_topology.rgs.keys())
        for cluster_topology in cluster_topologies.values()
    )

    rg_imbalances = [
        stats.get_replication_group_imbalance_stats(
            list(cluster_topology.rgs.values()),
            list(cluster_topology.partitions.values()),
        )
        for cluster_topology in cluster_topologies.values()
    ]

    _display_table_title_multicolumn(
        'Extra Replica Count',
        'Replication Group',
        rg_ids,
        list(cluster_topologies.keys()),
        [
            [erc[rg_id] for rg_id in rg_ids]
            for _, erc in rg_imbalances
        ],
    )

    for name, imbalance in zip(
            cluster_topologies.keys(),
            (imbalance for imbalance, _ in rg_imbalances)
    ):
        print(
            '\n'
            '{name}'
            'Total extra replica count: {imbalance}'
            .format(
                name='' if len(cluster_topologies) == 1 else name + '\n',
                imbalance=imbalance,
            )
        )


def display_partition_imbalance(cluster_topologies: dict[str, ClusterTopology]) -> None:
    """Display partition count and weight imbalance statistics.

    :param cluster_topologies: A dictionary mapping a string name to a
        ClusterTopology object.
    """
    broker_ids = list(next(iter(cluster_topologies.values())).brokers.keys())
    assert all(
        set(broker_ids) == set(cluster_topology.brokers.keys())
        for cluster_topology in cluster_topologies.values()
    )
    broker_partition_counts = [
        stats.get_broker_partition_counts(
            cluster_topology.brokers[broker_id]
            for broker_id in broker_ids
        )
        for cluster_topology in cluster_topologies.values()
    ]
    broker_weights = [
        stats.get_broker_weights(
            cluster_topology.brokers[broker_id]
            for broker_id in broker_ids
        )
        for cluster_topology in cluster_topologies.values()
    ]

    _display_table_title_multicolumn(
        'Partition Count',
        'Broker',
        broker_ids,
        list(cluster_topologies.keys()),
        broker_partition_counts,
    )

    print('')

    _display_table_title_multicolumn(
        'Partition Weight',
        'Broker',
        broker_ids,
        list(cluster_topologies.keys()),
        broker_weights,
    )

    for name, bpc, bw in zip(
            list(cluster_topologies.keys()),
            broker_partition_counts,
            broker_weights
    ):
        print(
            '\n'
            '{name}'
            'Partition count imbalance: {net_imbalance}\n'
            'Broker weight mean: {weight_mean}\n'
            'Broker weight stdev: {weight_stdev}\n'
            'Broker weight cv: {weight_cv}'
            .format(
                name='' if len(cluster_topologies) == 1 else name + '\n',
                net_imbalance=stats.get_net_imbalance(bpc),
                weight_mean=stats.mean(bw),
                weight_stdev=stats.stdevp(bw),
                weight_cv=stats.coefficient_of_variation(bw),
            )
        )


def display_leader_imbalance(cluster_topologies: dict[str, ClusterTopology]) -> None:
    """Display leader count and weight imbalance statistics.

    :param cluster_topologies: A dictionary mapping a string name to a
        ClusterTopology object.
    """
    broker_ids = list(next(iter(cluster_topologies.values())).brokers.keys())
    assert all(
        set(broker_ids) == set(cluster_topology.brokers.keys())
        for cluster_topology in cluster_topologies.values()
    )

    broker_leader_counts = [
        stats.get_broker_leader_counts(
            cluster_topology.brokers[broker_id]
            for broker_id in broker_ids
        )
        for cluster_topology in cluster_topologies.values()
    ]
    broker_leader_weights = [
        stats.get_broker_leader_weights(
            cluster_topology.brokers[broker_id]
            for broker_id in broker_ids
        )
        for cluster_topology in cluster_topologies.values()
    ]

    _display_table_title_multicolumn(
        'Leader Count',
        'Brokers',
        broker_ids,
        list(cluster_topologies.keys()),
        broker_leader_counts,
    )

    print('')

    _display_table_title_multicolumn(
        'Leader weight',
        'Brokers',
        broker_ids,
        list(cluster_topologies.keys()),
        broker_leader_weights,
    )

    for name, blc, blw in zip(
            list(cluster_topologies.keys()),
            broker_leader_counts,
            broker_leader_weights
    ):
        print(
            '\n'
            '{name}'
            'Leader count imbalance: {net_imbalance}\n'
            'Broker leader weight mean: {weight_mean}\n'
            'Broker leader weight stdev: {weight_stdev}\n'
            'Broker leader weight cv: {weight_cv}'
            .format(
                name='' if len(cluster_topologies) == 1 else name + '\n',
                net_imbalance=stats.get_net_imbalance(blc),
                weight_mean=stats.mean(blw),
                weight_stdev=stats.stdevp(blw),
                weight_cv=stats.coefficient_of_variation(blw),
            )
        )


def display_topic_broker_imbalance(cluster_topologies: dict[str, ClusterTopology]) -> None:
    """Display topic broker imbalance statistics.

    :param cluster_topologies: A dictionary mapping a string name to a
        ClusterTopology object.
    """
    broker_ids = list(next(iter(cluster_topologies.values())).brokers.keys())
    assert all(
        set(broker_ids) == set(cluster_topology.brokers.keys())
        for cluster_topology in cluster_topologies.values()
    )
    topic_names = list(next(iter(cluster_topologies.values())).topics.keys())
    assert all(
        set(topic_names) == set(cluster_topology.topics.keys())
        for cluster_topology in cluster_topologies.values()
    )

    imbalances = [
        stats.get_topic_imbalance_stats(
            [cluster_topology.brokers[broker_id] for broker_id in broker_ids],
            [cluster_topology.topics[tname] for tname in topic_names],
        )
        for cluster_topology in cluster_topologies.values()
    ]
    weighted_imbalances = [
        stats.get_weighted_topic_imbalance_stats(
            [cluster_topology.brokers[broker_id] for broker_id in broker_ids],
            [cluster_topology.topics[tname] for tname in topic_names],
        )
        for cluster_topology in cluster_topologies.values()
    ]

    _display_table_title_multicolumn(
        'Extra-Topic-Partition Count',
        'Brokers',
        broker_ids,
        list(cluster_topologies.keys()),
        [
            [i[1][broker_id] for broker_id in broker_ids]
            for i in imbalances
        ]
    )

    print('')

    _display_table_title_multicolumn(
        'Weighted Topic Imbalance',
        'Brokers',
        broker_ids,
        list(cluster_topologies.keys()),
        [
            [wi[1][broker_id] for broker_id in broker_ids]
            for wi in weighted_imbalances
        ]
    )

    for name, topic_imbalance, weighted_topic_imbalance in zip(
            cluster_topologies.keys(),
            (i[0] for i in imbalances),
            (wi[0] for wi in weighted_imbalances),
    ):
        print(
            '\n'
            '{name}'
            'Topic partition imbalance count: {topic_imbalance}\n'
            'Weighted topic partition imbalance: {weighted_topic_imbalance}'
            .format(
                name='' if len(cluster_topologies) == 1 else name + '\n',
                topic_imbalance=topic_imbalance,
                weighted_topic_imbalance=weighted_topic_imbalance,
            )
        )


def display_movements_stats(ct: ClusterTopology, base_assignment: dict[tuple[str, int], list[int]]) -> None:
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


def display_cluster_topology_stats(cluster_topology: ClusterTopology, base_assignment: dict[tuple[str, int], list[int]] | None = None) -> None:
    if base_assignment:
        def get_replication_group_id(broker: Broker) -> str:
            replication_group = cluster_topology.brokers[broker.id].replication_group
            assert replication_group is not None
            return replication_group.id

        base_cluster_topology = ClusterTopology(
            base_assignment,
            {broker.id: broker.metadata for broker in cluster_topology.brokers.values()},
            cluster_topology.partition_measurer,
            get_replication_group_id,
        )
        cluster_topologies = OrderedDict([
            ('Before', base_cluster_topology),
            ('After', cluster_topology),
        ])
    else:
        cluster_topologies = OrderedDict([
            ('', cluster_topology),
        ])

    display_replica_imbalance(cluster_topologies)
    print("")
    display_partition_imbalance(cluster_topologies)
    print("")
    display_leader_imbalance(cluster_topologies)
    print("")
    display_topic_broker_imbalance(cluster_topologies)
    if base_assignment:
        print("")
        display_movements_stats(cluster_topology, base_assignment)


def display_cluster_topology(cluster_topology: ClusterTopology) -> None:
    print(assignment_to_plan(cluster_topology.assignment))


def display_assignment_changes(plan_details: tuple[list[tuple[str, str]], list[tuple[str, str]], int], to_log: bool = True) -> None:
    """Display current and proposed changes in
    topic-partition to replica layout over brokers.
    """
    curr_plan_list, new_plan_list, total_changes = plan_details
    action_cnt = f'\n[INFO] Total actions required {total_changes}'
    _log_or_display(to_log, action_cnt)
    action_cnt = (
        '[INFO] Total actions that will be executed {}'
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
        tp_str = f'{tp[0]} - {tp[1]:<2d}'
        row = (
            '{tp:<30s}: {curr_repl:<20s} ==> {proposed_repl:<20s}'.format(
                tp=tp_str,
                curr_repl=curr_repl,
                proposed_repl=proposed_repl,
            )
        )
        _log_or_display(to_log, row)


def _log_or_display(to_log: bool, msg: str) -> None:
    """Log or display the information."""
    if to_log:
        _log.info(msg)
    else:
        print(msg)
