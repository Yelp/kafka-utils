from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import logging
_log = logging.getLogger('kafka-cluster-manager')


def display_cluster_topology(cluster_topology):
    print(cluster_topology.get_assignment_json())


def display_same_replica_count_rg(
    duplicate_replica_count_per_rg,
    imbalance,
):
    """Display same topic/partition count over brokers."""
    print("=" * 35)
    print("Replication-group Extra-Same-replica-count")
    print("=" * 35)
    for rg_id, replica_count in duplicate_replica_count_per_rg.iteritems():
        count = int(replica_count)
        print(
            "{b:^7s} {cnt:^10d}".format(
                b=rg_id,
                cnt=int(count),
            )
        )
    print("=" * 35)
    print('\nTotal replication-group imbalance {imbalance}\n\n'.format(
        imbalance=imbalance,
    ))


def display_same_topic_partition_count_broker(
    same_topic_partition_count,
    imbalance,
):
    """Display same topic/partition count over brokers."""
    print("=" * 55)
    print("Broker   Extra-Topic-Partition Count")
    print("=" * 55)
    for broker_id, partition_count in \
            same_topic_partition_count.iteritems():
        print(
            "{b:^7d} {partition_count:^18d}".format(
                b=broker_id,
                partition_count=partition_count,
            ),
        )
    print("=" * 55)
    print('\n\nSame topic/partition count imbalance:\n')
    print(
        'Total same topic-partition imbalance count: {imbalance}\n'.format(
            imbalance=imbalance,
        )
    )


def display_partition_count_per_broker(
    partition_count,
    stdev_imbalance,
    imbalance,
):
    """Display partition count over brokers."""
    print("=" * 25)
    print("Broker   Partition Count")
    print("=" * 25)
    for broker_id, count in partition_count.iteritems():
        print(
            "{b:^7d} {cnt:^18d}".format(
                b=broker_id,
                cnt=count,
            ),
        )
    print("=" * 25)

    print('\n\nPartition-count imbalance:\n')
    print('Standard-deviation: {stdev}'.format(stdev=stdev_imbalance))
    print('Net-imbalance: Total extra partitions over brokers')
    print('{imbalance}\n'.format(imbalance=imbalance))
    print(
        'Net-imbalance-ratio: Total extra partitions over brokers '
        'per 100 partitions'
    )
    total_partition_count = sum(partition_count.values())
    if total_partition_count > 0:
        ratio = imbalance / total_partition_count * 100
    else:
        ratio = 0
    print('{ratio}\n'.format(ratio=ratio))


def display_leader_count_per_broker(
    leader_count,
    stdev_imbalance,
    imbalance,
):
    """Display partition and leader count for each broker."""
    print("=" * 33)
    print("Broker   Preferred Leader Count")
    print("=" * 33)
    for broker_id, count in leader_count.iteritems():
        print(
            "{b:^7d} {cnt:^20d}".format(
                b=broker_id,
                cnt=count,
            ),
        )
    print("=" * 33)

    print('\n\nLeader-count imbalance:\n')
    print('Standard-deviation: {stdev}'.format(stdev=stdev_imbalance))
    print('Net-imbalance: Total extra brokers as leaders')
    print('{imbalance}\n'.format(imbalance=imbalance))
    print('Net-imbalance-ratio: Total extra brokers as leaders per 100 partitions')
    leader_count = sum(leader_count.values())
    if leader_count > 0:
        ratio = imbalance / leader_count * 100.0
    else:
        ratio = 0
    print('{ratio}\n'.format(ratio=ratio))


def display_assignment_changes(
    curr_plan_list,
    new_plan_list,
    total_changes,
    log_only=True,
):
    """Display current and proposed changes in
    topic-partition to replica layout over brokers.
    """
    action_cnt = '\n[INFO] Total actions required {0}'.format(total_changes)
    _log.info(action_cnt) if log_only else print(action_cnt)
    action_cnt = (
        '[INFO] Total actions that will be executed {0}'
        .format(len(new_plan_list))
    )
    _log.info(action_cnt) if log_only else print(action_cnt)
    changes = ('[INFO] Proposed Changes in current cluster-layout:\n')
    _log.info(changes) if log_only else print(changes)

    tp_str = 'Topic - Partition'
    curr_repl_str = 'Previous-Assignment'
    new_rep_str = 'Proposed-Assignment'
    tp_list = [tp_repl[0] for tp_repl in curr_plan_list]

    # Display heading
    _log.info('=' * 80) if log_only else print('=' * 80)
    row = (
        '{tp:^30s}: {curr_rep_str:^20s} ==> {new_rep_str:^20s}' .format(
            tp=tp_str,
            curr_rep_str=curr_repl_str,
            new_rep_str=new_rep_str,
        )
    )
    _log.info(row) if log_only else print(row)
    _log.info('=' * 80) if log_only else print('=' * 80)

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
        _log.info(row) if log_only else print(row)
