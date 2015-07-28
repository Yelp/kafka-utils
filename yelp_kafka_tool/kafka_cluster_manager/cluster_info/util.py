from __future__ import print_function
from __future__ import absolute_import


def display_initial_cluster_topology(cluster_topology):
    """Display the current cluster topology."""
    print(cluster_topology.get_initial_assignment_json())


def display_current_cluster_topology(cluster_topology):
    print(cluster_topology.get_assignment_json())


def display_same_replica_count_rg(cluster_topology, same_replica_per_rg):
    """Display same topic/partition count over brokers."""
    print("=" * 35)
    print("Replication-group Same-replica-count")
    print("=" * 35)
    for rg_id, replica_count in same_replica_per_rg.iteritems():
        count = int(replica_count)
        print(
            "{b:^7s} {cnt:^10d}".format(
                b=rg_id,
                cnt=int(count),
            )
        )
    print("=" * 35)
    print('\nTotal replication-group imbalance {imbalance}\n\n'.format(
        imbalance=sum(same_replica_per_rg.values()),
    ))


def display_same_topic_partition_count_broker(
    cluster_topology,
    same_topic_partition_count,
):
    """Display same topic/partition count over brokers."""
    print("=" * 55)
    print("Broker   Extra-Topic-Partition Count")
    print("=" * 55)
    for broker, partition_count in \
            same_topic_partition_count.iteritems():

        print(
            "{b:^7d} {partition_count:^18d}".format(
                b=broker.id,
                partition_count=partition_count,
            ),
        )
    print("=" * 55)
    print('\n\nSame topic/partition count imbalance:\n')
    print('Total same topic-partition imbalance count: {actual}\n'.format(
        actual=sum(same_topic_partition_count.values()))
    )


def display_partition_count_per_broker(
    cluster_topology,
    partition_count,
    stdev_imbalance,
    actual_imbalance,
):
    """Display partition count over brokers."""
    print("=" * 25)
    print("Broker   Partition Count")
    print("=" * 25)
    for broker, count in partition_count.iteritems():
        print(
            "{b:^7d} {cnt:^18d}".format(
                b=broker.id,
                cnt=count,
            ),
        )
    print("=" * 25)

    print('\n\nPartition-count imbalance:\n')
    print('Standard-deviation: {stdev}'.format(stdev=stdev_imbalance))
    print('Actual-imbalance: Total extra partitions over brokers')
    print('{actual}\n'.format(actual=actual_imbalance))
    print(
        'Actual-imbalance-ratio: Total extra partitions over brokers '
        'per 100 partitions'
    )
    ratio = actual_imbalance / float(sum(partition_count.values())) * 100
    print('{ratio}\n'.format(ratio=ratio))


def display_leader_count_per_broker(
    cluster_topology,
    leader_count,
    stdev_imbalance,
    actual_imbalance,
):
    """Display partition and leader count for each broker."""
    print("=" * 33)
    print("Broker   Preferred Leader Count")
    print("=" * 33)
    for broker, count in leader_count.iteritems():
        print(
            "{b:^7d} {cnt:^20d}".format(
                b=broker.id,
                cnt=count,
            ),
        )
    print("=" * 33)

    print('\n\nLeader-count imbalance:\n')
    print('Standard-deviation: {stdev}'.format(stdev=stdev_imbalance))
    print('Actual-imbalance: Total extra brokers as leaders')
    print('{actual}\n'.format(actual=actual_imbalance))
    print('Actual-imbalance-ratio: Total extra brokers as leaders per 100 partitions')
    ratio = actual_imbalance / float(sum(leader_count.values())) * 100.0
    print('{ratio}\n'.format(ratio=ratio))
