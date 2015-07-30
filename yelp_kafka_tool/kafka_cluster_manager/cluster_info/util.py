from __future__ import print_function
from __future__ import absolute_import


def display_cluster_topology(cluster_topology):
    print(cluster_topology.get_assignment_json())


def display_same_replica_count_rg(
    duplicate_replica_count_per_rg,
    imbalance,
):
    """Display same topic/partition count over brokers."""
    print("=" * 35)
    print("Replication-group Same-replica-count")
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
    print('Net-imbalance: Total extra partitions over brokers')
    print('{imbalance}\n'.format(imbalance=imbalance))
    print(
        'Net-imbalance-ratio: Total extra partitions over brokers '
        'per 100 partitions'
    )
    ratio = imbalance / float(sum(partition_count.values())) * 100
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
    print('Net-imbalance: Total extra brokers as leaders')
    print('{imbalance}\n'.format(imbalance=imbalance))
    print('Net-imbalance-ratio: Total extra brokers as leaders per 100 partitions')
    ratio = imbalance / float(sum(leader_count.values())) * 100.0
    print('{ratio}\n'.format(ratio=ratio))
