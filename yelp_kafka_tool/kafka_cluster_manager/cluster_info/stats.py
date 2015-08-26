"""This files contains supporting api's required to evaluate stats of the
cluster at any given time.
"""
from collections import defaultdict
from math import sqrt

from .util import (
    get_partitions_per_broker,
    get_leaders_per_broker,
    compute_optimal_count,
)


# Get imbalance stats
def standard_deviation(data):
    """Return standard deviation for given data with list of values."""
    avg_data = sum(data) / len(data)
    variance = map(lambda x: (x - avg_data) ** 2, data)
    avg_variance = sum(variance) / len(data)
    return sqrt(avg_variance)


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
        compute_optimal_count(sum(count_per_broker), len(count_per_broker))
    for count in count_per_broker:
        extra_cnt, extra_allowed = \
            get_extra_element_count(count, opt_count, extra_allowed)
        net_imbalance += extra_cnt
    return net_imbalance


def get_extra_element_count(
    curr_count,
    opt_count,
    extra_allowed_cnt,
):
    """Evaluate and return extra same element count based on given values.

    @key-term:
    group:  In here group can be any base where elements are place
            i.e. replication-group while placing replicas (elements)
            or  brokers while placing partitions (elements).
    element:  Generic term for units which are optimally placed over group.

    @params:
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
            compute_optimal_count(partition.replication_factor, tot_rgs)

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


def get_leader_imbalance_stats(brokers, partitions):
    """Return for each broker the number of times it is assigned as preferred
    leader. Also return net leader-imbalance and imbalance stdev.
    """
    leaders_per_broker = get_leaders_per_broker(brokers)

    # Calculate standard deviation of leader imbalance
    stdev_imbalance = standard_deviation(leaders_per_broker.values())

    # Calculation net imbalance
    net_imbalance = get_net_imbalance(leaders_per_broker.values())
    leaders_per_broker_id = dict(
        (broker.id, count)
        for broker, count in leaders_per_broker.iteritems()
    )
    return stdev_imbalance, net_imbalance, leaders_per_broker_id


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
    for topic in topics:
        # Optimal partition-count per topic per broker
        total_partition_replicas = \
            topic.partition_count * topic.replication_factor
        opt_partition_cnt, extra_partitions_allowed = \
            compute_optimal_count(total_partition_replicas, tot_brokers)
        # Get extra-partition count per broker for each topic
        for broker in brokers:
            partition_cnt_broker = broker.count_topic_partitions(topic)
            extra_partitions, extra_partitions_allowed = \
                get_extra_element_count(
                    partition_cnt_broker,
                    opt_partition_cnt,
                    extra_partitions_allowed,
                )
            extra_partition_cnt_per_broker[broker.id] += extra_partitions

    # Net extra partitions over all brokers
    net_imbalance = sum(extra_partition_cnt_per_broker.itervalues())
    return net_imbalance, extra_partition_cnt_per_broker


def get_partition_imbalance_stats(brokers):
    """Return partition count for each broker and net imbalance due to extra
    partition count on each broker.
    """
    partitions_per_broker = get_partitions_per_broker(brokers)

    # Calculate standard deviation of partition imbalance
    stdev_imbalance = standard_deviation(partitions_per_broker.values())

    # Net total imbalance of partition count over all brokers
    net_imbalance = get_net_imbalance(partitions_per_broker.values())
    partitions_per_broker_id = dict(
        (partition.id, count)
        for partition, count in partitions_per_broker.iteritems()
    )
    return stdev_imbalance, net_imbalance, partitions_per_broker_id
