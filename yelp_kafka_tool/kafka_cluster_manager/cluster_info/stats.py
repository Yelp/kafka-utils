"""This files contains supporting api's required to evaluate stats of the
cluster at any given time.
"""
from math import sqrt

from .util import (
    get_partitions_per_broker,
    get_leaders_per_broker,
    get_per_topic_partitions_count,
)

from collections import defaultdict


# Get imbalance stats
def standard_deviation(data):
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
    opt_count = sum(count_per_broker) // len(count_per_broker)
    more_opt_count_allowed = sum(count_per_broker) % len(count_per_broker)
    for count in count_per_broker:
        if count > opt_count:
            if more_opt_count_allowed > 0:
                more_opt_count_allowed -= 1
                net_imbalance += (count - opt_count - 1)
            else:
                net_imbalance += (count - opt_count)
    return net_imbalance


def get_evaluation_parameters(total_elements, total_groups):
    opt_element_cnt = total_elements // total_groups
    extra_elements_allowed_cnt = total_elements % total_groups
    evenly_distribute = bool(not extra_elements_allowed_cnt)
    return opt_element_cnt, extra_elements_allowed_cnt, evenly_distribute


def get_extra_element_count(
    given_ele_count,
    opt_ele_count,
    evenly_distribute,
    extra_allowed_ele_cnt,
):
    """Evaluate and return extra same element count based on given values.

    @params:
    given_ele_count:        Given count
    evenly_distribute:  Implies every broker should have equal number of
                        element-count. Any extra element from opt-count
                        is counted towards overall imbalance extra-element
                        count.
    opt_ele_count:     Optimal count for each broker.
    extra_allowed_ele_cnt: Count of brokers which can have 1 extra broker

    """
    extra_cnt = 0
    if evenly_distribute:
        if given_ele_count > opt_ele_count:
            extra_cnt = given_ele_count - opt_ele_count
    else:
        if given_ele_count <= opt_ele_count:
            # No extra partition of same topic
            pass
        elif given_ele_count > opt_ele_count:
            # We still can allow 1 extra count
            if extra_allowed_ele_cnt > 0:
                extra_allowed_ele_cnt -= 1
                extra_cnt = given_ele_count - opt_ele_count - 1
            else:
                extra_cnt = given_ele_count - opt_ele_count
    return extra_cnt, extra_allowed_ele_cnt


# Get imbalance stats
def get_replication_group_imbalance_stats(rgs, partitions):
    """Calculate same replica count over each replication-group.
    Can only be calculated on current cluster-state.
    """
    # Get broker-id to rg-id map
    broker_rg_id = {}
    for rg in rgs:
        for broker in rg.brokers:
            broker_rg_id[broker.id] = rg.id

    tot_rgs = len(rgs)
    extra_replica_cnt_per_rg = defaultdict(int)
    for partition in partitions:
        tot_replicas = partition.replication_factor
        # Get optimal replica-count for each partition
        opt_replica_cnt, extra_replicas_allowed_cnt, evenly_distribute = \
            get_evaluation_parameters(tot_replicas, tot_rgs)

        # Extra replica count for each rg
        for rg in rgs:
            replica_cnt_rg = len([
                rg_partition
                for rg_partition in rg.partitions
                if rg_partition.name == partition.name
            ])
            extra_replica_cnt, extra_replicas_allowed_cnt = \
                get_extra_element_count(
                    replica_cnt_rg,
                    opt_replica_cnt,
                    evenly_distribute,
                    extra_replicas_allowed_cnt,
                )
            extra_replica_cnt_per_rg[rg] += extra_replica_cnt

    # Evaluate net imbalance ecross all replication-groups
    net_imbalance = sum(extra_replica_cnt_per_rg.values())
    return net_imbalance, extra_replica_cnt_per_rg


def get_leader_imbalance_stats(brokers, partitions):
    leaders_per_broker = get_leaders_per_broker(
        brokers,
        partitions,
    )

    # Calculate standard deviation of leader imbalance
    stdev_imbalance = standard_deviation(leaders_per_broker.values())

    # Calculation net imbalance
    net_imbalance = get_net_imbalance(leaders_per_broker.values())
    return stdev_imbalance, net_imbalance, leaders_per_broker


def get_topic_imbalance_stats(brokers, topics):
    """Return count of topics and partitions on each broker having multiple
    partitions of same topic.

    :rtype dict(broker_id: same-topic-partition count)
    Example: If broker has 3 partitions of given topic then this implies
    it has 2 extra partitions than recommended partitions as 1.
    So the same-topic-partition-imbalance-count is 2
    """
    extra_partition_cnt_per_broker = defaultdict(int)
    tot_brokers = len(brokers)
    for topic in topics:
        # Optimal partition-count per topic per broker
        total_partitions_all = topic.partition_count * topic.replication_factor
        opt_part_count, extra_partitions_allowed_cnt, evenly_distribute = \
            get_evaluation_parameters(total_partitions_all, tot_brokers)

        # Get extra-partition count per broker for each topic
        for broker in brokers:
            partition_cnt_broker = len([
                partition
                for partition in broker.partitions
                if partition.topic == topic
            ])
            extra_partitions, extra_partitions_allowed_cnt = \
                get_extra_element_count(
                    partition_cnt_broker,
                    opt_part_count,
                    evenly_distribute,
                    extra_partitions_allowed_cnt,
                )
            extra_partition_cnt_per_broker[broker] += extra_partitions

    # Net extra partitions over all brokers
    net_imbalance = sum(extra_partition_cnt_per_broker.itervalues())
    return net_imbalance, extra_partition_cnt_per_broker


def get_partition_imbalance_stats(brokers):
    partitions_per_broker = get_partitions_per_broker(brokers)

    # Calculate standard deviation of partition imbalance
    stdev_imbalance = standard_deviation(partitions_per_broker.values())
    # Net total imbalance of partition count over all brokers
    net_imbalance = get_net_imbalance(partitions_per_broker.values())
    return stdev_imbalance, net_imbalance, partitions_per_broker
