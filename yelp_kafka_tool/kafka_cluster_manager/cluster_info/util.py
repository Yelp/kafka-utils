from collections import Counter


def get_partitions_per_broker(brokers):
    """Return partition count for each broker."""
    return dict(
        (broker, len(broker.partitions))
        for broker in brokers
    )


def get_leaders_per_broker(brokers, partitions):
    """Return count for each broker the number of times
    it is assigned as preferred leader.
    """
    leaders_per_broker = dict(
        (broker, 0)
        for broker in brokers
    )
    for partition in partitions:
        leaders_per_broker[partition.leader] += 1
    return leaders_per_broker


def get_per_topic_partitions_count(broker):
    """Return partition-count of each topic on given broker."""
    return Counter((partition.topic for partition in broker.partitions))


def computer_optimal_count(total_elements, total_groups):
    """Return optimal count and extra-elements allowed based on base
    total count of elements and groups.
    """
    opt_element_cnt = total_elements // total_groups
    extra_elements_allowed_cnt = total_elements % total_groups
    evenly_distribute = bool(not extra_elements_allowed_cnt)
    return opt_element_cnt, extra_elements_allowed_cnt, evenly_distribute
