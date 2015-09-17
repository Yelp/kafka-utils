from collections import Counter, OrderedDict


def get_partitions_per_broker(brokers):
    """Return partition count for each broker."""
    return dict(
        (broker, len(broker.partitions))
        for broker in brokers
    )


def get_leaders_per_broker(brokers):
    """Return count for each broker the number of times
    it is assigned as preferred leader.
    """
    return dict(
        (broker, broker.count_preferred_replica())
        for broker in brokers
    )


def get_per_topic_partitions_count(broker):
    """Return partition-count of each topic on given broker."""
    return Counter((partition.topic for partition in broker.partitions))


def compute_optimal_count(total_elements, total_groups):
    """Return optimal count and extra-elements allowed based on base
    total count of elements and groups.
    """
    opt_element_cnt = total_elements // total_groups
    extra_elements_allowed_cnt = total_elements % total_groups
    return opt_element_cnt, extra_elements_allowed_cnt


def get_assignment_map(assignment_json):
    """Convert given assignment from json format to partition-replica map.

    Arguments:
    assignment_json: Given un-ordered assignment in json format
    :return:         Return assignment ordered over topic, partition tuple
    """
    assignment = {}
    for ele_curr in assignment_json['partitions']:
        assignment[
            (ele_curr['topic'], ele_curr['partition'])
        ] = ele_curr['replicas']
    # assignment map created in sorted order for deterministic solution
    assignment = OrderedDict(sorted(assignment.items(), key=lambda t: t[0]))
    return assignment
