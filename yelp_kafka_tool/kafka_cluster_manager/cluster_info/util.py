import logging
from collections import Counter

_log = logging.getLogger('kafka-cluster-manager')


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


def compute_optimum(elements, groups):
    """Compute the number of elements per group and the reminder.

        :param elements: total number of elements
        :param groups: total number of groups
    """
    return elements // groups, elements % groups


def _smart_separate_groups(groups, key, total):
    """Given a list of group objects, and a function to extract the number of
    elements for each of them, return the list of groups that have an excessive
    number of elements (when compared to a uniform distribution), a list of
    groups with insufficient elements, and a list of groups that already have
    the optimal number of elements.

    :param list groups: list of group objects
    :param func key: function to retrieve the current number of elements from the group object
    :param int total: total number of elements to distribute

    Example:
        .. code-block:: python
           smart_separate_groups([11,  9, 10, 14], lambda g: g) => ([14], [10, 9], [11])
    """
    optimum, extra = compute_optimum(len(groups), total)
    over_loaded, under_loaded, optimal = [], [], []
    for group in sorted(groups, key=key, reverse=True):
        n_elements = key(group)
        additional_element = 1 if extra else 0
        if n_elements > optimum + additional_element:
            over_loaded.append(group)
        elif n_elements == optimum + additional_element:
            optimal.append(group)
        elif n_elements < optimum + additional_element:
            under_loaded.append(group)
        extra -= additional_element
    return over_loaded, under_loaded, optimal


def separate_groups(groups, key, total):
    """Separate the group into all potentially overloaded, optimal and
    under-loaded groups.

    The revised over-loaded groups increases the choice space for future
    selection of most suitable group based on search criteria.

    If all groups from smart-separate are optimal, return the original groups,
    since there's no use of creating potential over-loaded-groups.

    For example:
    Given the groups (a:4, b:4, c:3, d:2) where the number represents the number
    of elements for each group.
    smart_separate_groups sets 'a' and 'c' as optimal, 'b' as over-loaded
    and 'd' as under-loaded.

    separate-groups combines 'a' with 'b' as over-loaded, allowing to select
    between these two groups to transfer the element to 'd'.
    """
    optimum, _ = compute_optimum(len(groups), total)
    over_loaded, under_loaded, optimal = _smart_separate_groups(groups, key)
    # If every group is optimal return
    if not over_loaded:
        return over_loaded, under_loaded
    # Potential-over-loaded groups also have potential to be categorised
    # into over-loaded groups
    potential_over_loaded = [
        group for group in optimal
        if key(group) > optimum
    ]
    revised_over_loaded = over_loaded + potential_over_loaded
    return revised_over_loaded, under_loaded


def validate_plan(
    new_plan,
    base_plan=None,
    is_partition_subset=True,
):
    """Verify that the new plan is valid for execution.

    Given kafka-reassignment plan should affirm with following rules:
    - Plan should have at least one partition for re-assignment
    - Partition-name list should be subset of base-plan partition-list
    - Replication-factor for each partition of same topic is same
    - Replication-factor for each partition remains unchanged
    - No duplicate broker-ids in each replicas
    """
    if not _validate_plan(new_plan):
        _log.error('Invalid proposed-plan.')
        return False

    # Validate given plan in reference to base-plan
    if base_plan:
        if not _validate_plan(base_plan):
            _log.error('Invalid assignment from cluster.')
            return False
        if not _validate_plan_base(
            new_plan,
            base_plan,
            is_partition_subset,
        ):
            return False
    # Plan validation successful
    return True


def _validate_plan_base(
    new_plan,
    base_plan,
    is_partition_subset=True,
):
    """Validate if given plan is valid comparing with given base-plan.

    Validate following assertions:
    - Partition-check: New partition-set should be subset of base-partition set
    - Replica-count check: Replication-factor for each partition remains same
    - Broker-check: New broker-set should be subset of base broker-set
    """

    # Verify that partitions in plan are subset of base plan.
    new_partitions = set([
        (p_data['topic'], p_data['partition'])
        for p_data in new_plan['partitions']
    ])
    base_partitions = set([
        (p_data['topic'], p_data['partition'])
        for p_data in base_plan['partitions']
    ])
    if is_partition_subset:
        invalid_partitions = list(new_partitions - base_partitions)
    else:
        # partition set should be equal
        invalid_partitions = list(
            new_partitions.union(base_partitions) -
            new_partitions.intersection(base_partitions),
        )
    if invalid_partitions:
        _log.error(
            'Invalid partition(s) found: {p_list}'.format(
                p_list=invalid_partitions,
            )
        )
        return False

    # Verify replication-factor remains consistent
    base_partition_replicas = {
        (p_data['topic'], p_data['partition']): p_data['replicas']
        for p_data in base_plan['partitions']
    }
    new_partition_replicas = {
        (p_data['topic'], p_data['partition']): p_data['replicas']
        for p_data in new_plan['partitions']
    }
    invalid_replication_factor = False
    for new_partition, replicas in new_partition_replicas.iteritems():
        base_replica_cnt = len(base_partition_replicas[new_partition])
        if len(replicas) != base_replica_cnt:
            invalid_replication_factor = True
            _log.error(
                'Replication-factor Mismatch: Partition: {partition}: '
                'Base-replicas: {expected}, Proposed-replicas: {actual}'.format(
                    partition=new_partition,
                    expected=base_partition_replicas[new_partition],
                    actual=replicas,
                ),
            )
    if invalid_replication_factor:
        return False

    # Validation successful
    return True


def _validate_format(plan):
    """Validate if the format of the plan as expected.

    Validate format of plan on following rules:
    a) Verify if it ONLY and MUST have keys and value, 'version' and 'partitions'
    b) Verify if each value of 'partitions' ONLY and MUST have keys 'replicas',
        'partition', 'topic'
    c) Verify desired type of each value
    d) Verify non-empty partitions and replicas
    Sample-plan format:
    {
        "version": 1,
        "partitions": [
            {"partition":0, "topic":'t1', "replicas":[0,1,2]},
            {"partition":0, "topic":'t2', "replicas":[1,2]},
            ...
        ]}
    """
    # Verify presence of required keys
    if set(plan.keys()) != set(['version', 'partitions']):
        _log.error(
            'Invalid or incomplete keys in given plan. Expected: "version", '
            '"partitions". Found:{keys}'
            .format(keys=', '.join(plan.keys())),
        )
        return False

    # Invalid version
    if plan['version'] != 1:
        _log.error(
            'Invalid version of plan {version}'
            .format(version=plan['version']),
        )
        return False

    # Empty partitions
    if not plan['partitions']:
        _log.error(
            '"partitions" list found empty"'
            .format(version=plan['partitions']),
        )
        return False

    # Invalid partitions type
    if not isinstance(plan['partitions'], list):
        _log.error('"partitions" of type list expected.')
        return False

    # Invalid partition-data
    for p_data in plan['partitions']:
        if set(p_data.keys()) != set(['topic', 'partition', 'replicas']):
            _log.error(
                'Invalid keys in partition-data {keys}'
                .format(keys=', '.join(p_data.keys())),
            )
            return False
        # Check types
        if not isinstance(p_data['topic'], unicode):
            _log.error(
                '"topic" of type unicode expected {p_data}, found {t_type}'
                .format(p_data=p_data, t_type=type(p_data['topic'])),
            )
            return False
        if not isinstance(p_data['partition'], int):
            _log.error(
                '"partition" of type int expected {p_data}, found {p_type}'
                .format(p_data=p_data, p_type=type(p_data['partition'])),
            )
            return False
        if not isinstance(p_data['replicas'], list):
            _log.error(
                '"replicas" of type list expected {p_data}, found {r_type}'
                .format(p_data=p_data, r_type=type(p_data['replicas'])),
            )
            return False
        if not p_data['replicas']:
            _log.error(
                'Non-empty "replicas" expected: {p_data}'
                .format(p_data=p_data),
            )
            return False
        # Invalid broker-type
        for broker in p_data['replicas']:
            if not isinstance(broker, int):
                _log.error(
                    '"replicas" of type integer list expected {p_data}'
                    .format(p_data=p_data),
                )
                return False
    return True


def _validate_plan(plan):
    """Validate if given plan is valid based on kafka-cluster-assignment protocols.

    Validate following parameters:
    - Correct format of plan
    - Partition-list should be unique
    - Every partition of a topic should have same replication-factor
    - Replicas of a partition should have unique broker-set
    """
    # Validate format of plan
    if not _validate_format(plan):
        return False

    # Verify no duplicate partitions
    partition_names = [
        (p_data['topic'], p_data['partition'])
        for p_data in plan['partitions']
    ]
    duplicate_partitions = [
        partition for partition, count in Counter(partition_names).iteritems()
        if count > 1
    ]
    if duplicate_partitions:
        _log.error(
            'Duplicate partitions in plan {p_list}'
            .format(p_list=duplicate_partitions),
        )
        return False

    # Verify no duplicate brokers in partition-replicas
    dup_replica_brokers = []
    for p_data in plan['partitions']:
        dup_replica_brokers = [
            broker
            for broker, count in Counter(p_data['replicas']).items()
            if count > 1
        ]
        if dup_replica_brokers:
            _log.error(
                'Duplicate brokers: ({topic}, {p_id}) in replicas {replicas}'
                .format(
                    topic=p_data['topic'],
                    p_id=p_data['partition'],
                    replicas=p_data['replicas'],
                )
            )
            return False

    # Verify same replication-factor for every topic
    topic_replication_factor = {}
    for partition_info in plan['partitions']:
        topic = partition_info['topic']
        replication_factor = len(partition_info['replicas'])
        if topic in topic_replication_factor.keys():
            if topic_replication_factor[topic] != replication_factor:
                _log.error(
                    'Mismatch in replication-factor of partitions for topic '
                    '{topic}'.format(topic=topic),
                )
                return False
        else:
            topic_replication_factor[topic] = replication_factor
    return True
