import json
import logging
from collections import Counter
from collections import defaultdict
from collections import OrderedDict

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
    assignment_json: Given unordered assignment in json format
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


def get_reduced_proposed_plan(
    original_assignment,
    new_assignment,
    max_partition_movements,
    max_leader_only_changes,
):
    """Return new plan with upper limit on total actions.

    These actions involve actual partition movement
    and/or change in preferred leader.
    Get the difference of current and new proposed plan
    and take the subset of this plan for given limit.
    Convert the resultant assignment into json format and return.

    Argument(s):
    original_assignment:    Current assignment of cluster in zookeeper
    new_assignment:         New proposed-assignment of cluster
    max_partition_movements:Maximum number of partition-movements in
                            final set of actions
    max_leader_only_changes:Maximum number of actions with leader only changes
    :return:
    :red_curr_plan:         Original replicas of final set of actions
    :type:                  List of tuple (topic-partition, replica)
    :red_proposed_plan:     Final proposed plan for execution
    :type:                  List of tuple (topic-partition, replica)
    :tot_actions:           Total actions to be executed
    :type:                  integer

    """
    if (
        original_assignment == new_assignment or
        max_partition_movements + max_leader_only_changes < 1 or
        not original_assignment or
        not new_assignment
    ):
        return {}
    # Get change-list for given assignments
    proposed_assignment_leaders = [
        (t_p_key, new_assignment[t_p_key])
        for t_p_key, replica in original_assignment.iteritems()
        if replica != new_assignment[t_p_key] and
        set(replica) == set(new_assignment[t_p_key])
    ]
    proposed_assignment_part = [
        (
            t_p_key,
            new_assignment[t_p_key],
            len(set(replica) - set(new_assignment[t_p_key])),
        )
        for t_p_key, replica in original_assignment.iteritems()
        if set(replica) != set(new_assignment[t_p_key])
    ]
    tot_actions = len(proposed_assignment_part) + len(proposed_assignment_leaders)

    # Extract reduced plan maximizing uniqueness of topics
    red_proposed_plan_partitions = extract_actions_unique_topics(
        proposed_assignment_part,
        max_partition_movements,
    )
    red_proposed_plan_leaders = proposed_assignment_leaders[:max_leader_only_changes]
    red_proposed_plan = red_proposed_plan_partitions + red_proposed_plan_leaders
    red_curr_plan = [
        (tp_repl[0], original_assignment[tp_repl[0]])
        for tp_repl in red_proposed_plan
    ]
    return red_curr_plan, red_proposed_plan, tot_actions


def extract_actions_unique_topics(proposed_assignment, max_partition_movements):
    """Extract actions limiting to given max value such that
       the resultant has minimum possible set of duplicate topics.

       Algorithm:
       1. Return given assignment if number of actions < max_partition_movements
       2. Group actions by by topic-name: {topic: action-list}
       3. Iterate through the dictionary in circular fashion and keep
          extracting actions with until max_partition_movements
          are reached.
       :proposed_assignment: Final plan with set of actions and changes
       :type:                Tuple (topic-partition, proposed-replica, replica-change count
       :max_partition_movements: Maximum set of partition-movements allowed
       :type:                    integer
    """
    # Group actions by topic
    topic_actions = defaultdict(list)
    for t_p, replica, replica_change_cnt in proposed_assignment:
        topic_actions[t_p[0]].append((t_p, replica, replica_change_cnt))

    # Create reduced assignment minimizing duplication of topics
    red_proposed_plan = []
    curr_partition_cnt = 0
    prev_partition_cnt = -1
    while curr_partition_cnt < max_partition_movements and \
            prev_partition_cnt < curr_partition_cnt:
        prev_partition_cnt = curr_partition_cnt
        for topic, actions in topic_actions.iteritems():
            if not actions:
                continue
            action = actions[0]
            # If current action increases current partiton-movements
            # skip the action
            if curr_partition_cnt + action[2] > \
                    max_partition_movements:
                continue
            red_proposed_plan.append(action[0:2])
            curr_partition_cnt += action[2]
            actions.remove(action)
            if curr_partition_cnt == max_partition_movements:
                break
    return sorted(red_proposed_plan)


def get_plan(proposed_assignment):
    return {
        'version': 1,
        'partitions':
        [{'topic': t_p_key[0],
          'partition': t_p_key[1],
          'replicas': replica
          } for t_p_key, replica in proposed_assignment.iteritems()]
    }


def confirm_execution():
    """Confirm from your if proposed-plan be executed."""
    permit = ''
    while permit.lower() not in ('yes', 'no'):
        permit = raw_input('Execute Proposed Plan? [yes/no] ')
    if permit.lower() == 'yes':
        return True
    else:
        return False


def proposed_plan_json(proposed_layout, proposed_plan_file):
    """Dump proposed json plan to given output file for future usage."""
    with open(proposed_plan_file, 'w') as output:
        json.dump(proposed_layout, output)


def compute_group_optimum(groups, key):
    total = sum(key(g) for g in groups)
    return total // len(groups), total % len(groups)


def smart_separate_groups(groups, key):
    """Given a list of group objects, and a function to extract the number of
    elements for each of them, return the list of groups that have an excessive
    number of elements (when compared to a uniform distribution), a list of
    groups with insufficient elements, and a list of groups that already have
    the optimal number of elements.

    Examples:
        smart_separate_groups([12, 10, 10, 11], lambda g: g) => ([12], [10], [11, 10])
        smart_separate_groups([12,  8, 12, 11], lambda g: g) => ([12, 12], [8], [11])
        smart_separate_groups([14,  9,  6, 14], lambda g: g) => ([14, 14], [9, 6], [])
        smart_separate_groups([11,  9, 10, 14], lambda g: g) => ([14], [10, 9], [11])
    """
    optimum, extra = compute_group_optimum(groups, key)
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


def separate_groups(groups, key):
    """Separate the group into all potentially overloaded, optimal and
    under-loaded groups.

    The revised over-loaded groups increases the choice space for future
    selection of most suitable group based on on search criteria.

    If all groups from smart-separate are optimal, return the original groups,
    since there's no use of creating potential over-loaded-groups.

    For example:
    Consider, replication-group to replica-count map: (a:4, b:4, c:3, d:2)
    smart_separate_groups sets 'a' and 'c' as optimal, 'b' as over-loaded
    and 'd' as under-loaded, so we transfer the partition from group 'b' to 'd'.

    separate-groups combines 'a' with 'b' as over-loaded, allowing to select
    between these two groups (based on total-partition-count), to transfer the
    partition to 'd'.
    """
    optimum, _ = compute_group_optimum(groups, key)
    over_loaded, under_loaded, optimal = smart_separate_groups(groups, key)
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
    curr_assignment_str,
    base_assignment_str=None,
    active_brokers=None,
    is_partition_subset=True,
):
    """Verify that the curr-assignment is valid for execution.

    Given kafka-reassignment plan should affirm with following rules:
    a) Format of plan is expected format for reassignment to zookeeper
    b) Plan should have at least one  partition for re-assignment
    c) Partition-name list should be subset of base-plan partition-list
    d) Replication-factor for each partition of same topic is same
    e) Replication-factor for each partition remains unchanged
    f) No duplicate broker-ids in each replicas
    g) Broker-ids in replicas be subset of brokers of base-plan
    """
    # Get correct json formatted assignment
    curr_assignment = json.loads(json.dumps(curr_assignment_str))
    # Standard individual validation of given assignment
    if not _validate_assignment(curr_assignment):
        _log.error('Invalid proposed-plan.')
        return False

    # Validate given plan in reference to base-plan
    if base_assignment_str:
        base_assignment = json.loads(json.dumps(base_assignment_str))
        if not _validate_assignment(base_assignment):
            _log.error('Invalid assignment from cluster.')
            return False
        if not _validate_plan_base(
            curr_assignment,
            base_assignment,
            active_brokers,
            is_partition_subset,
        ):
            return False
    # Plan validation successful
    return True


def _validate_plan_base(
    curr_assignment,
    base_assignment,
    active_brokers=None,
    is_partition_subset=True,
):
    """Validate if given assignment is valid comparing with given base-assignment.

    Validate following assertions:
    a) Validate format of given assignments
    b) Partition-check: New partition-set should be subset of base-partition set
    c) Replica-count check: Replication-factor for each partition remains same
    d) Broker-check: New broker-set should be subset of base broker-set
    """

    # Verify that assignment-partitions are subset of base-assignment
    curr_partitions = set([
        (p_data['topic'], p_data['partition'])
        for p_data in curr_assignment['partitions']
    ])
    base_partitions = set([
        (p_data['topic'], p_data['partition'])
        for p_data in base_assignment['partitions']
    ])
    if is_partition_subset:
        invalid_partitions = list(curr_partitions - base_partitions)
    else:
        # partition set should be equal
        invalid_partitions = list(
            curr_partitions.union(base_partitions) -
            curr_partitions.intersection(base_partitions),
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
        for p_data in base_assignment['partitions']
    }
    curr_partition_replicas = {
        (p_data['topic'], p_data['partition']): p_data['replicas']
        for p_data in curr_assignment['partitions']
    }
    invalid_replication_factor = False
    for curr_partition, replicas in curr_partition_replicas.iteritems():
        base_replica_cnt = len(base_partition_replicas[curr_partition])
        if len(replicas) != base_replica_cnt:
            invalid_replication_factor = True
            _log.error(
                'Replication-factor Mismatch: Partition: {partition}: '
                'Base-replicas: {expected}, Proposed-replicas: {actual}'.format(
                    partition=curr_partition,
                    expected=base_partition_replicas[curr_partition],
                    actual=replicas,
                ),
            )
    if invalid_replication_factor:
        return False

    # Verify that replicas-brokers of new-assignment are subset of brokers
    # in base-assignment
    curr_brokers = set([
        broker
        for replicas in curr_partition_replicas.itervalues()
        for broker in replicas
    ])
    if active_brokers:
        base_brokers = set(active_brokers)
    else:
        # If active brokers not given should be checked against base-assignment
        base_brokers = set([
            broker
            for replicas in base_partition_replicas.itervalues()
            for broker in replicas
        ])
    invalid_brokers = list(curr_brokers - base_brokers)
    if invalid_brokers:
        _log.error(
            'Invalid brokers in assignment found {brokers}'
            .format(brokers=invalid_brokers),
        )
        return False

    # Validation successful
    return True


def _validate_format(assignment):
    """Validate if the format of the assignment as expected.

    Validate format of assignment on following rules:
    a) Verify if it ONLY and MUST have keys and value, 'version' and 'partitions'
    b) Verify if each value of 'partitions' ONLY and MUST have keys 'replicas',
        'partition', 'topic'
    c) Verify desired type of each value
    d) Verify non-empty partitions and replicas
    Sample-assignment format:
    {
        "version": 1,
        "partitions": [
            {"partition":0, "topic":'t1', "replicas":[0,1,2]},
            {"partition":0, "topic":'t2', "replicas":[1,2]},
            ...
        ]}
    """
    try:
        assignment = json.loads(json.dumps(assignment))
        # Verify presence of required keys
        if sorted(assignment.keys()) != sorted(['version', 'partitions']):
            _log.error(
                'Invalid or incomplete keys in given plan. Expected: "version", '
                '"partitions". Found:{keys}'
                .format(keys=', '.join(assignment.keys())),
            )
            return False

        # Invalid version
        if assignment['version'] != 1:
            _log.error(
                'Invalid version of assignment {version}'
                .format(version=assignment['version']),
            )
            return False

        # Empty partitions
        if not assignment['partitions']:
            _log.error(
                '"partitions" list found empty"'
                .format(version=assignment['partitions']),
            )
            return False

        # Invalid partitions type
        if not isinstance(assignment['partitions'], list):
            _log.error('"partitions" of type list expected.')
            return False

        # Invalid partition-data
        for p_data in assignment['partitions']:
            if sorted(p_data.keys()) != sorted(['topic', 'partition', 'replicas']):
                _log.error(
                    'Invalid keys in partition-data {keys}'
                    .format(keys=', '.join(p_data.keys())),
                )
                return False
            # Check types of keys
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
    except KeyError as e:
        _log.exception('Invalid given plan format: {e}'.format(e=e))
        return False
    return True


def _validate_assignment(assignment):
    """Validate if given assignment is valid based on kafka-cluster-assignment protocols.

    Validate following parameters:
    a) Correct format of assignment
    b) Partition-list should be unique
    c) Every partition of a topic should have same replication-factor
    d) Replicas of a partition should have unique broker-set
    """
    # Validate format of assignment
    if not _validate_format(assignment):
        return False

    # Verify no duplicate partitions
    partition_names = [
        (p_data['topic'], p_data['partition'])
        for p_data in assignment['partitions']
    ]
    duplicate_partitions = [
        partition for partition, count in Counter(partition_names).iteritems()
        if count > 1
    ]
    if duplicate_partitions:
        _log.error(
            'Duplicate partitions in assignment {p_list}'
            .format(p_list=duplicate_partitions),
        )
        return False

    # Verify no duplicate brokers in partition-replicas
    dup_replica_brokers = []
    for p_data in assignment['partitions']:
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
    for partition_info in assignment['partitions']:
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
