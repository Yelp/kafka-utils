import json
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


def get_reduced_proposed_plan(original_assignment, new_assignment, max_changes):
    """Return new plan with upper limit on total actions.

    These actions involve actual partition movement
    and/or change in preferred leader.
    Get the difference of current and new proposed plan
    and take the subset of this plan for given limit.
    Convert the resultant assignment into json format and return.

    Argument(s):
    original_assignment: Current assignment of cluster in zookeeper
    new_assignment:     New proposed-assignment of cluster
    max_changes:        Maximum number of actions allowed
    """
    if original_assignment == new_assignment or \
            max_changes < 1 or not original_assignment or not new_assignment:
        return {}
    assert(
        set(original_assignment.keys()) == set(new_assignment.keys())
    ), 'Mismatch in topic-partitions set in original and proposed plans.'
    # Get change-list for given assignments
    proposed_assignment = [
        (t_p_key, new_assignment[t_p_key])
        for t_p_key, replica in original_assignment.iteritems()
        if replica != new_assignment[t_p_key]
    ]
    tot_actions = len(proposed_assignment)
    red_proposed_plan_list = proposed_assignment[:max_changes]
    red_curr_plan_list = [(tp_repl[0], original_assignment[tp_repl[0]])
                          for tp_repl in red_proposed_plan_list]
    red_proposed_assignment = dict(
        (ele[0], ele[1])
        for ele in red_proposed_plan_list
    )
    plan_str = get_plan_str(red_proposed_assignment)
    return plan_str, red_curr_plan_list, red_proposed_plan_list, tot_actions


def get_plan_str(proposed_assignment):
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


def separate_groups(groups, key):
    """Given a list of group objects, and a function to extract the number of
    elements for each of them, return the list of groups that have an excessive
    number of elements (when compared to a uniform distribution), a list of
    groups with insufficient elements, and a list of groups that already have
    the optimal number of elements.

    Examples:
        separate_groups([12, 10, 10, 11], lambda g: g) => ([12], [10], [11, 10])
        separate_groups([12,  8, 12, 11], lambda g: g) => ([12, 12], [8], [11])
        separate_groups([14,  9,  6, 14], lambda g: g) => ([14, 14], [9, 6], [])
        separate_groups([11,  9, 10, 14], lambda g: g) => ([14], [10, 9], [11])
    """
    optimum, extra = compute_group_optimum(groups, key)
    over_loaded, under_loaded, optimal = [], [], []
    additional_element = bool(extra)

    for group in sorted(groups, key=key, reverse=True):
        n_elements = key(group)
        if n_elements > optimum:
            over_loaded.append(group)
        elif n_elements < optimum + additional_element:
            under_loaded.append(group)
        else:
            optimal.append(group)
    return over_loaded, under_loaded, optimal
