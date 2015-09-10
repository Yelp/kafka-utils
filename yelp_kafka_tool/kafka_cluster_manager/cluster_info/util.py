import json
from collections import Counter, OrderedDict
from .display import display_assignment_changes


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
    if original_assignment == new_assignment:
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
    total_changes = len(proposed_assignment)
    red_proposed_plan_list = proposed_assignment[:max_changes]
    red_curr_plan_list = [(tp_repl[0], original_assignment[tp_repl[0]])
                          for tp_repl in red_proposed_plan_list]
    display_assignment_changes(
        red_curr_plan_list,
        red_proposed_plan_list,
        total_changes,
    )
    red_proposed_assignment = dict(
        (ele[0], ele[1])
        for ele in red_proposed_plan_list
    )
    return get_plan_str(red_proposed_assignment)


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
