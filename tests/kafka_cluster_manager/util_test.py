from collections import OrderedDict
from pytest import fixture
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    get_reduced_proposed_plan,
    get_assignment_map,
)


@fixture
def orig_assignment():
    return OrderedDict([
        ((u'T1', 1), [2, 1]),
        ((u'T0', 0), [0, 1]),
        ((u'T0', 1), [1, 2]),
        ((u'T1', 0), [0, 1]),
        ((u'T2', 0), [3, 1]),
    ])


@fixture
def new_assignment():
    return OrderedDict([
        ((u'T0', 0), [2, 0]),
        ((u'T1', 1), [2, 3]),
        ((u'T0', 1), [2, 1]),
        ((u'T1', 0), [0, 1]),
        ((u'T2', 0), [1, 3]),
    ])


def test_reduced_proposed_plan_no_change(orig_assignment):
    # Provide same assignment
    proposed_assignment = get_reduced_proposed_plan(
        original_assignment=orig_assignment,
        new_assignment=orig_assignment,
        max_changes=1,
    )

    # Verify no proposed plan
    assert proposed_assignment == {}


def test_reduced_proposed_plan_max_invalid(orig_assignment, new_assignment):
    # Provide less than max_changes parameter
    proposed_assignment = get_reduced_proposed_plan(
        orig_assignment,
        new_assignment,
        0,
    )

    # Verify no proposed plan
    assert proposed_assignment == {}

    proposed_assignment = get_reduced_proposed_plan(
        orig_assignment,
        new_assignment,
        -1,
    )

    # Verify no proposed plan
    assert proposed_assignment == {}


def test_reduced_proposed_plan_empty(orig_assignment):
    # Provide empty assignment
    proposed_assignment = get_reduced_proposed_plan(
        orig_assignment,
        new_assignment=None,
        max_changes=1,
    )

    # Verify no proposed plan
    assert proposed_assignment == {}

    proposed_assignment = get_reduced_proposed_plan(
        original_assignment=None,
        new_assignment=orig_assignment,
        max_changes=1,
    )

    # Verify no proposed plan
    assert proposed_assignment == {}


def test_reduced_proposed_plan(orig_assignment, new_assignment):
    # Provide same assignment
    proposed_assignment = get_reduced_proposed_plan(
        orig_assignment,
        new_assignment,
        2,
    )

    # Verify lenght of proposed-plan actions as 2
    assert len(proposed_assignment['partitions']) == 2

    proposed_assignment = get_reduced_proposed_plan(
        orig_assignment,
        new_assignment,
        5,
    )

    # Verify no proposed plan less than max-changes
    assert len(proposed_assignment['partitions']) == 4

    # Verify that proposed-plan is first change in sorted order of
    # partition-name
    assert sorted(proposed_assignment['partitions']) == sorted([
        {'partition': 0, 'topic': u'T0', 'replicas': [2, 0]},
        {'partition': 1, 'topic': u'T0', 'replicas': [2, 1]},
        {'partition': 1, 'topic': u'T1', 'replicas': [2, 3]},
        {'partition': 0, 'topic': u'T2', 'replicas': [1, 3]}
    ])


def test_assignment_map():
    assignment_json = {u'version': 1, u'partitions': [
        {u'replicas': [0, 1, 2], u'topic': u'T3', u'partition': 0},
        {u'replicas': [0, 3], u'topic': u'T0', u'partition': 1},
    ]}
    expected_map = OrderedDict(sorted([
        (('T3', 0), [0, 1, 2]),
        (('T0', 1), [0, 3]),
    ]))

    assert expected_map == get_assignment_map(assignment_json)
