from collections import OrderedDict

from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cmds.store_assignments import StoreAssignmentsCmd


@fixture
def assignment():
    return OrderedDict([
        ((u'T1', 1), [2, 1]),
        ((u'T0', 0), [0, 1]),
        ((u'T0', 1), [1, 2]),
        ((u'T1', 0), [0, 1]),
        ((u'T2', 0), [3, 1]),
    ])


@fixture
def cmd():
    return StoreAssignmentsCmd()


class TestClusterManagerCmd(object):

    def test_reduced_proposed_plan_no_change(self, cmd, assignment):
        # Provide same assignment
        json = cmd.generate_kafka_json(assignment)
        assert json == (
            '{"partitions": [{"partition": 1, "replicas": [2, 1], "topic": "T1"}, '
            '{"partition": 0, "replicas": [0, 1], "topic": "T0"}, '
            '{"partition": 1, "replicas": [1, 2], "topic": "T0"}, '
            '{"partition": 0, "replicas": [0, 1], "topic": "T1"}, '
            '{"partition": 0, "replicas": [3, 1], "topic": "T2"}], '
            '"version": 1}'
        )
