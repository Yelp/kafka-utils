from mock import sentinel
import pytest

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import ClusterTopology

# TODO: rename all variables into capital letters;
_name = 'localhost'
_zk = sentinel.zk
broker_ids = [0, 1, 2]
topic_ids = ['T0', 'T1', 'T2']
_fetch_initial_assignment(side_effect=None)
_initial_assignment = OrderedDict(
    [
        ((u'T0', 0), [0, 1]),
        ((u'T0', 1), [1, 0]),
        ((u'T1', 0), [2, 1]),
        ((u'T2', 0), [2]),
        ((u'T2', 1), [1]),
    ]
)

broker_id_rg_id_map = {0: 'rg1', 1: 'rg1', 2: 'rg2'}


def get_rg_id(broker_id):
    return broker_id_rg_id_map[broker_id]

rg_id_mock._get_replication_group_id(return_value=get_rg_id(broker.id))
mock_ct._initial_assignment = _initial_assignment
mock_ct._init_(broker_ids=broker_ids, topic_ids=topic_ids)


class TestClusterToplogy(object):
    @pytest.fixture
    def ct(self):
        # Mock: zookeeper to return assignment as we want
        return ClusterTopology(sentinel.zk)

    def test_rebalance_partition():
        mock_ct = mock(spec=ClusterTopology(sentinel.zk))
        initial_assignment = mock_ct.assignment
        assert(initial_assignment == _initial_assignment)
        mock_ct.reassign_partitions()
