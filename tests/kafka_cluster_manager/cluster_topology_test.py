from collections import OrderedDict
from mock import sentinel, patch, MagicMock

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import (
    ClusterTopology,
)
from yelp_kafka_tool.kafka_cluster_manager.main import ZK
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    get_assignment_map,
)
from yelp_kafka.config import ClusterConfig


class TestClusterToplogy(object):
    broker_id_rg_id_map = {0: 'rg1', 1: 'rg1', 2: 'rg3'}
    topic_ids = ['T0', 'T1', 'T2']
    brokers_info = {
        '0': sentinel.obj1,
        '1': sentinel.obj2,
        '2': sentinel.obj3
    }
    _initial_assignment = OrderedDict(
        [
            ((u'T0', 0), [0, 1]),
            ((u'T0', 1), [1, 0]),
            ((u'T1', 0), [2, 1]),
            ((u'T2', 0), [2]),
            ((u'T2', 1), [1]),
        ]
    )

    def get_replication_group_id(self, broker):
        return self.broker_id_rg_id_map[broker.id]

    @patch('yelp_kafka_tool.kafka_cluster_manager.'
           'cluster_info.cluster_topology.KafkaInterface')
    def test_creating_cluster_object(self, mock_kafka):
        with patch.object(
            ClusterTopology,
            "_get_replication_group_id",
            spec=ClusterTopology._get_replication_group_id,
            side_effect=self.get_replication_group_id,
        ):
            # Mock zookeeper
            mock_cluster_config = MagicMock(spec=ClusterConfig)
            mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)

            mock_zk.get_brokers.return_value = self.brokers_info
            mock_zk.get_topics.return_value = self.topic_ids
            mock_kafka.return_value.get_cluster_assignment.return_value = \
                self._initial_assignment

            # Create cluster-object
            ct = ClusterTopology(mock_zk)
            mock_kafka.get_assignment_map.side_effect = get_assignment_map(
                ct.get_assignment_json()
            )

            # Verify creation of_objects
            assert(ct.assignment == ct.initial_assignment)
