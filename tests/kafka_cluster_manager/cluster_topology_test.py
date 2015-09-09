import contextlib
from collections import OrderedDict
from mock import sentinel, patch, MagicMock
from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import (
    ClusterTopology,
    KafkaInterface,
)
from yelp_kafka_tool.kafka_cluster_manager.main import ZK
from yelp_kafka.config import ClusterConfig


class TestClusterToplogy(object):
    broker_id_rg_id_map = {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg1', 4: 'rg3'}
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

    @fixture
    def mock_zk(self):
        # Mock zookeeper
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        mock_zk.get_brokers.return_value = self.brokers_info
        mock_zk.get_topics.return_value = self.topic_ids
        return mock_zk

    @fixture
    def ct(self, mock_zk):
        with contextlib.nested(
            self.mock_cluster_assignment(),
            self.mock_get_replication_group_id(),
        ):
            # Create cluster-object
            return ClusterTopology(mock_zk)

    @contextlib.contextmanager
    def mock_cluster_assignment(self):
        with patch.object(
            KafkaInterface,
            "get_cluster_assignment",
            spec=KafkaInterface.get_cluster_assignment,
            return_value=self._initial_assignment,
        ) as mock_cluster_assignment:
            yield mock_cluster_assignment

    @contextlib.contextmanager
    def mock_get_replication_group_id(self):
        with patch.object(
            ClusterTopology,
            "_get_replication_group_id",
            spec=ClusterTopology._get_replication_group_id,
            side_effect=self.get_replication_group_id,
        ) as mock_get_replication_group_id:
            yield mock_get_replication_group_id

    def get_replication_group_id(self, broker):
        return self.broker_id_rg_id_map[broker.id]

    def ct_assignment(self, assignment, broker_ids):
        """Create cluster topology from given assignment."""
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        brokers_info = {broker_id: sentinel.obj for broker_id in broker_ids}
        topic_ids = sorted(set([t_p[0] for t_p in assignment.iterkeys()]))
        print('topic-ids', topic_ids)
        mock_zk.get_brokers.return_value = brokers_info
        mock_zk.get_topics.return_value = topic_ids
        with contextlib.nested(
            patch.object(
                KafkaInterface,
                "get_cluster_assignment",
                spec=KafkaInterface.get_cluster_assignment,
                return_value=assignment,
            ),
            self.mock_get_replication_group_id(),
        ):
            # Create cluster-object
            return ClusterTopology(mock_zk)

    def test_creating_cluster_object(self, ct):
        # Verify creation of_cluster-topology objects
        assert(ct.assignment == ct.initial_assignment)

    def test_sample_cluster_object(self):
        ct = self.ct_assignment(
            self._initial_assignment,
            self.brokers_info.keys(),
        )
        assert(ct.assignment == ct.initial_assignment)

    def test_rebalance_replication_groups(self, ct):
        ct.rebalance_replication_groups()
        expected_assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T0', 1), [2, 0]),
                ((u'T1', 0), [2, 1]),
                ((u'T2', 0), [2]),
                ((u'T2', 1), [1]),
            ]
        )
        assert(ct.assignment == expected_assignment)
