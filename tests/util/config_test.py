import contextlib
from StringIO import StringIO

import mock
import pytest

from yelp_kafka_tool.util.config import ClusterConfig
from yelp_kafka_tool.util.config import load_yaml_config
from yelp_kafka_tool.util.config import TopologyConfiguration
from yelp_kafka_tool.util.error import ConfigurationError

TEST_BASE_KAFKA = '/base/kafka_discovery'

MOCK_TOPOLOGY_CONFIG = """
---
  clusters:
    cluster1:
      broker_list:
        - "mybrokerhost1:9092"
      zookeeper: "0.1.2.3,0.2.3.4/kafka"
    cluster2:
      broker_list:
        - "mybrokerhost2:9092"
      zookeeper: "0.3.4.5,0.4.5.6/kafka"
  local_config:
    cluster: cluster1
"""


MOCK_YAML = {
    'clusters': {
        'cluster1': {
            'broker_list': ["mybrokerhost1:9092"],
            'zookeeper': "0.1.2.3,0.2.3.4/kafka"
        },
        'cluster2': {
            'broker_list': ["mybrokerhost2:9092"],
            'zookeeper': "0.3.4.5,0.4.5.6/kafka"
        }
    },
    'local_config': {
        'cluster': 'cluster1',
    }
}


class TestClusterConfig():

    def test___eq___broker_list(self):
        cluster_config1 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list=['kafka-cluster-1:9092', 'kafka-cluster-2:9092'],
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list=['kafka-cluster-1:9092', 'kafka-cluster-2:9092'],
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        assert cluster_config1 == cluster_config2
        # Re-ordering the list of brokers
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list=['kafka-cluster-2:9092', 'kafka-cluster-1:9092'],
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        assert cluster_config1 == cluster_config2

    def test___eq___broker_str(self):
        cluster_config1 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-1:9092,kafka-cluster-2:9092',
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-1:9092,kafka-cluster-2:9092',
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        assert cluster_config1 == cluster_config2
        # Re-order the comma separated pair of brokers and zookeeper nodes
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-2:9092,kafka-cluster-1:9092',
            zookeeper='zookeeper-cluster-2:2181,zookeeper-cluster-1:2181,'
        )
        assert cluster_config1 == cluster_config2

    def test___ne___broker_str(self):
        cluster_config1 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-1:9092,kafka-cluster-2:9092',
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        # Different comma separated pair of brokers
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-2:9092,kafka-cluster-3:9092',
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        assert cluster_config1 != cluster_config2
        # Different comma separated pair of zookeeper nodes
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list='kafka-cluster-1:9092,kafka-cluster-2:9092',
            zookeeper='zookeeper-cluster-2:2181,zookeeper-cluster-3:2181,'
        )
        assert cluster_config1 != cluster_config2

    def test___ne___broker_list(self):
        cluster_config1 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list=['kafka-cluster-1:9092', 'kafka-cluster-2:9092'],
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        # Different broker list
        cluster_config2 = ClusterConfig(
            type='some_type',
            name='some_cluster',
            broker_list=['kafka-cluster-1:9092', 'kafka-cluster-3:9092'],
            zookeeper='zookeeper-cluster-1:2181,zookeeper-cluster-2:2181,'
        )
        assert cluster_config1 != cluster_config2


@pytest.yield_fixture
def mock_yaml():
    with contextlib.nested(
        mock.patch(
            'yelp_kafka_tool.util.config.load_yaml_config',
            return_value=MOCK_YAML,
            create=True,
        ),
        mock.patch('os.path.isfile', return_value=True)
    ) as (m, mock_isfile):
        yield m


def test_load_yaml():
    stio = StringIO()
    stio.write(MOCK_TOPOLOGY_CONFIG)
    stio.seek(0)
    with mock.patch(
        '__builtin__.open',
        return_value=contextlib.closing(stio)
    ) as mock_open:
        actual = load_yaml_config('test')
        mock_open.assert_called_once_with("test", "r")
        assert actual == MOCK_YAML


class TestTopologyConfig(object):

    def test_missing_cluster(self):
        with pytest.raises(ConfigurationError):
            with mock.patch("os.path.isfile", return_value=False):
                TopologyConfiguration(
                    cluster_type="wrong_cluster",
                    kafka_topology_path=TEST_BASE_KAFKA
                )

    def test_get_local_cluster(self, mock_yaml):
        topology = TopologyConfiguration(
            cluster_type='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        mock_yaml.assert_called_once_with('/base/kafka_discovery/mykafka.yaml')
        actual_cluster = topology.get_local_cluster()
        expected_cluster = ClusterConfig(
            'mykafka',
            'cluster1',
            ['mybrokerhost1:9092'],
            '0.1.2.3,0.2.3.4/kafka',
        )
        assert actual_cluster == expected_cluster

    def test_get_local_cluster_error(self, mock_yaml):
        # Should raise ConfigurationError if a cluster is in region but not in
        # the cluster list
        mock_yaml.return_value = {
            'clusters': {
                'cluster1': {
                    'broker_list': ['mybroker'],
                    'zookeeper': '0.1.2.3,0.2.3.4/kafka'
                },
            },
            'local_config': {
                'cluster': 'cluster3'
            }
        }
        topology = TopologyConfiguration(
            cluster_type='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        # Raise ConfigurationError because cluster 3 does not exist
        with pytest.raises(ConfigurationError):
            topology.get_local_cluster()

    def test_get_all_clusters(self, mock_yaml):
        topology = TopologyConfiguration(
            cluster_type='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        actual_clusters = topology.get_all_clusters()
        expected_clusters = [
            ClusterConfig(
                'mykafka', 'cluster1', ["mybrokerhost1:9092"], "0.1.2.3,0.2.3.4/kafka"
            ),
            ClusterConfig(
                'mykafka', 'cluster2', ["mybrokerhost2:9092"], "0.3.4.5,0.4.5.6/kafka"
            )
        ]
        assert sorted(expected_clusters) == sorted(actual_clusters)

    def test_get_cluster_by_name(self, mock_yaml):
        topology = TopologyConfiguration(
            cluster_type='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )
        actual_cluster = topology.get_cluster_by_name('cluster1')
        expected_cluster = ClusterConfig(
            'mykafka', 'cluster1', ["mybrokerhost1:9092"], "0.1.2.3,0.2.3.4/kafka"
        )
        assert expected_cluster == actual_cluster

    def test_get_cluster_by_name_error(self, mock_yaml):
        topology = TopologyConfiguration(
            cluster_type='mykafka',
            kafka_topology_path=TEST_BASE_KAFKA,
        )

        with pytest.raises(ConfigurationError):
            topology.get_cluster_by_name('does-not-exist')

    def test___eq__(self):
        topology1 = TopologyConfiguration("standard", "/nail/etc/kafka_discovery")
        topology2 = TopologyConfiguration("standard", "/nail/etc/kafka_discovery")
        assert topology1 == topology2

        topology1 = TopologyConfiguration("scribe")
        topology2 = TopologyConfiguration("scribe")
        assert topology1 == topology2

    def test___ne__(self):
        topology1 = TopologyConfiguration("standard", "/nail/etc/kafka_discovery")
        topology2 = TopologyConfiguration("scribe", "/nail/etc/kafka_discovery")
        assert topology1 != topology2

        topology1 = TopologyConfiguration("standard")
        topology2 = TopologyConfiguration("spam")
        assert topology1 != topology2
