# -*- coding: utf-8 -*-
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import contextlib
from StringIO import StringIO

import mock
import pytest

from kafka_tools.util.config import ClusterConfig
from kafka_tools.util.config import load_yaml_config
from kafka_tools.util.config import TopologyConfiguration
from kafka_tools.util.error import ConfigurationError

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


MOCK_YAML_1 = {
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


MOCK_YAML_2 = {
    'clusters': {
        'cluster3': {
            'broker_list': ["mybrokerhost3:9092"],
            'zookeeper': "0.1.2.3,0.2.3.4/kafka_2"
        },
        'cluster4': {
            'broker_list': ["mybrokerhost4:9092"],
            'zookeeper': "0.3.4.5,0.4.5.6/kafka_2"
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

    def get_fake_yaml(path):
        if 'mykafka' in path:
            return MOCK_YAML_1
        else:
            return MOCK_YAML_2

    with contextlib.nested(
        mock.patch(
            'kafka_tools.util.config.load_yaml_config',
            side_effect=get_fake_yaml,
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
        assert actual == MOCK_YAML_1


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
        mock_yaml.side_effect = None
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

    def test___eq__(self, mock_yaml):
        topology1 = TopologyConfiguration("mykafka", "/nail/etc/kafka_discovery")
        topology2 = TopologyConfiguration("mykafka", "/nail/etc/kafka_discovery")
        assert topology1 == topology2

        topology1 = TopologyConfiguration("mykafka")
        topology2 = TopologyConfiguration("mykafka")
        assert topology1 == topology2

    def test___ne__(self, mock_yaml):
        topology1 = TopologyConfiguration("mykafka", "/nail/etc/kafka_discovery")
        topology2 = TopologyConfiguration("somethingelse", "/nail/etc/kafka_discovery")
        assert topology1 != topology2

        topology1 = TopologyConfiguration("mykafka")
        topology2 = TopologyConfiguration("somethingelse")
        assert topology1 != topology2
