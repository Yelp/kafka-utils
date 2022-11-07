# Copyright 2016 Yelp Inc.
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
from __future__ import annotations

import glob
import logging
import os
from typing import Iterator
from typing import NamedTuple

import yaml
from typing_extensions import TypedDict

from kafka_utils.util.error import ConfigurationError
from kafka_utils.util.error import InvalidConfigurationError
from kafka_utils.util.error import MissingConfigurationError


DEFAULT_KAFKA_TOPOLOGY_BASE_PATH = '/etc/kafka_discovery'
HOME_OVERRIDE = '.kafka_discovery'


class ClusterConfig(NamedTuple):
    """Cluster configuration.
    :param name: cluster name
    :param broker_list: list of kafka brokers
    :param zookeeper: zookeeper connection string
    """
    type: str
    name: str
    broker_list: list[str]
    zookeeper: str

    def __ne__(self, other: object) -> bool:
        return self.__hash__() != other.__hash__()

    def __eq__(self, other: object) -> bool:
        return self.__hash__() == other.__hash__()

    def __hash__(self) -> int:
        if isinstance(self.broker_list, list):
            broker_list = self.broker_list
        else:
            broker_list = self.broker_list.split(',')  # type: ignore[unreachable]
        zk_list = self.zookeeper.split(',')
        return hash((
            self.type,
            self.name,
            ",".join(sorted([_f for _f in broker_list if _f])),
            ",".join(sorted([_f for _f in zk_list if _f]))
        ))


class ClusterConfigDict(TypedDict):
    broker_list: list[str]
    zookeeper: str


class LocalConfigDict(TypedDict):
    cluster: str


class TopologyConfigurationDict(TypedDict):
    clusters: dict[str, ClusterConfigDict]
    local_config: LocalConfigDict


def load_yaml_config(config_path: str) -> TopologyConfigurationDict:
    with open(config_path) as config_file:
        return yaml.safe_load(config_file)


class TopologyConfiguration:
    """Topology configuration for a kafka cluster.

    Read a cluster_type.yaml from the kafka_topology_path.
    Example config file:
    .. code-block:: yaml

       clusters:
         cluster1:
             broker_list:
               - "broker1:9092"
               - "broker2:9092"
             zookeeper: "zookeeper1:2181/mykafka"
         cluster2:
             broker_list:
               - "broker3:9092"
               - "broker4:9092"
             zookeeper: "zookeeper2:2181/mykafka"
       local_config:
         cluster: cluster1


    :param cluster_type: kafka cluster type.
    :type cluster_type: string
    :param kafka_topology_path: path of the directory containing
        the kafka topology.yaml config
    :type kafka_topology_path: string
    """

    def __init__(
        self,
        cluster_type: str,
        kafka_topology_path: str = DEFAULT_KAFKA_TOPOLOGY_BASE_PATH,
    ):
        self.kafka_topology_path = kafka_topology_path
        self.cluster_type = cluster_type
        self.log = logging.getLogger(self.__class__.__name__)
        self.clusters: dict[str, ClusterConfigDict] | None = None
        self.local_config: LocalConfigDict | None = None
        self.load_topology_config()

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, TopologyConfiguration)
        if all([
            self.cluster_type == other.cluster_type,
            self.clusters == other.clusters,
            self.local_config == other.local_config,
        ]):
            return True
        return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def load_topology_config(self) -> None:
        """Load the topology configuration"""
        config_path = os.path.join(
            self.kafka_topology_path,
            f'{self.cluster_type}.yaml',
        )
        self.log.debug("Loading configuration from %s", config_path)
        if os.path.isfile(config_path):
            topology_config = load_yaml_config(config_path)
        else:
            raise MissingConfigurationError(
                "Topology configuration {} for cluster {} "
                "does not exist".format(
                    config_path,
                    self.cluster_type,
                )
            )
        self.log.debug("Topology configuration %s", topology_config)
        try:
            self.clusters = topology_config['clusters']
        except KeyError:
            self.log.exception("Invalid topology file")
            raise InvalidConfigurationError("Invalid topology file {}".format(
                config_path))
        if 'local_config' in topology_config:
            self.local_config = topology_config['local_config']

    def get_all_clusters(self) -> list[ClusterConfig]:
        assert self.clusters is not None
        return [
            ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
            for name, cluster in self.clusters.items()
        ]

    def get_cluster_by_name(self, name: str) -> ClusterConfig:
        assert self.clusters is not None
        if name in self.clusters:
            cluster = self.clusters[name]
            return ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
        raise ConfigurationError(f"No cluster with name: {name}")

    def get_local_cluster(self) -> ClusterConfig:
        if self.local_config:
            try:
                assert self.clusters is not None
                local_cluster = self.clusters[self.local_config['cluster']]
                return ClusterConfig(
                    type=self.cluster_type,
                    name=self.local_config['cluster'],
                    broker_list=local_cluster['broker_list'],
                    zookeeper=local_cluster['zookeeper'])
            except KeyError:
                self.log.exception("Invalid topology file")
                raise InvalidConfigurationError("Invalid topology file")
        else:
            raise ConfigurationError("No default local cluster configured")

    def __repr__(self) -> str:
        return ("TopologyConfig: cluster_type {}, clusters: {},"
                "local_config {}".format(
                    self.cluster_type,
                    self.clusters,
                    self.local_config
                ))


def get_conf_dirs() -> list[str]:
    config_dirs = []
    if os.environ.get("KAFKA_DISCOVERY_DIR"):
        config_dirs.append(os.environ["KAFKA_DISCOVERY_DIR"])
    if os.environ.get("HOME"):
        home_config = os.path.join(
            os.path.abspath(os.environ['HOME']),
            HOME_OVERRIDE,
        )
        if os.path.isdir(home_config):
            config_dirs.append(home_config)
    config_dirs.append(DEFAULT_KAFKA_TOPOLOGY_BASE_PATH)
    return config_dirs


def get_cluster_config(
    cluster_type: str,
    cluster_name: str | None = None,
    kafka_topology_base_path: str | None = None,
) -> ClusterConfig:
    """Return the cluster configuration.
    Use the local cluster if cluster_name is not specified.

    :param cluster_type: the type of the cluster
    :type cluster_type: string
    :param cluster_name: the name of the cluster
    :type cluster_name: string
    :param kafka_topology_base_path: base path to look for <cluster_type>.yaml
    :type cluster_name: string
    :returns: the cluster
    :rtype: ClusterConfig
    """
    if not kafka_topology_base_path:
        config_dirs = get_conf_dirs()
    else:
        config_dirs = [kafka_topology_base_path]

    topology = None
    for config_dir in config_dirs:
        try:
            topology = TopologyConfiguration(
                cluster_type,
                config_dir,
            )
        except MissingConfigurationError:
            pass
    if not topology:
        raise MissingConfigurationError(
            f"No available configuration for type {cluster_type}",
        )

    if cluster_name:
        return topology.get_cluster_by_name(cluster_name)
    else:
        return topology.get_local_cluster()


def iter_configurations(kafka_topology_base_path: str | None = None) -> Iterator[TopologyConfiguration]:
    """Cluster topology iterator.
    Iterate over all the topologies available in config.
    """
    if not kafka_topology_base_path:
        config_dirs = get_conf_dirs()
    else:
        config_dirs = [kafka_topology_base_path]

    types = set()
    for config_dir in config_dirs:
        new_types = [x for x in map(
            lambda x: os.path.basename(x)[:-5],
            glob.glob(f'{config_dir}/*.yaml'),
        ) if x not in types]
        for cluster_type in new_types:
            try:
                topology = TopologyConfiguration(
                    cluster_type,
                    config_dir,
                )
            except ConfigurationError:
                continue
            types.add(cluster_type)
            yield topology
