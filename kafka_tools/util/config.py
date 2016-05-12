# -*- coding: utf-8 -*-
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
from __future__ import print_function

import logging
import os
from collections import namedtuple

import yaml

from kafka_tools.util.error import ConfigurationError


conf = None  # The content of the config file
debug = False  # Set to true to print debug info

DEFAULT_KAFKA_TOPOLOGY_BASE_PATH = '/nail/etc/kafka_discovery'


class ClusterConfig(
    namedtuple(
        'ClusterConfig',
        ['type', 'name', 'broker_list', 'zookeeper'],
    ),
):
    """Cluster configuration.
    :param name: cluster name
    :param broker_list: list of kafka brokers
    :param zookeeper: zookeeper connection string
    """

    def __ne__(self, other):
        return self.__hash__() != other.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __hash__(self):
        if isinstance(self.broker_list, list):
            broker_list = self.broker_list
        else:
            broker_list = self.broker_list.split(',')
        zk_list = self.zookeeper.split(',')
        return hash((
            self.type,
            self.name,
            ",".join(sorted(filter(None, broker_list))),
            ",".join(sorted(filter(None, zk_list)))
        ))


def load_yaml_config(config_path):
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)


class TopologyConfiguration(object):
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
        cluster_type,
        kafka_topology_path=DEFAULT_KAFKA_TOPOLOGY_BASE_PATH
    ):
        self.kafka_topology_path = kafka_topology_path
        self.cluster_type = cluster_type
        self.log = logging.getLogger(self.__class__.__name__)
        self.clusters = None
        self.local_config = None
        self.load_topology_config()

    def __eq__(self, other):
        if all([
            self.cluster_type == other.cluster_type,
            self.clusters == other.clusters,
            self.local_config == other.local_config,
        ]):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def load_topology_config(self):
        """Load the topology configuration"""
        config_path = os.path.join(
            self.kafka_topology_path,
            '{id}.yaml'.format(id=self.cluster_type),
        )
        self.log.debug("Loading configuration from %s", config_path)
        if os.path.isfile(config_path):
            topology_config = load_yaml_config(config_path)
        else:
            raise ConfigurationError(
                "Topology configuration {0} for cluster {1} "
                "does not exist".format(
                    config_path,
                    self.cluster_type,
                )
            )
        self.log.debug("Topology configuration %s", topology_config)
        try:
            self.clusters = topology_config['clusters']
            self.local_config = topology_config['local_config']
        except KeyError:
            self.log.exception("Invalid topology file")
            raise ConfigurationError("Invalid topology file {0}".format(
                config_path))

    def get_all_clusters(self):
        return [
            ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
            for name, cluster in self.clusters.iteritems()
        ]

    def get_cluster_by_name(self, name):
        if name in self.clusters:
            cluster = self.clusters[name]
            return ClusterConfig(
                type=self.cluster_type,
                name=name,
                broker_list=cluster['broker_list'],
                zookeeper=cluster['zookeeper'],
            )
        raise ConfigurationError("No cluster with name: {0}".format(name))

    def get_local_cluster(self):
        try:
            if self.local_config:
                local_cluster = self.clusters[self.local_config['cluster']]
                return ClusterConfig(
                    type=self.cluster_type,
                    name=self.local_config['cluster'],
                    broker_list=local_cluster['broker_list'],
                    zookeeper=local_cluster['zookeeper'])
        except KeyError:
            self.log.exception("Invalid topology file")
            raise ConfigurationError("Invalid topology file.")

    def __repr__(self):
        return ("TopologyConfig: cluster_type {0}, clusters: {1},"
                "local_config {2}".format(
                    self.cluster_type,
                    self.clusters,
                    self.local_config
                ))


def get_cluster_config(
    cluster_type,
    cluster_name=None,
    kafka_topology_base_path=None,
):
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
    if kafka_topology_base_path:
        topology = TopologyConfiguration(cluster_type, kafka_topology_base_path)
    else:
        topology = TopologyConfiguration(cluster_type)

    if cluster_name:
        return topology.get_cluster_by_name(cluster_name)
    else:
        return topology.get_local_cluster()


def load(path):
    global conf
    with open(path, 'r') as config_file:
        conf = yaml.load(config_file)
    return conf
