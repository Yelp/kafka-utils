from __future__ import print_function

import yaml
from yelp_kafka.config import TopologyConfiguration


conf = None  # The content of the config file
debug = False  # Set to true to print debug info


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
    :rtype: yelp_kafka.config.ClusterConfig
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
