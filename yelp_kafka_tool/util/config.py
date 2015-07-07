from __future__ import print_function

import sys

import yaml
from yelp_kafka.discovery import get_all_clusters, get_local_cluster


conf = None  # The content of the config file
debug = False  # Set to true to print debug info


def get_cluster_config(cluster_type, cluster_name=None):
    if not cluster_name:
        cluster_config = get_local_cluster(cluster_type)
    else:
        matching_clusters = [cluster for cluster in get_all_clusters(
            cluster_type
        ) if cluster_name in cluster.name]
        if len(matching_clusters) == 0:
            print("[ERROR] Cluster \'{0}\' not found.".format(cluster_name), file=sys.stderr)
            sys.exit(1)
        if len(matching_clusters) > 1:
            print("[ERROR] Multiple clusters with the same name.", file=sys.stderr)
            sys.exit(1)
        cluster_config = matching_clusters[0]
    return cluster_config


def load(path):
    global conf
    with open(path, 'r') as config_file:
        conf = yaml.load(config_file)
    return conf
