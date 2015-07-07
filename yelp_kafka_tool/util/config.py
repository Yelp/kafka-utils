from __future__ import print_function

import sys

import yaml
from yelp_kafka.discovery import (
    get_cluster_by_name,
    get_local_cluster,
)


conf = None  # The content of the config file
debug = False  # Set to true to print debug info


def get_cluster_config(cluster_type, cluster_name=None):
    if not cluster_name:
        return get_local_cluster(cluster_type)
    else:
        return get_cluster_by_name(cluster_type, cluster_name)


def load(path):
    global conf
    with open(path, 'r') as config_file:
        conf = yaml.load(config_file)
    return conf
