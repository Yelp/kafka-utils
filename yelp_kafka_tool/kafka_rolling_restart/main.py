from __future__ import print_function

import argparse
import sys

from fabric.api import env
from fabric.api import execute
from fabric.api import settings
from fabric.api import sudo
from fabric.api import task

from operator import itemgetter

from yelp_kafka import discovery

from yelp_kafka_tool.util.zookeeper import ZK


PRINT_MESSAGE = "Will restart the following brokers in {0}:"
CONFIRM_MESSAGE = "Do you want to restart these brokers?"

RESTART_COMMAND = "service kafka restart"


@task
def restart_broker():
    #sudo(RESTART_COMMAND)
    sudo("ls /")


def execute_rolling_restart(brokers, jmx_port):
    for _, host in brokers.items():
        with settings(forward_agent=True, connection_attempts=3, timeout=2):
            try:
                result = execute(restart_broker, hosts=host)
                print(result)
            except Exception as e:
                print("Unexpected exception {0}".format(e))
                return result
    return result


def get_cluster(cluster_type, cluster_name):
    try:
        if cluster_name:
            return discovery.get_cluster_by_name(cluster_type, cluster_name)
        else:
            return discovery.get_local_cluster(cluster_type)
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)


def get_broker_list(cluster_config):
    """Returns a dictionary of brokers in the form {id: host}"""
    with ZK(cluster_config) as zk:
        result = {}
        for id, data in zk.get_brokers().items():
            result[id] = data['host']
        return result


def parse_options():
    parser = argparse.ArgumentParser(
        description=('Performs a rolling restart of the specified'
                     'kafka cluster.'))
    parser.add_argument(
        '--cluster-type',
        required=True,
        help='cluster type, e.g. "standard"',
    )
    parser.add_argument(
        '--cluster-name',
        help='cluster name, e.g. "uswest1-devc" (defaults to local cluster)',
    )
    parser.add_argument(
        '--no-confirm',
        help='proceed without asking confirmation',
        action="store_true",
    )
    return parser.parse_args()


def print_brokers(cluster_config, brokers):
    print(PRINT_MESSAGE.format(cluster_config.name))
    for id, host in sorted(brokers.items(), key=itemgetter(0)):
        print("  {0}: {1}".format(id, host))


def ask_confirmation(message):
    while True:
        print(message + " ", end="")
        choice = raw_input().lower()
        if choice == 'yes':
            return True
        elif choice == 'no':
            return False
        else:
            print("Please respond with 'yes' or 'no'")


def run():
    options = parse_options()
    cluster_config = get_cluster(options.cluster_type, options.cluster_name)
    brokers = get_broker_list(cluster_config)
    print_brokers(cluster_config, brokers)
    if options.no_confirm or ask_confirmation(CONFIRM_MESSAGE):
        print("\nExecute restart")
        print(execute_rolling_restart(brokers, 1234))

