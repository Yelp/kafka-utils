from __future__ import print_function

import argparse
import sys
import time

from collections import OrderedDict

from fabric.api import execute
from fabric.api import settings
from fabric.api import sudo
from fabric.api import task

from operator import itemgetter
from requests_futures.sessions import FuturesSession
from requests.exceptions import RequestException

from yelp_kafka import discovery
from yelp_kafka.error import ConfigurationError
from yelp_kafka_tool.util.zookeeper import ZK


RESTART_COMMAND = "service kafka restart"

UNDER_REPL_KEY = "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager/Value"

DEFAULT_CHECK_INTERVAL = 10  # seconds
DEFAULT_CHECK_COUNT = 12
DEFAULT_JOLOKIA_PORT = 8778
DEFAULT_JOLOKIA_PREFIX = "jolokia/"


def parse_opts():
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
        '--check-interval',
        help=('the interval between each check, in seconds. '
              'Default: %(default)s seconds'),
        type=int,
        default=DEFAULT_CHECK_INTERVAL,
    )
    parser.add_argument(
        '--check-count',
        help=('the minimum number of time the cluster should result stable '
              'before restarting the next broker. Default: %(default)s'),
        type=int,
        default=DEFAULT_CHECK_COUNT,
    )
    parser.add_argument(
        '--jolokia-port',
        help='the jolokia port on the server. Default: %(default)s',
        type=int,
        default=DEFAULT_JOLOKIA_PORT,
    )
    parser.add_argument(
        '--jolokia-prefix',
        help='the jolokia HTTP prefix. Default: %(default)s',
        default=DEFAULT_JOLOKIA_PREFIX,
    )
    parser.add_argument(
        '--no-confirm',
        help='proceed without asking confirmation. Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '--skip',
        help=('the number of brokers to skip without restarting. '
              'Default: %(default)s'),
        type=int,
        default=0,
    )
    return parser.parse_args()


def get_cluster(cluster_type, cluster_name):
    """Returns the cluster configuration, given cluster type and name.
    Use the local cluster if cluster_name is not speficied.

    :param cluster_type: the type of the cluster
    :type cluster_type: string
    :param cluster_name: the name of the cluster
    :type cluster_name: string
    """
    try:
        if cluster_name:
            return discovery.get_cluster_by_name(cluster_type, cluster_name)
        else:
            return discovery.get_local_cluster(cluster_type)
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)


def get_broker_list(cluster_config, skip):
    """Returns a dictionary of brokers in the form {id: host}

    :param cluster_config: the configuration of the cluster
    :type cluster_config: map
    :param skip: the number of brokers to skip
    :type skip: integer
    """
    with ZK(cluster_config) as zk:
        result = OrderedDict([])
        brokers = sorted(zk.get_brokers().items(), key=itemgetter(0))
        for id, data in brokers[skip:]:
            result[id] = data['host']
        return result


def generate_requests(hosts, jolokia_port, jolokia_prefix):
    """Return a generator of requests to fetch the under replicated
    partition number from the specified hosts.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param jolokia_port: HTTP port for Jolokia
    :type jolokia_port: integer
    :param jolokia_prefix: HTTP prefix on the server for the Jolokia queries
    :type jolokia_prefix: string
    :returns: generator of requests
    """
    session = FuturesSession()
    for host in hosts:
        url = "http://{host}:{port}/{prefix}/read/{key}".format(
            host=host,
            port=jolokia_port,
            prefix=jolokia_prefix,
            key=UNDER_REPL_KEY,
        )
        yield host, session.get(url)


def read_cluster_status(hosts, jolokia_port, jolokia_prefix):
    """Read and return the number of under replicated partitions and
    missing brokers from the specified hosts.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param jolokia_port: HTTP port for Jolokia
    :type jolokia_port: integer
    :param jolokia_prefix: HTTP prefix on the server for the Jolokia queries
    :type jolokia_prefix: string
    :returns: tuple of integers
    """
    under_replicated = 0
    missing_brokers = 0
    for host, request in generate_requests(hosts, jolokia_port, jolokia_prefix):
        try:
            json = request.result().json()
            under_replicated += json['value']
        except RequestException as e:
            print("Broker {0} is down: {1}".format(host, e), file=sys.stderr)
            missing_brokers += 1
        except KeyError:
            print("Cannot find the key, Kafka is probably starting up", file=sys.stderr)
            missing_brokers += 1
    return under_replicated, missing_brokers


def print_brokers(cluster_config, brokers):
    """Print the list of brokers that will be restarted.

    :param cluster_config: the cluster configuration as returned by yelpkafka discovery
    :type cluster_config: map
    :param brokers: the brokers that will be restarted
    :type brokers: map of broker ids and host names
    """
    print("Will restart the following brokers in {0}:".format(cluster_config.name))
    for id, host in sorted(brokers.items(), key=itemgetter(0)):
        print("  {0}: {1}".format(id, host))
    print()


def ask_confirmation():
    """Ask for confirmation to the user. Return true if the user confirmed
    the execution, false otherwise.
    :returns: bool
    """
    while True:
        print("Do you want to restart these brokers? ", end="")
        choice = raw_input().lower()
        if choice in ['yes', 'y']:
            return True
        elif choice in ['no', 'n']:
            return False
        else:
            print("Please respond with 'yes' or 'no'")


@task
def restart_broker():
    """Execute the restart"""
    sudo(RESTART_COMMAND)


def wait_for_cluster_stable(
    hosts,
    jolokia_port,
    jolokia_prefix,
    check_interval,
    check_count,
):
    """
    Block the caller until the cluster can be considered stable.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param jolokia_port: HTTP port for Jolokia
    :type jolokia_port: integer
    :param jolokia_prefix: HTTP prefix on the server for the Jolokia queries
    :type jolokia_prefix: string
    :param check_interval: the number of seconds it will wait between each check
    :type check_interval: integer
    :param check_count: the number of times the check should be positive before
    restarting the next broker
    :type check_count: integer
    """
    stable_counter = 0
    while True:
        partitions, brokers = read_cluster_status(
            hosts,
            jolokia_port,
            jolokia_prefix,
        )
        if partitions == 0 and brokers == 0:
            stable_counter += 1
        else:
            stable_counter = 0
        print(
            "Under replicated partitions: {0}, missing brokers: {1} ({2}/{3})".format(
                partitions,
                brokers,
                stable_counter,
                check_count,
            ))
        if stable_counter >= check_count:
            break
        time.sleep(check_interval)
    print("The cluster is stable")


def execute_rolling_restart(
    brokers,
    jolokia_port,
    jolokia_prefix,
    check_interval,
    check_count,
):
    """Execute the rolling restart on the specified brokers. It check the
    number of under replicated partitions on each broker, using Jolokia.

    The check is performed at constant intervals, and a broker will be restarted
    when all the brokers are answering and are reporting zero under replicated
    partitions.

    :param brokers: the brokers that will be restarted
    :type brokers: map of broker ids and host names
    :param jolokia_port: HTTP port for Jolokia
    :type jolokia_port: integer
    :param jolokia_prefix: HTTP prefix on the server for the Jolokia queries
    :type jolokia_prefix: string
    :param check_interval: the number of seconds it will wait between each check
    :type check_interval: integer
    :param check_count: the number of times the check should be positive before
    restarting the next broker
    :type check_count: integer
    """
    for n, host in enumerate(brokers.values()):
        with settings(forward_agent=True, connection_attempts=3, timeout=2):
            wait_for_cluster_stable(
                brokers.values(),
                jolokia_port,
                jolokia_prefix,
                check_interval,
                1 if n == 0 else check_count
            )
            result = execute(restart_broker, hosts=host)
            print(result)
    return result


def run():
    opts = parse_opts()
    cluster_config = get_cluster(opts.cluster_type, opts.cluster_name)
    brokers = get_broker_list(cluster_config, opts.skip)
    print_brokers(cluster_config, brokers)
    if opts.no_confirm or ask_confirmation():
        print("\nExecute restart")
        execute_rolling_restart(
            brokers,
            opts.jolokia_port,
            opts.jolokia_prefix,
            opts.check_interval,
            opts.check_count,
        )
