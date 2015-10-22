from __future__ import print_function

import argparse
import sys
import time

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


HEADER_MESSAGE = "Will restart the following brokers in {0}:"
CONFIRM_MESSAGE = "Do you want to restart these brokers?"

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
        help='the interval between each check, in second',
        type=int,
        default=DEFAULT_CHECK_INTERVAL,
    )
    parser.add_argument(
        '--check-count',
        help=('the minimum number of time the cluster should result stable ',
              'before restarting the next broker'),
        type=int,
        default=DEFAULT_CHECK_COUNT,
    )
    parser.add_argument(
        '--jolokia-port',
        help='the jolokia port on the server',
        type=int,
        default=DEFAULT_JOLOKIA_PORT,
    )
    parser.add_argument(
        '--jolokia-prefix',
        help='the jolokia HTTP prefix',
        default=DEFAULT_JOLOKIA_PREFIX,
    )
    parser.add_argument(
        '--no-confirm',
        help='proceed without asking confirmation',
        action="store_true",
    )
    return parser.parse_args()


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


def generate_requests(hosts, jolokia_port, jolokia_prefix):
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
    under_replicated = 0
    missing_brokers = 0
    for host, request in generate_requests(hosts, jolokia_port, jolokia_prefix):
        try:
            json = request.result().json()
            under_replicated += json['value']
        except RequestException as e:
            print("Error while fetching {0}: {1}".format(host, e), file=sys.stderr)
            missing_brokers += 1
        except KeyError:
            print("Cannot find the key, Kafka is probably starting up", file=sys.stderr)
            missing_brokers += 1
    return under_replicated, missing_brokers


def print_brokers(cluster_config, brokers):
    print(HEADER_MESSAGE.format(cluster_config.name))
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


@task
def restart_broker():
    sudo(RESTART_COMMAND)


def wait_for_cluster_stable(
    hosts,
    jolokia_port,
    jolokia_prefix,
    check_interval,
    check_count,
):
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
    brokers = get_broker_list(cluster_config)
    print_brokers(cluster_config, brokers)
    if opts.no_confirm or ask_confirmation(CONFIRM_MESSAGE):
        print("\nExecute restart")
        execute_rolling_restart(
            brokers,
            opts.jolokia_port,
            opts.jolokia_prefix,
            opts.check_interval,
            opts.check_count,
        )
