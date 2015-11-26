from __future__ import division
from __future__ import print_function

import argparse
import itertools
import logging
import math
import sys
import time
from operator import itemgetter

from fabric.api import execute
from fabric.api import hide
from fabric.api import settings
from fabric.api import run
from fabric.tasks import Task
from fabric.api import task
from requests.exceptions import RequestException
from requests_futures.sessions import FuturesSession
from yelp_kafka import discovery
from yelp_kafka.error import ConfigurationError

from yelp_kafka_tool.util.zookeeper import ZK


DEFAULT_DATA_PATH = "/nail/var/kafka/md1/kafka-logs"
DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-8-oracle-1.8.0.20/"

DEFAULT_MIN = 60  # Last hour

FIND_COMMAND = "find {data_path} -mtime -{min} -name '*.log'"
CHECK_COMMAND = "JAVA_HOME=\"{java_home}\"kafka-run-class kafka.tools.DumpLogSegments --files {files}"


class WaitTimeoutException(Exception):
    pass


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
        '--min',
        help=('check all the log files modified in the last --min minutes'
              'Default: %(default)s minutes'),
        type=int,
        default=DEFAULT_MIN,
    )
    parser.add_argument(
        '--leader-only',
        help='only check current leader data. Default: %(default)s',
        action="store_true",
        default=True,
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    return parser.parse_args()


def get_cluster(cluster_type, cluster_name):
    """Return the cluster configuration, given cluster type and name.
    Use the local cluster if cluster_name is not specified.

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


def get_broker_list(cluster_config):
    """Returns a dictionary of brokers in the form {id: host}

    :param cluster_config: the configuration of the cluster
    :type cluster_config: map
    """
    with ZK(cluster_config) as zk:
        brokers = sorted(zk.get_brokers().items(), key=itemgetter(0))
        return [(id, data['host']) for id, data in brokers]


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
def find_files_task(data_path, min):
    """Find the log files depending on their modification time.

    :param data_path: the path to the Kafka data directory
    :type data_path: string
    :param min: return only files modified in the last 'min' minutes
    :type min: integer
    """
    command = FIND_COMMAND.format(data_path=data_path, min=min)
    run(command)


@task
def check_corrupted_files_task(java_home, files):
    """Check the file corruption of the specified files.

    :param java_home: the JAVA_HOME
    :type java_home: string
    :param files: list of files to be checked
    :type files: list of string
    """
    files_str = ",".join(files)
    command = CHECK_COMMAND.format(java_home=java_home, files=files_str)
    run(command)


def validate_opts(opts, brokers_num):
    """Basic option validation. Returns True if the options are not valid,
    False otherwise.

    :param opts: the command line options
    :type opts: map
    :param brokers_num: the number of brokers
    :type brokers_num: integer
    :returns: bool
    """
    if opts.min <= 0:
        print("Error: --min must be > 0")
        return True
    return False


def check_cluster(brokers, min_minutes, verbose):
    hidden = [] if verbose else ['output', 'running']
    for broker_id, host in brokers:
        print(broker_id, host)
        with settings(forward_agent=True, connection_attempts=3, timeout=2, host_string=host), hide(*hidden):
            find_files_task(DEFAULT_DATA_PATH, min_minutes)



def run_tool():
    opts = parse_opts()
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)
    cluster_config = get_cluster(opts.cluster_type, opts.cluster_name)
    brokers = get_broker_list(cluster_config)
    if validate_opts(opts, len(brokers)):
        sys.exit(1)
#    print_brokers(cluster_config, brokers[opts.skip:])
    check_cluster(brokers, opts.min, opts.verbose)
