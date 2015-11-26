from __future__ import division
from __future__ import print_function

import argparse
import itertools
import logging
import math
import re
import sys
import time
from operator import itemgetter
from fabric.api import execute
from fabric.api import hide
from fabric.api import settings
from fabric.api import run
from requests.exceptions import RequestException
from requests_futures.sessions import FuturesSession
from yelp_kafka import discovery
from yelp_kafka.error import ConfigurationError

from yelp_kafka_tool.util.zookeeper import ZK


DEFAULT_DATA_PATH = "/nail/var/kafka/md1/kafka-logs"
DEFAULT_JAVA_HOME = "/usr/lib/jvm/java-8-oracle-1.8.0.20/"

DEFAULT_MIN = 60  # Last hour
DEFAULT_BATCH_SIZE = 5

FIND_REC_COMMAND = "find \"{data_path}\" -type f -mtime -{min} -name '*.log'"
FIND_RANGE_COMMAND = "find \"{data_path}\" -type f -newermt \"{start_range}\" \\! -newermt \"{end_range}\" -name '*.log'"
FIND_START+COMMAND = "find \"{data_path}\" -type f -newermt \"{start_range}\" -name '*.log'"
CHECK_COMMAND = "JAVA_HOME=\"{java_home}\" kafka-run-class kafka.tools.DumpLogSegments --files \"{files}\""

FILE_PATH_REGEX = re.compile("Dumping (.*)")
INVALID_MESSAGE_REGEX = re.compile(".* isvalid: false")
INVALID_BYTES_REGEX = re.compile(".*invalid bytes")

# TODO: check dump segments output


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


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


def find_files_cmd(data_path, min):
    """Find the log files depending on their modification time.

    :param data_path: the path to the Kafka data directory
    :type data_path: string
    :param min: return only files modified in the last 'min' minutes
    :type min: integer
    """
    command = FIND_REC_COMMAND.format(data_path=data_path, min=min)
    return run(command)


def check_corrupted_files_cmd(java_home, files):
    """Check the file corruption of the specified files.

    :param java_home: the JAVA_HOME
    :type java_home: string
    :param files: list of files to be checked
    :type files: list of string
    """
    files_str = ",".join(files)
    command = CHECK_COMMAND.format(java_home=java_home, files=files_str)
    return run(command)


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


def find_files(brokers, min_minutes, verbose):
    files_by_host = {}
    hidden = [] if verbose else ['output', 'running']
    for broker_id, host in brokers:
        files = []
        with settings(forward_agent=True, connection_attempts=3, timeout=2, host_string=host), hide(*hidden):
            result = find_files_cmd(DEFAULT_DATA_PATH, min_minutes)
            print(result.return_code)
            for file_path in result.stdout.splitlines():
                files.append(file_path)
                print(file_path)
        files_by_host[host] = files
        return files_by_host   ## TODO REMOVE LIMIT


def parse_output(host, output):
    current_file = None
    for line in output.splitlines():
        file_name_search = FILE_PATH_REGEX.search(line)
        if file_name_search:
            current_file = file_name_search.group(1)
            continue
        if INVALID_MESSAGE_REGEX.match(line) or INVALID_BYTES_REGEX.match(line):
            print(
                "Host: {host}, File: {path}\n Output: {line}".format(
                    host=host,
                    path=current_file,
                    line=line,
                    )
                )


def check_files_on_host(host, files, verbose):
    hidden = [] if verbose else ['output', 'running']
    with settings(forward_agent=True, connection_attempts=3, timeout=2, host_string=host, warn_only=True), hide(*hidden):
        for i, batch in enumerate(chunks(files, DEFAULT_BATCH_SIZE)):
            result = check_corrupted_files_cmd(DEFAULT_JAVA_HOME, batch)
            print(
                "{host}: file {n_file} of {total}".format(
                    host=host,
                    n_file=(i * DEFAULT_BATCH_SIZE),
                    total=len(files),
                )
            )
            parse_output(host, result.stdout)


def check_cluster(brokers, min_minutes, verbose):
    files_by_host = find_files(brokers, min_minutes, verbose)
    for host, files in files_by_host.iteritems():
        print("Broker: {host}, {n} files to check".format(host=host, n=len(files)))
        check_files_on_host(host, files, verbose)




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
    check_files_on_host(
        "10-40-11-219-uswest1adevc",
        ["/nail/var/kafka/md1/kafka-logs/fgiraud.cli_test-1/00000000000000002367.log"],
        False,
    )
    return
#    print_brokers(cluster_config, brokers[opts.skip:])
    check_cluster(brokers, opts.min, opts.verbose)
