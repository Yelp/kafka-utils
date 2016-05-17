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
from fabric.api import sudo
from fabric.api import task
from requests.exceptions import RequestException
from requests_futures.sessions import FuturesSession

from kafka_utils.util import config
from kafka_utils.util.zookeeper import ZK


RESTART_COMMAND = "service kafka restart"

UNDER_REPL_KEY = "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager/Value"

DEFAULT_CHECK_INTERVAL_SECS = 10
DEFAULT_CHECK_COUNT = 12
DEFAULT_TIME_LIMIT_SECS = 600
DEFAULT_JOLOKIA_PORT = 8778
DEFAULT_JOLOKIA_PREFIX = "jolokia/"


class WaitTimeoutException(Exception):
    pass


def parse_opts():
    parser = argparse.ArgumentParser(
        description=('Performs a rolling restart of the specified '
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
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        '--check-interval',
        help=('the interval between each check, in seconds. '
              'Default: %(default)s seconds'),
        type=int,
        default=DEFAULT_CHECK_INTERVAL_SECS,
    )
    parser.add_argument(
        '--check-count',
        help=('the minimum number of times the cluster should result stable '
              'before restarting the next broker. Default: %(default)s'),
        type=int,
        default=DEFAULT_CHECK_COUNT,
    )
    parser.add_argument(
        '--unhealthy-time-limit',
        help=('the maximum amount of time the cluster can be unhealthy before '
              'stopping the rolling restart. Default: %(default)s'),
        type=int,
        default=DEFAULT_TIME_LIMIT_SECS,
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
        type=str,
        default=DEFAULT_JOLOKIA_PREFIX,
    )
    parser.add_argument(
        '--no-confirm',
        help='proceed without asking confirmation. Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '--skip',
        help=('the number of brokers to skip without restarting. Brokers are '
              'restarted in increasing broker-id order. Default: %(default)s'),
        type=int,
        default=0,
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    return parser.parse_args()


def get_broker_list(cluster_config):
    """Returns a dictionary of brokers in the form {id: host}

    :param cluster_config: the configuration of the cluster
    :type cluster_config: map
    """
    with ZK(cluster_config) as zk:
        brokers = sorted(zk.get_brokers().items(), key=itemgetter(0))
        return [(id, data['host']) for id, data in brokers]


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
            response = request.result()
            if 400 <= response.status_code <= 599:
                print("Got status code {0}. Exiting.".format(response.status_code))
                sys.exit(1)
            json = response.json()
            under_replicated += json['value']
        except RequestException as e:
            print("Broker {0} is down: {1}".format(host, e), file=sys.stderr)
            missing_brokers += 1
        except KeyError:
            print("Cannot find the key, Kafka is probably still starting up", file=sys.stderr)
            missing_brokers += 1
    return under_replicated, missing_brokers


def print_brokers(cluster_config, brokers):
    """Print the list of brokers that will be restarted.

    :param cluster_config: the cluster configuration
    :type cluster_config: map
    :param brokers: the brokers that will be restarted
    :type brokers: map of broker ids and host names
    """
    print("Will restart the following brokers in {0}:".format(cluster_config.name))
    for id, host in brokers:
        print("  {0}: {1}".format(id, host))


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


def wait_for_stable_cluster(
    hosts,
    jolokia_port,
    jolokia_prefix,
    check_interval,
    check_count,
    unhealthy_time_limit,
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
    :param unhealthy_time_limit: the maximum number of seconds it will wait for
    the cluster to become stable before exiting with error
    :type unhealthy_time_limit: integer
    """
    stable_counter = 0
    max_checks = int(math.ceil(unhealthy_time_limit / check_interval))
    for i in itertools.count():
        partitions, brokers = read_cluster_status(
            hosts,
            jolokia_port,
            jolokia_prefix,
        )
        if partitions or brokers:
            stable_counter = 0
        else:
            stable_counter += 1
        print(
            "Under replicated partitions: {p_count}, missing brokers: {b_count} ({stable}/{limit})".format(
                p_count=partitions,
                b_count=brokers,
                stable=stable_counter,
                limit=check_count,
            ))
        if stable_counter >= check_count:
            print("The cluster is stable")
            return
        if i >= max_checks:
            raise WaitTimeoutException()
        time.sleep(check_interval)


def execute_rolling_restart(
    brokers,
    jolokia_port,
    jolokia_prefix,
    check_interval,
    check_count,
    unhealthy_time_limit,
    skip,
    verbose,
):
    """Execute the rolling restart on the specified brokers. It checks the
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
    :param unhealthy_time_limit: the maximum number of seconds it will wait for
    the cluster to become stable before exiting with error
    :type unhealthy_time_limit: integer
    :param skip: the number of brokers to skip
    :type skip: integer
    :param verbose: print commend execution information
    :type verbose: bool
    """
    all_hosts = [b[1] for b in brokers]
    hidden = [] if verbose else ['output', 'running']
    for n, host in enumerate(all_hosts[skip:]):
        with settings(forward_agent=True, connection_attempts=3, timeout=2), hide(*hidden):
            wait_for_stable_cluster(
                all_hosts,
                jolokia_port,
                jolokia_prefix,
                check_interval,
                1 if n == 0 else check_count,
                unhealthy_time_limit,
            )
            print("Restarting {0} ({1}/{2})".format(host, n, len(all_hosts) - skip))
            execute(restart_broker, hosts=host)
    # Wait before terminating the script
    wait_for_stable_cluster(
        all_hosts,
        jolokia_port,
        jolokia_prefix,
        check_interval,
        check_count,
        unhealthy_time_limit,
    )


def validate_opts(opts, brokers_num):
    """Basic option validation. Returns True if the options are not valid,
    False otherwise.

    :param opts: the command line options
    :type opts: map
    :param brokers_num: the number of brokers
    :type brokers_num: integer
    :returns: bool
    """
    if opts.skip < 0 or opts.skip >= brokers_num:
        print("Error: --skip must be >= 0 and < #brokers")
        return True
    if opts.check_count < 0:
        print("Error: --check-count must be >= 0")
        return True
    if opts.unhealthy_time_limit < 0:
        print("Error: --unhealthy-time-limit must be >= 0")
        return True
    if opts.check_count == 0:
        print("Warning: no check will be performed")
    if opts.check_interval < 0:
        print("Error: --check-interval must be >= 0")
        return True
    return False


def run():
    opts = parse_opts()
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)
    cluster_config = config.get_cluster_config(
        opts.cluster_type,
        opts.cluster_name,
        opts.discovery_base_path,
    )
    brokers = get_broker_list(cluster_config)
    if validate_opts(opts, len(brokers)):
        sys.exit(1)
    print_brokers(cluster_config, brokers[opts.skip:])
    if opts.no_confirm or ask_confirmation():
        print("Execute restart")
        try:
            execute_rolling_restart(
                brokers,
                opts.jolokia_port,
                opts.jolokia_prefix,
                opts.check_interval,
                opts.check_count,
                opts.unhealthy_time_limit,
                opts.skip,
                opts.verbose,
            )
        except WaitTimeoutException:
            print("ERROR: cluster is still unhealthy, exiting")
            sys.exit(1)
