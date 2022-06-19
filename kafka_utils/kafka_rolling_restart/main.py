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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import itertools
import logging
import math
import sys
import time
from operator import itemgetter
from typing import Any
from prometheus_client.parser import text_string_to_metric_families

from requests.exceptions import RequestException
from requests_futures.sessions import FuturesSession
from six.moves import input
from six.moves import zip

from .task import PostStopTask
from .task import PreStopTask
from .task import TaskFailedException
from .response import PrometheusRes, JolokiaRes
from kafka_utils.util import config
from kafka_utils.util.ssh import report_stderr
from kafka_utils.util.ssh import report_stdout
from kafka_utils.util.ssh import ssh
from kafka_utils.util.utils import dynamic_import
from kafka_utils.util.zookeeper import ZK


JOLOKIA_UNDER_REPL_KEY = "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager/Value"
EXPORTER_UNDER_REPL_KEY = "kafka_server_replicamanager_underreplicatedpartitions"

DEFAULT_CHECK_INTERVAL_SECS = 10
DEFAULT_CHECK_COUNT = 12
DEFAULT_TIME_LIMIT_SECS = 600
DEFAULT_JOLOKIA_PORT = 8778
DEFAULT_JOLOKIA_PREFIX = "jolokia/"
DEFAULT_JOLOKIA_USER = None
DEFAULT_JOLOKIA_PASSWORD = None
DEFAULT_EXPORTER_PORT = 9393
DEFAULT_EXPORTER_SUFFIX = "metrics"
DEFAULT_STOP_COMMAND = "service kafka stop"
DEFAULT_START_COMMAND = "service kafka start"


class WaitTimeoutException(Exception):
    pass

class PrometheusNotReady(Exception):
    pass

def parse_opts():
    parser = argparse.ArgumentParser(
        description=('Performs a rolling restart of the specified '
                     'kafka cluster.'))
    parser.add_argument(
        '--cluster-type',
        '-t',
        required=True,
        help='cluster type, e.g. "standard"',
    )
    parser.add_argument(
        '--cluster-name',
        '-c',
        help='cluster name, e.g. "uswest1-devc" (defaults to local cluster)',
    )
    parser.add_argument(
        '--broker-ids',
        '-b',
        required=False,
        type=int,
        nargs='+',
        help='space separated broker IDs to restart (optional, will restart all Kafka brokers in cluster if not specified)',
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
        '-p',
        '--prometheus',
        help='use Prometheus metrics exporter Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '--exporter-port',
        help='the metrics exporter port on the server. Default: %(default)s',
        type=int,
        default=DEFAULT_EXPORTER_PORT,
    )
    parser.add_argument(
        '--exporter-suffix',
        help='the metrics exporter suffix. Default: %(default)s',
        type=str,
        default=DEFAULT_EXPORTER_SUFFIX,
    )
    parser.add_argument(
        '--jolokia-port',
        help='the jolokia port on the server. Default: %(default)s',
        type=int,
        default=DEFAULT_JOLOKIA_PORT,
        dest='metrics_port'
    )
    parser.add_argument(
        '--jolokia-prefix',
        help='the jolokia HTTP prefix. Default: %(default)s',
        type=str,
        default=DEFAULT_JOLOKIA_PREFIX,
        dest='metrics_prefix'
    )
    parser.add_argument(
        '--jolokia-user',
        help='the jolokia username. Default: %(default)s',
        type=str,
        default=DEFAULT_JOLOKIA_USER,
    )
    parser.add_argument(
        '--jolokia-password',
        help='the jolokia password. Default: %(default)s',
        type=str,
        default=DEFAULT_JOLOKIA_PASSWORD,
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
        '--active-controller-for-last',
        help=('leave the active controller for last. Default: %(default)s'),
        action='store_true'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '--task',
        type=str,
        action='append',
        help='Module containing an implementation of Task.'
        'The module should be specified as path_to_include_to_py_path. '
        'ex. --task kafka_utils.kafka_rolling_restart.version_precheck'
    )
    parser.add_argument(
        '--task-args',
        type=str,
        action='append',
        help='Arguements which are needed by the task(prestoptask or poststoptask).'
    )
    parser.add_argument(
        '--start-command',
        type=str,
        help=('Override start command for kafka (do not include sudo)'
              'Default: %(default)s'),
        default=DEFAULT_START_COMMAND,
    )
    parser.add_argument(
        '--stop-command',
        type=str,
        help=('Override stop command for kafka (do not include sudo)'
              'Default: %(default)s'),
        default=DEFAULT_STOP_COMMAND,
    )
    parser.add_argument(
        '--ssh-password',
        type=str,
        help=('SSH passowrd to use if needed'),
    )
    parser.add_argument(
        '--ssh-username',
        type=str,
        help=('SSH username to use if needed'),
    )
    return parser.parse_args()


def get_broker_list(cluster_config, active_controller_for_last=False):
    """Returns a dictionary of brokers in the form {id: host}

    :param cluster_config: the configuration of the cluster
    :type cluster_config: map
    :param active_controller_for_last: end with active controller
    :type active_controller_for_last: bool
    """
    with ZK(cluster_config) as zk:
        brokers_dict = zk.get_brokers()
        if active_controller_for_last:
            active_controller_id = zk.get_json('/controller').get('brokerid')
            active_controller = brokers_dict.pop(active_controller_id, None)
            if active_controller is None:
                print("ERROR: Unable to retrieve the active controller information")
                sys.exit(1)
        brokers = sorted(list(brokers_dict.items()), key=itemgetter(0))
        if active_controller_for_last:
            brokers.append((active_controller_id, active_controller))
        return [(id, data['host']) for id, data in brokers]


def filter_broker_list(brokers, filter_by):
    """Returns sorted list, a subset of elements from brokers in the form [(id, host)].
    Passing empty list for filter_by will return empty list.

    :param brokers: list of brokers to filter, assumes the data is in so`rted order
    :type brokers: list of (id, host)
    :param filter_by: the list of ids of brokers to keep
    :type filter_by: list of integers
    """
    filter_by_set = set(filter_by)
    return [(id, host) for id, host in brokers if id in filter_by_set]

def prometheus_requests_parse(metrics_output, metric_name):
  metrics = list(text_string_to_metric_families(metrics_output))
  target_metric = list(filter(lambda x: x.name == metric_name, metrics))
  assert len(target_metric) == 1
  return target_metric[0]

def prometheus_requests(hosts, metrics_port, metrics_prefix):
    """Use Prometheus to fetch the under replicated partition number from the specified hosts.
    
    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param metrics_port: HTTP port for Prometheus
    :type metrics_port: integer
    :param metrics_prefix: Endpoint suffix for the Prometheus request
    :type metrics_prefix: string
    """
    session = FuturesSession()
    for host in hosts:
        url = "http://{host}:{port}/{prefix}".format(
            host=host,
            port=metrics_port,
            prefix=metrics_prefix
        )
        try:
            s = session.get(url).result()
        except Exception as exception_type:
            yield host, PrometheusRes(400,Any, exception_type)
        if s.status_code != 200:
            yield host, PrometheusRes(400,Any, exception_type)
        else:
            int_value = 0
            try:
                match = prometheus_requests_parse(s.text, EXPORTER_UNDER_REPL_KEY)
                int_value = int(match.samples[0].value)
            except AssertionError:
                print("Prometheus is not ready, or unable to find key: {}".format(EXPORTER_UNDER_REPL_KEY))
                yield host, PrometheusRes(400, Any, PrometheusNotReady)
            except Exception as e:
                print("Prometheus is not ready.\n{e}".format(e))
                yield host, PrometheusRes(400, Any, PrometheusNotReady)
            yield host, PrometheusRes(s.status_code, int_value)

def jolokia_requests(hosts, metrics_port, metrics_prefix, jolokia_user, jolokia_password):
    """Use Jolokita to fetch the under replicated partition number from the specified hosts.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param metrics_port: HTTP port for Jolokia
    :type metrics_port: integer
    :param metrics_prefix: HTTP prefix on the server for the Jolokia queries
    :type metrics_prefix: string
    :param jolokia_user: Username for Jolokia, if needed
    :type jolokia_user: string
    :param jolokia_password: Password for Jolokia, if needed
    :type jolokia_password: string
    :returns: generator of requests
    """
    session = FuturesSession()
    if jolokia_user and jolokia_password:
        session.auth = (jolokia_user, jolokia_password)
    for host in hosts:
        url = "http://{host}:{port}/{prefix}/read/{key}".format(
            host=host,
            port=metrics_port,
            prefix=metrics_prefix,
            key=JOLOKIA_UNDER_REPL_KEY,
        )
        try:
            s = session.get(url).result()
        except Exception as exception_type:
            yield host, JolokiaRes(s.status_code, s.json(), exception_type)
        yield host, JolokiaRes(s.status_code, s.json())

def generate_requests(hosts, metrics_port, metrics_prefix, jolokia_user, jolokia_password, is_prometheus):
    """By default, use Jolokia to fetch under replicated metrics.
    If is_prometheus bool set to true, use Prometheus exporter to fetch it.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param metrics_port: HTTP port for Jolokia
    :type metrics_port: integer
    :param metrics_prefix: HTTP prefix on the server for the Jolokia queries
    :type metrics_prefix: string
    :param jolokia_user: Username for Jolokia, if needed
    :type jolokia_user: string
    :param jolokia_password: Password for Jolokia, if needed
    :type jolokia_password: string
    :returns: generator of requests
    :param is_prometheus: Use Prometheus over Jolokia
    :type is_prometheus: bool
    """
    if is_prometheus:
        return prometheus_requests(hosts, metrics_port, metrics_prefix)
    else:
        return jolokia_requests(hosts, metrics_port, metrics_prefix, jolokia_user, jolokia_password)


def read_cluster_status(hosts, metrics_port, metrics_prefix, jolokia_user, jolokia_password, is_prometheus):
    """Read and return the number of under replicated partitions and
    missing brokers from the specified hosts.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param metrics_port: HTTP port for Jolokia
    :type metrics_port: integer
    :param metrics_prefix: HTTP prefix on the server for the Jolokia queries
    :type metrics_prefix: string
    :returns: tuple of integers
    :param jolokia_user: Username for Jolokia, if needed
    :type jolokia_user: string
    :param jolokia_password: Password for Jolokia, if needed
    :type jolokia_password: string
    :param is_prometheus: Use Prometheus over Jolokia
    :type is_prometheus: bool
    """
    under_replicated = 0
    missing_brokers = 0
    for host, response in generate_requests(hosts, metrics_port, metrics_prefix, jolokia_user, jolokia_password, is_prometheus):
        try:
            if response.exception != None:
                raise response.exception
            if response.status_code == 401:
                print("Jolokia Authentication Failed. Exiting.")
                sys.exit(1)
            if 400 <= response.status_code <= 599:
                print("Got status code {0}. Exiting.".format(response.status_code))
                sys.exit(1)
            under_replicated += response.json if is_prometheus else response.json['value']
        except RequestException as e:
            print("Broker {0} is down: {1}."
                  "This maybe because it is starting up".format(host, e), file=sys.stderr)
            missing_brokers += 1
        except KeyError:
            print("Cannot find the key, Kafka is probably still starting up", file=sys.stderr)
            missing_brokers += 1
        except PrometheusNotReady:
            print("Prometheus is not yet ready and probably still starting up", file=sys.stderr)
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
        choice = input().lower()
        if choice in ['yes', 'y']:
            return True
        elif choice in ['no', 'n']:
            return False
        else:
            print("Please respond with 'yes' or 'no'")


def start_broker(host, connection, start_command, verbose):
    """Execute the start"""
    _, stdout, stderr = connection.sudo_command(start_command)
    if verbose:
        report_stdout(host, stdout)
        report_stderr(host, stderr)


def stop_broker(host, connection, stop_command, verbose):
    """Execute the stop"""
    _, stdout, stderr = connection.sudo_command(stop_command)
    if verbose:
        report_stdout(host, stdout)
        report_stderr(host, stderr)


def wait_for_stable_cluster(
    hosts,
    metrics_port,
    metrics_prefix,
    jolokia_user,
    jolokia_password,
    check_interval,
    check_count,
    unhealthy_time_limit,
    is_prometheus,
):
    """
    Block the caller until the cluster can be considered stable.

    :param hosts: list of brokers ip addresses
    :type hosts: list of strings
    :param metrics_port: HTTP port for Jolokia or Prometheus
    :type metrics_port: integer
    :param metrics_prefix: HTTP prefix on the server for the Jolokia or Prometheus queries
    :type metrics_prefix: string
    :param jolokia_user: Username for Jolokia, if needed
    :type jolokia_user: string
    :param jolokia_password: Password for Jolokia, if needed
    :type jolokia_password: string
    :param check_interval: the number of seconds it will wait between each check
    :type check_interval: integer
    :param check_count: the number of times the check should be positive before
    restarting the next broker
    :type check_count: integer
    :param unhealthy_time_limit: the maximum number of seconds it will wait for
    the cluster to become stable before exiting with error
    :type unhealthy_time_limit: integer
    :param is_prometheus: Use Prometheus over Jolokia
    :type is_prometheus: bool
    """
    stable_counter = 0
    max_checks = int(math.ceil(unhealthy_time_limit / check_interval))
    for i in itertools.count():
        partitions, brokers = read_cluster_status(
            hosts,
            metrics_port,
            metrics_prefix,
            jolokia_user,
            jolokia_password,
            is_prometheus
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


def execute_task(tasks, host):
    """Execute all the prechecks for the host.
    Expected to raise a TaskFailedException() in case of failing to execute a task.
    """
    for t in tasks:
        t.run(host)


def execute_rolling_restart(
    brokers,
    metrics_port,
    metrics_prefix,
    jolokia_user,
    jolokia_password,
    check_interval,
    check_count,
    unhealthy_time_limit,
    skip,
    verbose,
    pre_stop_task,
    post_stop_task,
    start_command,
    stop_command,
    is_prometheus,
    ssh_password=None,
    ssh_username=None
):
    """Execute the rolling restart on the specified brokers. It checks the
    number of under replicated partitions on each broker, using Jolokia.

    The check is performed at constant intervals, and a broker will be restarted
    when all the brokers are answering and are reporting zero under replicated
    partitions.

    :param brokers: the brokers that will be restarted
    :type brokers: map of broker ids and host names
    :param metrics_port: HTTP port for Jolokia or Prometheus
    :type metrics_port: integer
    :param metrics_prefix: HTTP prefix on the server for the Jolokia or Prometheus queries
    :type metrics_prefix: string
    :param jolokia_user: Username for Jolokia, if needed
    :type jolokia_user: string
    :param jolokia_password: Password for Jolokia, if needed
    :type jolokia_password: string
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
    :param pre_stop_task: a list of tasks to execute before running stop
    :type pre_stop_task: list
    :param post_stop_task: a list of task to execute after running stop
    :type post_stop_task: list
    :param start_command: the start command for kafka
    :type start_command: string
    :param stop_command: the stop command for kafka
    :type stop_command: string
    :param is_prometheus: Use Prometheus over Jolokia
    :type is_prometheus: bool
    :param ssh_password: The ssh password to use if needed
    :type ssh_password: string
    :param ssh_username: The ssh username to use if needed
    :type ssh_username: string
    """
    all_hosts = [b[1] for b in brokers]
    for n, host in enumerate(all_hosts[skip:]):
        with ssh(host=host, forward_agent=True, sudoable=True, max_attempts=3, max_timeout=2,
                 ssh_password=ssh_password, ssh_username=ssh_username) as connection:
            execute_task(pre_stop_task, host)
            wait_for_stable_cluster(
                all_hosts,
                metrics_port,
                metrics_prefix,
                jolokia_user,
                jolokia_password,
                check_interval,
                1 if n == 0 else check_count,
                unhealthy_time_limit,
                is_prometheus,
            )
            print("Stopping {0} ({1}/{2})".format(host, n + 1, len(all_hosts) - skip))
            stop_broker(host, connection, stop_command, verbose)
            execute_task(post_stop_task, host)
        # we open a new SSH connection in case the hostname has a new IP
        with ssh(host=host, forward_agent=True, sudoable=True, max_attempts=3, max_timeout=2,
                 ssh_password=ssh_password, ssh_username=ssh_username) as connection:
            print("Starting {0} ({1}/{2})".format(host, n + 1, len(all_hosts) - skip))
            start_broker(host, connection, start_command, verbose)
    # Wait before terminating the script
    wait_for_stable_cluster(
        all_hosts,
        metrics_port,
        metrics_prefix,
        jolokia_user,
        jolokia_password,
        check_interval,
        check_count,
        unhealthy_time_limit,
        is_prometheus,
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


def validate_broker_ids_subset(broker_ids, subset_ids):
    """Validate that user specified broker ids to restart exist in the broker ids retrieved
    from cluster config.

    :param broker_ids: all broker IDs in a cluster
    :type broker_ids: list of integers
    :param subset_ids: broker IDs specified by user
    :type subset_ids: list of integers
    :returns: bool
    """
    all_ids = set(broker_ids)
    valid = True
    for subset_id in subset_ids:
        valid = valid and subset_id in all_ids
        if subset_id not in all_ids:
            print("Error: user specified broker id {0} does not exist in cluster.".format(subset_id))
    return valid


def get_task_class(tasks, task_args):
    """Reads in a list of tasks provided by the user,
    loads the appropiate task, and returns two lists,
    pre_stop_tasks and post_stop_tasks
    :param tasks: list of strings locating tasks to load
    :type tasks: list
    :param task_args: list of strings to be used as args
    :type task_args: list
    """
    pre_stop_tasks = []
    post_stop_tasks = []
    task_to_task_args = dict(list(zip(tasks, task_args)))
    tasks_classes = [PreStopTask, PostStopTask]

    for func, task_args in task_to_task_args.items():
        for task_class in tasks_classes:
            imported_class = dynamic_import(func, task_class)
            if imported_class:
                if task_class is PreStopTask:
                    pre_stop_tasks.append(imported_class(task_args))
                elif task_class is PostStopTask:
                    post_stop_tasks.append(imported_class(task_args))
                else:
                    print("ERROR: Class is not a type of Pre/Post StopTask:" + func)
                    sys.exit(1)
    return pre_stop_tasks, post_stop_tasks


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
    brokers = get_broker_list(cluster_config, opts.active_controller_for_last)
    if opts.broker_ids:
        if not validate_broker_ids_subset([id for id, host in brokers], opts.broker_ids):
            sys.exit(1)
        brokers = filter_broker_list(brokers, opts.broker_ids)
    if validate_opts(opts, len(brokers)):
        sys.exit(1)
    pre_stop_tasks = []
    post_stop_tasks = []
    if opts.prometheus:
        print("Using Prometheus metrics exporter")
        opts.metrics_port, opts.metrics_prefix = DEFAULT_EXPORTER_PORT, DEFAULT_EXPORTER_SUFFIX
    if opts.task:
        pre_stop_tasks, post_stop_tasks = get_task_class(opts.task, opts.task_args)
    print_brokers(cluster_config, brokers[opts.skip:])
    if opts.no_confirm or ask_confirmation():
        print("Execute restart")
        try:
            execute_rolling_restart(
                brokers,
                opts.metrics_port,
                opts.metrics_prefix,
                opts.jolokia_user,
                opts.jolokia_password,
                opts.check_interval,
                opts.check_count,
                opts.unhealthy_time_limit,
                opts.skip,
                opts.verbose,
                pre_stop_tasks,
                post_stop_tasks,
                opts.start_command,
                opts.stop_command,
                opts.prometheus,
                opts.ssh_password,
                opts.ssh_username,
            )
        except TaskFailedException:
            print("ERROR: pre/post tasks failed, exiting")
            sys.exit(1)
        except WaitTimeoutException:
            print("ERROR: cluster is still unhealthy, exiting")
            sys.exit(1)
