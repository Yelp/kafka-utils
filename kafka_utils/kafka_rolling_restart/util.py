# -*- coding: utf-8 -*-
# Copyright 2019 Yelp Inc.
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

import itertools
import math
import sys
import time
from operator import itemgetter

from requests.exceptions import RequestException
from requests_futures.sessions import FuturesSession

from .error import WaitTimeoutException
from kafka_utils.util.zookeeper import ZK


UNDER_REPL_KEY = "kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager/Value"


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


def execute_task(tasks, host):
    """Execute all the prechecks for the host.
    Excepted to raise a TaskFailedException() in case of failing to execute a task.
    """
    for t in tasks:
        t.run(host)


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
            print("Broker {0} is down: {1}."
                  "This maybe because it is starting up".format(host, e), file=sys.stderr)
            missing_brokers += 1
        except KeyError:
            print("Cannot find the key, Kafka is probably still starting up", file=sys.stderr)
            missing_brokers += 1
    return under_replicated, missing_brokers


def get_broker_list(cluster_config):
    """Returns a list of brokers in the form [(id: host)]

    :param cluster_config: the configuration of the cluster
    :type cluster_config: map
    """
    with ZK(cluster_config) as zk:
        brokers = sorted(list(zk.get_brokers().items()), key=itemgetter(0))
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
