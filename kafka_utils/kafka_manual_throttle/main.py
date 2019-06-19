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
import logging
import sys

from kafka_utils.util import config
from kafka_utils.util.zookeeper import ZK


LEADER_THROTTLE_RATE_CONFIGURATION = "leader.replication.throttled.rate"
FOLLOWER_THROTTLE_RATE_CONFIGURATION = "follower.replication.throttled.rate"


def parse_opts():
    parser = argparse.ArgumentParser(
        description='Manually applies throttle values for brokers in a Kafka cluster',
    )
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
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        '--leader-throttle',
        type=int,
        required=False,
        help='value (B/s) of the leader throttle to apply for brokers',
    )
    parser.add_argument(
        '--follower-throttle',
        type=int,
        required=False,
        help='value (B/s) of the leader throttle to apply for brokers',
    )
    parser.add_argument(
        '--clear',
        help='if set, throttles will be cleared instead of being set',
        action='store_true',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    return parser.parse_args()


def validate_opts(opts, brokers_num):
    """
    Basic option validation.

    True if the options are valid, False otherwise.

    :opts : the command line options
    :returns : bool
    """
    if opts.clear:
        if opts.leader_throttle is not None:
            print("Error: --clear cannot be used with --leader-throttle")
            return False
        if opts.follower_throttle is not None:
            print("Error: --clear cannot be used with --follower-throttle")
            return False
    else:
        if opts.leader_throttle < 0:
            print("Error: --leader-throttle must be >= 0")
            return False
        if opts.follower_throttle < 0:
            print("Error: --follower-throttle must be >= 0")
            return False
    return True


def apply_throttles(zk, brokers, leader_throttle, follower_throttle):
    """
    Apply new leader/follower replication throttles to the given brokers

    :zk : a ZK client
    :brokers : a collection of broker ids
    :leader_throttle : new leader replication throttle (in B/s)
    :follower_throttle : new follower replication throttle (in B/s)
    """
    for broker_id in brokers:
        write_throttle(zk, broker_id, str(leader_throttle), str(follower_throttle))


def clear_throttles(zk, brokers):
    """
    Clear replication throttles for the given brokers.

    :zk : a ZK client
    :brokers : a collection of broker ids
    """
    for broker_id in brokers:
        write_throttle(zk, broker_id, None, None)


def write_throttle(zk, broker_id, leader_throttle, follower_throttle):
    """
    Write leader/follower replication throttle rates for a given broker.

    If a given replication throttle is None, the configuration will be cleared
    from the broker instead of applying a new one.

    More details can be found in:
        https://kafka.apache.org/documentation/#rep-throttle

    :zk : a ZK client
    :broker_id : broker to change replication throttles for
    :leader_throttle : new leader replication throttle (in B/s) or None
    :follower_throttle : new follower replication throttle (in B/s) or None
    """
    config = zk.get_broker_config(broker_id)

    if leader_throttle is not None:
        config[LEADER_THROTTLE_RATE_CONFIGURATION] = leader_throttle
    else:
        config.pop(LEADER_THROTTLE_RATE_CONFIGURATION, None)

    if follower_throttle is not None:
        config[FOLLOWER_THROTTLE_RATE_CONFIGURATION] = follower_throttle
    else:
        config.pop(FOLLOWER_THROTTLE_RATE_CONFIGURATION, None)

    zk.set_broker_config(broker_id, config)


def run():
    opts = parse_opts()
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    if not validate_opts(opts):
        sys.exit(1)

    cluster_config = config.get_cluster_config(
        opts.cluster_type,
        opts.cluster_name,
        opts.discovery_base_path,
    )

    print("Applying throttles")

    with ZK(cluster_config) as zk:
        brokers = zk.get_brokers(names_only=True)

        if not opts.clear:
            apply_throttles(
                zk,
                brokers,
                opts.leader_throttle,
                opts.follower_throttle,
                opts.verbose,
            )
        else:
            clear_throttles(zk, brokers, opts.verbose)

    print("Throttles applied.")
    print("NOTE: Do not forget to --clear throttles once the reassignment plan completes.")
