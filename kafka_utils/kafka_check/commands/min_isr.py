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
from __future__ import print_function

from kazoo.exceptions import NoNodeError

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_metadata


class MinIsrCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'min_isr',
            description='Check min isr number for each topic in the cluster.',
            help='This command will check actual number of insync replicas for each '
                 'topic-partition in the cluster with configuration for that topic '
                 'in Zookeeper or default min.isr param if it is specified and there '
                 'is no settings in Zookeeper for partition.',
        )
        subparser.add_argument(
            '--default-min-isr',
            type=int,
            default=1,
            help='Default min.isr value for cases without settings in Zookeeper '
            'for some topics. Default: %(default)s',
        )
        return subparser

    def run_command(self):
        """Min_isr command, checks number of actual min-isr
        for each topic-partition with configuration for that topic."""
        topics = get_topic_partition_metadata(self.cluster_config.broker_list)
        not_in_sync = process_metadata_response(
            topics,
            self.zk,
            self.args.default_min_isr,
            self.args.verbose,
        )

        if not_in_sync == 0:
            return status_code.OK, "All replicas in sync."
        else:
            msg = ("{0} partition(s) have the number of replicas in "
                   "sync that is lower than the specified min ISR.").format(not_in_sync)
            return status_code.CRITICAL, msg


def get_min_isr(zk, topic):
    """Return the min-isr for topic, or None if not specified"""
    ISR_CONF_NAME = 'min.insync.replicas'
    try:
        config = zk.get_topic_config(topic)
    except NoNodeError:
        return None
    if ISR_CONF_NAME in config['config']:
        return int(config['config'][ISR_CONF_NAME])
    else:
        return None


def process_metadata_response(topics, zk, default_min_isr, verbose):
    not_in_sync = 0
    for topic_name, partitions in topics.items():
        min_isr = get_min_isr(zk, topic_name) or default_min_isr
        if min_isr is None:
            continue
        for metadata in partitions.values():
            cur_isr = len(metadata.isr)
            if cur_isr < min_isr:
                if verbose:
                    print("isr={isr} is lower than min_isr={min_isr} for {topic}:{partition}".format(
                        isr=cur_isr,
                        min_isr=min_isr,
                        topic=metadata.topic,
                        partition=metadata.partition,
                    ))
                not_in_sync += 1

    return not_in_sync
