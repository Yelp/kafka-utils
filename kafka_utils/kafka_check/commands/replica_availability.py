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

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_with_error
from kafka_utils.util.metadata import REPLICA_NOT_AVAILABLE_ERROR


class ReplicaAvailabilityCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'replica_availability',
            description='Check availability of replicas for all brokers in cluster.',
            help='This command will fail if there are any replicas which are not'
                 ' available for communication in the cluster.',
        )
        return subparser

    def run_command(self):
        """Replica_availability command, checks number of replicas not available
        for communication over all brokers in the Kafka cluster."""
        replica_availability = get_topic_partition_with_error(
            self.cluster_config,
            REPLICA_NOT_AVAILABLE_ERROR,
        )

        errcode = status_code.OK if not replica_availability else status_code.CRITICAL
        out = _prepare_output(replica_availability, self.args.verbose)
        return errcode, out


def _prepare_output(partitions, verbose):
    """Returns dict with 'raw' and 'message' keys filled."""
    partitions_count = len(partitions)
    out = {}
    out['raw'] = {
        'replica_availability_count': partitions_count,
    }

    if partitions_count == 0:
        out['message'] = 'All replicas available for communication.'
    else:
        out['message'] = "{replica_availability} replicas not available for communication.".format(
            replica_availability=partitions_count,
        )
        if verbose:
            lines = (
                '{}:{}'.format(topic, partition)
                for (topic, partition) in partitions
            )
            out['verbose'] = "Partitions:\n" + "\n".join(lines)

    if verbose:
        out['raw']['partitions'] = [
            {'topic': topic, 'partition': partition}
            for (topic, partition) in partitions
        ]

    return out
