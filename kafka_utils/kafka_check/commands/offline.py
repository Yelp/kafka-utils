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

import sys

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_with_error
from kafka_utils.util.metadata import LEADER_NOT_AVAILABLE_ERROR


class OfflineCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'offline',
            description='Check offline partitions on the specified broker',
            help='This subcommand will fail if there are any offline partitions '
            'in the cluster.'
        )

        return subparser

    def run_command(self):
        """Checks the number of offline partitions"""
        offline = get_topic_partition_with_error(
            self.cluster_config,
            LEADER_NOT_AVAILABLE_ERROR,
        )

        errcode = status_code.OK if not offline else status_code.CRITICAL
        out = _prepare_output(offline, self.args.verbose)
        return errcode, out


def _prepare_output(partitions, verbose):
    """Returns dict with 'raw' and 'message' keys filled."""
    out = {}
    partitions_count = len(partitions)
    out['raw'] = {
        'offline_count': partitions_count,
    }

    if partitions_count == 0:
        out['message'] = 'No offline partitions.'
    else:
        out['message'] = "{count} offline partitions.".format(count=partitions_count)
        if verbose:
            lines = (
                '{}:{}'.format(topic, partition)
                for (topic, partition) in partitions
            )
            out['verbose'] = "Partitions:\n" + "\n".join(lines)
        else:
            cmdline = sys.argv[:]
            cmdline.insert(1, '-v')
            out['message'] += '\nTo see all offline partitions run: ' + ' '.join(cmdline)

    if verbose:
        out['raw']['partitions'] = [
            {'topic': topic, 'partition': partition}
            for (topic, partition) in partitions
        ]

    return out
