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
from kafka_utils.util.metadata import get_topic_partition_metadata


class AllIsrCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'all_isr',
            description='Check that all configured replicas are in sync for each topic in the cluster.',
            help='This command will check if the number of in sync replicas equals the number '
                 'configured replicas for each topic-partition in the cluster.',
        )
        return subparser

    def run_command(self):
        """All_isr command, checks if number of isr is equal to
        the configured replicas for each topic-partition."""
        topics = get_topic_partition_metadata(self.cluster_config.broker_list)
        not_in_sync = _process_metadata_response(
            topics,
        )

        errcode = status_code.OK if not not_in_sync else status_code.CRITICAL
        out = _prepare_output(not_in_sync, self.args.verbose)
        return errcode, out


def _process_metadata_response(topics):
    """Returns partitions that are not in sync."""
    not_in_sync_partitions = []
    for topic_name, partitions in topics.items():
        for metadata in partitions.values():
            cur_isr = len(metadata.isr)
            replicas = len(metadata.replicas)
            if cur_isr < replicas:
                not_in_sync_partitions.append({
                    'isr': cur_isr,
                    'replicas': replicas,
                    'topic': metadata.topic,
                    'partition': metadata.partition,
                })

    return not_in_sync_partitions


def _prepare_output(partitions, verbose):
    """Returns dict with 'raw' and 'message' keys filled."""
    out = {}
    partitions_count = len(partitions)
    out['raw'] = {
        'not_enough_replicas_count': partitions_count,
    }

    if partitions_count == 0:
        out['message'] = 'All configured replicas in sync.'
    else:
        out['message'] = (
            "{0} partition(s) have configured replicas that "
            "are not in sync."
        ).format(partitions_count)

        if verbose:
            lines = (
                "isr={isr} is lower than replicas={replicas} for {topic}:{partition}"
                .format(
                    isr=p['isr'],
                    replicas=p['replicas'],
                    topic=p['topic'],
                    partition=p['partition'],
                )
                for p in partitions
            )
            out['verbose'] = "Partitions:\n" + "\n".join(lines)
    if verbose:
        out['raw']['partitions'] = partitions

    return out
