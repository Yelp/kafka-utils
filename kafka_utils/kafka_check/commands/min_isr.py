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
from __future__ import annotations

import itertools
from typing import Any

from kafka.structs import PartitionMetadata
from kazoo.exceptions import NoNodeError
from typing_extensions import TypedDict

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_metadata
from kafka_utils.util.zookeeper import ZK


class MinIsrCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers: Any) -> Any:
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

    def run_command(self) -> tuple[int, dict[str, Any]]:
        """Min_isr command, checks number of actual min-isr
        for each topic-partition with configuration for that topic."""
        topics = get_topic_partition_metadata(self.cluster_config.broker_list)
        not_in_sync = _process_metadata_response(
            topics,
            self.zk,
            self.args.default_min_isr,
        )

        errcode = status_code.OK if not not_in_sync else status_code.CRITICAL
        out = _prepare_output(not_in_sync, self.args.verbose, self.args.head)
        return errcode, out


def get_min_isr(zk: ZK, topic: str) -> int | None:
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


class PartitionDict(TypedDict):
    isr: int
    min_isr: int
    topic: str
    partition: int


def _process_metadata_response(topics: dict[str, dict[int, PartitionMetadata]], zk: ZK, default_min_isr: int) -> list[PartitionDict]:
    """Returns not in sync partitions."""
    not_in_sync_partitions: list[PartitionDict] = []
    for topic_name, partitions in topics.items():
        min_isr = get_min_isr(zk, topic_name) or default_min_isr
        if min_isr is None:
            continue
        for metadata in partitions.values():
            cur_isr = len(metadata.isr)
            if cur_isr < min_isr:
                not_in_sync_partitions.append({
                    'isr': cur_isr,
                    'min_isr': min_isr,
                    'topic': metadata.topic,
                    'partition': metadata.partition,
                })

    return not_in_sync_partitions


def _prepare_output(partitions: list[PartitionDict], verbose: bool, head_limit: int) -> dict[str, Any]:
    """Returns dict with 'raw' and 'message' keys filled."""
    out: dict[str, Any] = {}
    partitions_count = len(partitions)
    out['raw'] = {
        'not_enough_replicas_count': partitions_count,
    }
    if head_limit != -1:
        partitions = list(itertools.islice(partitions, head_limit))

    if partitions_count == 0:
        out['message'] = 'All replicas in sync.'
    else:
        out['message'] = (
            "{} partition(s) have the number of replicas in "
            "sync that is lower than the specified min ISR."
        ).format(partitions_count)
        if verbose:
            lines = (
                "isr={isr} is lower than min_isr={min_isr} for {topic}:{partition}"
                .format(
                    isr=p['isr'],
                    min_isr=p['min_isr'],
                    topic=p['topic'],
                    partition=p['partition'],
                )
                for p in partitions
            )
            title = f"Top {head_limit} partitions:\n" + "\n".join(lines) if head_limit != -1 else "Partitions:\n"

            out['verbose'] = title + "\n".join(lines)
    if verbose:
        out['raw']['partitions'] = partitions

    return out
