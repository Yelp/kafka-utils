# Copyright 2018 Yelp Inc.
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
from typing_extensions import TypedDict

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.kafka_check.commands.min_isr import get_min_isr
from kafka_utils.util.metadata import get_topic_partition_metadata
from kafka_utils.util.zookeeper import ZK


class ReplicationFactorCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers: Any) -> Any:
        subparser = subparsers.add_parser(
            'replication_factor',
            description='Check replication factor settings for each topic in the cluster.',
            help='This command will check replication factor each topic in the cluster '
                 'and compare it with min.isr settings in Zookeeper or default min.isr param '
                 'if it is specified and there is no settings in Zookeeper for a topic.',
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
        """Replication factor command, checks replication factor settings and compare it with
        min.isr in the cluster."""
        topics = get_topic_partition_metadata(self.cluster_config.broker_list)

        topics_with_wrong_rf = _find_topics_with_wrong_rp(
            topics,
            self.zk,
            self.args.default_min_isr,
        )

        errcode = status_code.OK if not topics_with_wrong_rf else status_code.CRITICAL
        out = _prepare_output(topics_with_wrong_rf, self.args.verbose, self.args.head)
        return errcode, out


class TopicDict(TypedDict):
    replication_factor: int
    min_isr: int
    topic: str


def _find_topics_with_wrong_rp(topics: dict[str, dict[int, PartitionMetadata]], zk: ZK, default_min_isr: int) -> list[TopicDict]:
    """Returns topics with wrong replication factor."""
    topics_with_wrong_rf: list[TopicDict] = []

    for topic_name, partitions in topics.items():
        min_isr = get_min_isr(zk, topic_name) or default_min_isr
        replication_factor = len(partitions[0].replicas)

        if replication_factor >= min_isr + 1:
            continue

        topics_with_wrong_rf.append({
            'replication_factor': replication_factor,
            'min_isr': min_isr,
            'topic': topic_name,
        })

    return topics_with_wrong_rf


def _prepare_output(topics_with_wrong_rf: list[TopicDict], verbose: bool, head_limit: int) -> dict[str, Any]:
    """Returns dict with 'raw' and 'message' keys filled."""
    out: dict[str, Any] = {}
    topics_count = len(topics_with_wrong_rf)
    out['raw'] = {
        'topics_with_wrong_replication_factor_count': topics_count,
    }
    if head_limit != -1:
        topics_with_wrong_rf = list(itertools.islice(topics_with_wrong_rf, head_limit))

    if topics_count == 0:
        out['message'] = 'All topics have proper replication factor.'
    else:
        out['message'] = (
            "{} topic(s) have replication factor lower than specified min ISR + 1."
        ).format(topics_count)

        if verbose:
            lines = (
                "replication_factor={replication_factor} is lower than min_isr={min_isr} + 1 for {topic}"
                .format(
                    min_isr=topic['min_isr'],
                    topic=topic['topic'],
                    replication_factor=topic['replication_factor'],
                )
                for topic in topics_with_wrong_rf
            )
            if head_limit != -1:
                title = f"Top {head_limit} topics:\n"
            else:
                title = "Topics:\n"

            out['verbose'] = title + "\n".join(lines)

    if verbose:
        out['raw']['topics'] = topics_with_wrong_rf

    return out
