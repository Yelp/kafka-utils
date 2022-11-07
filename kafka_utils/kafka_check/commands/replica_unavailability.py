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
from collections.abc import Collection
from typing import Any
from typing import cast
from typing import Set
from typing import Tuple

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.command import KafkaCheckCmd
from kafka_utils.util.metadata import get_topic_partition_with_error
from kafka_utils.util.metadata import REPLICA_NOT_AVAILABLE_ERROR


class ReplicaUnavailabilityCmd(KafkaCheckCmd):

    def build_subparser(self, subparsers: Any) -> Any:
        subparser = subparsers.add_parser(
            'replica_unavailability',
            description='Check availability of replicas for all brokers in cluster.',
            help='This command will fail if there are any replicas which are not'
                 ' available for communication in the cluster.',
        )
        return subparser

    def run_command(self) -> tuple[int, dict[str, Any]]:
        """replica_unavailability command, checks number of replicas not available
        for communication over all brokers in the Kafka cluster."""
        result = cast(Tuple[Set[Tuple[str, int]], Set[int]], get_topic_partition_with_error(
            self.cluster_config,
            REPLICA_NOT_AVAILABLE_ERROR,
            fetch_unavailable_brokers=True,
        ))
        replica_unavailability, unavailable_brokers = result

        errcode = status_code.OK if not replica_unavailability else status_code.CRITICAL
        out = _prepare_output(replica_unavailability, unavailable_brokers, self.args.verbose, self.args.head)
        return errcode, out


def _prepare_output(partitions: Collection[tuple[str, int]], unavailable_brokers: set[int], verbose: bool, head_limit: int) -> dict[str, Any]:
    """Returns dict with 'raw' and 'message' keys filled."""
    partitions_count = len(partitions)
    out: dict[str, Any] = {}
    out['raw'] = {
        'replica_unavailability_count': partitions_count,
    }

    if head_limit != -1:
        partitions = list(itertools.islice(partitions, head_limit))

    if partitions_count == 0:
        out['message'] = 'All replicas available for communication.'
    else:
        out['message'] = "{replica_unavailability} replicas unavailable for communication. " \
            "Unavailable Brokers: {unavailable_brokers}".format(
            replica_unavailability=partitions_count,
            unavailable_brokers=', '.join([str(e) for e in unavailable_brokers]),
        )
        if verbose:
            lines = (
                f'{topic}:{partition}'
                for (topic, partition) in partitions
            )
            if head_limit != -1:
                title = f"Top {head_limit} partitions:\n"
            else:
                title = "Partitions:\n"

            out['verbose'] = title + "\n".join(lines)

    if verbose:
        out['raw']['partitions'] = [
            {'topic': topic, 'partition': partition}
            for (topic, partition) in partitions
        ]

    return out
