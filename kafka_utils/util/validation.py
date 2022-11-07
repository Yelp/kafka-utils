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
""""Provide functions to validate and generate a Kafka assignment"""
from __future__ import annotations

import logging
from collections import Counter

from typing_extensions import TypedDict


_log = logging.getLogger(__name__)


class PartitionDict(TypedDict):
    topic: str
    partition: int
    replicas: list[int]


class PlanDict(TypedDict):
    version: int
    partitions: list[PartitionDict]


def plan_to_assignment(plan: PlanDict) -> dict[tuple[str, int], list[int]]:
    """Convert the plan to the format used by cluster-topology."""
    assignment = {}
    for elem in plan['partitions']:
        assignment[
            (elem['topic'], elem['partition'])
        ] = elem['replicas']
    return assignment


def assignment_to_plan(assignment: dict[tuple[str, int], list[int]]) -> PlanDict:
    """Convert an assignment to the format used by Kafka to
    describe a reassignment plan.
    """
    return {
        'version': 1,
        'partitions':
        [{'topic': t_p[0],
          'partition': t_p[1],
          'replicas': replica
          } for t_p, replica in assignment.items()]
    }


def validate_plan(
    new_plan: PlanDict,
    base_plan: PlanDict | None = None,
    is_partition_subset: bool = True,
    allow_rf_change: bool = False,
    allow_rf_mismatch: bool = False,
) -> bool:
    """Verify that the new plan is valid for execution.

    Given kafka-reassignment plan should affirm with following rules:
    - Plan should have at least one partition for re-assignment
    - Partition-name list should be subset of base-plan partition-list
    - Replication-factor for each partition of same topic is same
    - Replication-factor for each partition remains unchanged
    - No duplicate broker-ids in each replicas
    """
    if not _validate_plan(new_plan, allow_rf_mismatch=allow_rf_mismatch):
        _log.error('Invalid proposed-plan.')
        return False

    # Validate given plan in reference to base-plan
    if base_plan:
        if not _validate_plan(base_plan, allow_rf_mismatch=allow_rf_mismatch):
            _log.error('Invalid assignment from cluster.')
            return False
        if not _validate_plan_base(
            new_plan,
            base_plan,
            is_partition_subset,
            allow_rf_change,
        ):
            return False
    # Plan validation successful
    return True


def _validate_plan_base(
    new_plan: PlanDict,
    base_plan: PlanDict,
    is_partition_subset: bool = True,
    allow_rf_change: bool = False,
) -> bool:
    """Validate if given plan is valid comparing with given base-plan.

    Validate following assertions:
    - Partition-check: New partition-set should be subset of base-partition set
    - Replica-count check: Replication-factor for each partition remains same
    - Broker-check: New broker-set should be subset of base broker-set
    """

    # Verify that partitions in plan are subset of base plan.
    new_partitions = {
        (p_data['topic'], p_data['partition'])
        for p_data in new_plan['partitions']
    }
    base_partitions = {
        (p_data['topic'], p_data['partition'])
        for p_data in base_plan['partitions']
    }
    if is_partition_subset:
        invalid_partitions = list(new_partitions - base_partitions)
    else:
        # partition set should be equal
        invalid_partitions = list(
            new_partitions.union(base_partitions) -
            new_partitions.intersection(base_partitions),
        )
    if invalid_partitions:
        _log.error(
            'Invalid partition(s) found: {p_list}'.format(
                p_list=invalid_partitions,
            )
        )
        return False

    # Verify replication-factor remains consistent
    base_partition_replicas = {
        (p_data['topic'], p_data['partition']): p_data['replicas']
        for p_data in base_plan['partitions']
    }
    new_partition_replicas = {
        (p_data['topic'], p_data['partition']): p_data['replicas']
        for p_data in new_plan['partitions']
    }
    if not allow_rf_change:
        invalid_replication_factor = False
        for new_partition, replicas in new_partition_replicas.items():
            base_replica_cnt = len(base_partition_replicas[new_partition])
            if len(replicas) != base_replica_cnt:
                invalid_replication_factor = True
                _log.error(
                    'Replication-factor Mismatch: Partition: {partition}: '
                    'Base-replicas: {expected}, Proposed-replicas: {actual}'
                    .format(
                        partition=new_partition,
                        expected=base_partition_replicas[new_partition],
                        actual=replicas,
                    ),
                )
        if invalid_replication_factor:
            return False

    # Validation successful
    return True


def _validate_format(plan: PlanDict) -> bool:
    """Validate if the format of the plan as expected.

    Validate format of plan on following rules:
    a) Verify if it ONLY and MUST have keys and value, 'version' and 'partitions'
    b) Verify if each value of 'partitions' ONLY and MUST have keys 'replicas',
        'partition', 'topic'
    c) Verify desired type of each value
    d) Verify non-empty partitions and replicas
    Sample-plan format:
    {
        "version": 1,
        "partitions": [
            {"partition":0, "topic":'t1', "replicas":[0,1,2]},
            {"partition":0, "topic":'t2', "replicas":[1,2]},
            ...
        ]}
    """
    # Verify presence of required keys
    if set(plan.keys()) != {'version', 'partitions'}:
        _log.error(
            'Invalid or incomplete keys in given plan. Expected: "version", '
            '"partitions". Found:{keys}'
            .format(keys=', '.join(list(plan.keys()))),
        )
        return False

    # Invalid version
    if plan['version'] != 1:
        _log.error(
            'Invalid version of plan {version}'
            .format(version=plan['version']),
        )
        return False

    # Empty partitions
    if not plan['partitions']:
        _log.error('"partitions" list found empty"')
        return False

    # Invalid partitions type
    if not isinstance(plan['partitions'], list):
        _log.error('"partitions" of type list expected.')  # type: ignore[unreachable]
        return False

    # Invalid partition-data
    for p_data in plan['partitions']:
        if set(p_data.keys()) != {'topic', 'partition', 'replicas'}:
            _log.error(
                'Invalid keys in partition-data {keys}'
                .format(keys=', '.join(list(p_data.keys()))),
            )
            return False
        # Check types
        if not isinstance(p_data['topic'], str):
            _log.error(  # type: ignore[unreachable]
                '"topic" of type unicode expected {p_data}, found {t_type}'
                .format(p_data=p_data, t_type=type(p_data['topic'])),
            )
            return False
        if not isinstance(p_data['partition'], int):
            _log.error(  # type: ignore[unreachable]
                '"partition" of type int expected {p_data}, found {p_type}'
                .format(p_data=p_data, p_type=type(p_data['partition'])),
            )
            return False
        if not isinstance(p_data['replicas'], list):
            _log.error(  # type: ignore[unreachable]
                '"replicas" of type list expected {p_data}, found {r_type}'
                .format(p_data=p_data, r_type=type(p_data['replicas'])),
            )
            return False
        if not p_data['replicas']:
            _log.error(
                'Non-empty "replicas" expected: {p_data}'
                .format(p_data=p_data),
            )
            return False
        # Invalid broker-type
        for broker in p_data['replicas']:
            if not isinstance(broker, int):
                _log.error(  # type: ignore[unreachable]
                    '"replicas" of type integer list expected {p_data}'
                    .format(p_data=p_data),
                )
                return False
    return True


def _validate_plan(plan: PlanDict, allow_rf_mismatch: bool = False) -> bool:
    """Validate if given plan is valid based on kafka-cluster-assignment protocols.

    Validate following parameters:
    - Correct format of plan
    - Partition-list should be unique
    - Every partition of a topic should have same replication-factor
    - Replicas of a partition should have unique broker-set
    """
    # Validate format of plan
    if not _validate_format(plan):
        return False

    # Verify no duplicate partitions
    partition_names = [
        (p_data['topic'], p_data['partition'])
        for p_data in plan['partitions']
    ]
    duplicate_partitions = [
        partition for partition, count in Counter(partition_names).items()
        if count > 1
    ]
    if duplicate_partitions:
        _log.error(
            'Duplicate partitions in plan {p_list}'
            .format(p_list=duplicate_partitions),
        )
        return False

    # Verify no duplicate brokers in partition-replicas
    dup_replica_brokers = []
    for p_data in plan['partitions']:
        dup_replica_brokers = [
            broker
            for broker, count in Counter(p_data['replicas']).items()
            if count > 1
        ]
        if dup_replica_brokers:
            _log.error(
                'Duplicate brokers: ({topic}, {p_id}) in replicas {replicas}'
                .format(
                    topic=p_data['topic'],
                    p_id=p_data['partition'],
                    replicas=p_data['replicas'],
                )
            )
            return False

    # Verify same replication-factor for partitions in the same topic
    if not allow_rf_mismatch:
        topic_replication_factor: dict[str, int] = {}
        for partition_info in plan['partitions']:
            topic = partition_info['topic']
            replication_factor = len(partition_info['replicas'])
            if topic in list(topic_replication_factor.keys()):
                if topic_replication_factor[topic] != replication_factor:
                    _log.error(
                        'Mismatch in replication-factor of partitions for topic '
                        '{topic}'.format(topic=topic),
                    )
                    return False
            else:
                topic_replication_factor[topic] = replication_factor
    return True
