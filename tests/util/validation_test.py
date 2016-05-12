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
from kafka_tools.util.validation import _validate_format
from kafka_tools.util.validation import _validate_plan
from kafka_tools.util.validation import _validate_plan_base
from kafka_tools.util.validation import validate_plan


def test_validate_format():
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 1]},
            {"partition": 1, "topic": u't1', "replicas": [1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [1, 2]}
        ]
    }

    # Verify correct format
    assert _validate_format(assignment) is True


def test_validate_format_version_absent():
    assignment = {
        "partitions":
        [{"partition": 0, "topic": u't1', "replicas": [0, 1, 2]}]
    }

    # 'version' key missing: Verify Validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_key():
    # Invalid key "cluster"
    assignment = {
        "cluster": "invalid_key",
        "partitions":
        [{"partition": 0, "topic": u't1', "replicas": [0, 1, 2]}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_partitions_absent():
    assignment = {"version": 1}

    # 'partitions' key missing: Verify Validation failed
    assert _validate_format(assignment) is False


def test_validate_format_version_wrong():
    # Invalid version 2
    assignment = {
        "version": 2,
        "partitions":
        [{"partition": 0, "topic": u't1', "replicas": [0, 1, 2]}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_empty():
    # Empty assignment
    assignment = {}

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_partitions_empty():
    # Empty partitions-list
    assignment = {
        "version": 1,
        "partitions": []
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_key_partition():
    # Invalid/unwanted key 'isr'
    assignment = {
        "version": 1,
        "partitions":
        [
            {"topic": u't1', "replicas": [0, 1, 2]},
            {"isr": [0, 1, 2], "partition": 0, "topic": u't2', "replicas": [0, 1, 2]}
        ]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_missing_key_partition():
    # 'partition' key missing in partitions
    assignment = {
        "version": 1,
        "partitions":
        [
            {"topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1, 2]}
        ]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_missing_key_topic():
    # 'topic' key missing in partitions
    assignment = {
        "version": 1,
        "partitions":
        [
            {"partition": 0, "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1, 2]}
        ]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_missing_key_replicas():
    # 'replicas' key missing in partitions
    assignment = {
        "version": 1,
        "partitions":
        [
            {"partition": 0, "topic": u't1'},
            {"partition": 0, "topic": u't2', "replicas": [0, 1, 2]}
        ]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_type_partitions():
    # 'partitions' is not list
    assignment = {
        "version": 1,
        "partitions": {"partition": 0, "topic": u't2', "replicas": [0, 1, 2]}
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_type_partition():
    # partition-id is not int
    assignment = {
        "version": 1,
        "partitions": [{"partition": '0', "topic": u't2', "replicas": [0, 1, 2]}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_type_topic():
    # topic-id is not string
    assignment = {
        "version": 1,
        "partitions": [{"partition": 0, "topic": 2, "replicas": [0, 1, 2]}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_type_replicas():
    # replicas is not list
    assignment = {
        "version": 1,
        "partitions": [{"partition": 0, "topic": u't1', "replicas": 0}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_invalid_type_replica_brokers():
    # replica-brokers are not int
    assignment = {
        "version": 1,
        "partitions": [{"partition": 0, "topic": u't1', "replicas": ['0', 1]}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_format_replica_empty():
    # replica-brokers are empty
    assignment = {
        "version": 1,
        "partitions": [{"partition": 0, "topic": u't1', "replicas": []}]
    }

    # Verify validation failed
    assert _validate_format(assignment) is False


def test_validate_plan():
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 1]},
            {"partition": 1, "topic": u't1', "replicas": [1, 2]},
        ]
    }

    # Verify valid assignment
    assert _validate_plan(assignment) is True


def test_validate_plan_duplicate_partitions():
    # Duplicate partition (t1, 0)
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 1]},
            {"partition": 0, "topic": u't1', "replicas": [1, 2]},
        ]
    }

    # Verify validation failed
    assert _validate_plan(assignment) is False


def test_validate_plan_duplicate_replica_brokers():
    # Duplicate replica-brokers [0, 0, 1]
    assignment = {
        "version": 1,
        "partitions": [{"partition": 0, "topic": u't1', "replicas": [0, 0, 1]}]
    }

    # Verify validation failed
    assert _validate_plan(assignment) is False


def test_validate_plan_different_replication_factor():
    # Replication-factor (t1, 0): 2
    # Replication-factor (t1, 1): 3
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 1]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1]}
        ]
    }

    # Verify validation failed
    assert _validate_plan(assignment) is False


def test_validate_plan_base():
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [1, 2]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2]},
            {"partition": 1, "topic": u't1', "replicas": [1, 2]},
            {"partition": 1, "topic": u't2', "replicas": [0]}
        ]
    }

    # Verify valid assignment compared to base
    assert _validate_plan_base(assignment, base_assignment) is True


def test_validate_plan_base_invalid_partitions():
    # partition: ('invalid', 0) not present in base assignment
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [1, 2]},
            {"partition": 0, "topic": u'invalid', "replicas": [0, 1]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify validation failed
    assert _validate_plan_base(assignment, base_assignment) is False


def test_validate_plan_base_invalid_partitions_2():
    # partition: ('t2', 1) not present in base assignment
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [1, 2]},
            {"partition": 1, "topic": u't2', "replicas": [0, 1]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify validation failed
    assert _validate_plan_base(assignment, base_assignment) is False


def test_validate_plan_base_replication_factor_changed():
    # replication-factor of (t1, 0) is 1 in assignment and 2 in base-assignment
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [1]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify validation failed
    assert _validate_plan_base(assignment, base_assignment) is False


def test_validate_plan_1():
    # Only given assignment without base assignment
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [1, 4]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify valid plan
    assert validate_plan(assignment) is True


def test_validate_plan_2():
    # All partitions in new-plan
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 3]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 3]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify valid plan
    assert validate_plan(assignment, base_assignment, is_partition_subset=False) is True


def test_validate_plan_incomplete_partition_subset():
    # All partitions in new-plan
    # Given complete-assignment partition-set incomplete
    complete_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 3]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify valid plan
    assert validate_plan(
        complete_assignment,
        base_assignment,
        is_partition_subset=False,
    ) is False


def test_validate_plan_incomplete_partition_subset_2():
    # All partitions in new-plan
    # Given complete-assignment partition-set superset
    complete_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]},
            {"partition": 0, "topic": u't3', "replicas": [0, 1]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 3]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify valid plan
    assert validate_plan(
        complete_assignment,
        base_assignment,
        is_partition_subset=False,
    ) is False


def test_validate_plan_3():
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 0, "topic": u't2', "replicas": [0, 3]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 3]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify valid plan
    assert validate_plan(assignment, base_assignment) is True


def test_validate_plan_invalid_format():
    # Invalid format: partition-id string
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": '0', "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 0, "topic": u't2', "replicas": [0, 3]}
        ]
    }
    base_assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [0, 2, 3]},
            {"partition": 1, "topic": u't1', "replicas": [0, 1, 2]},
            {"partition": 0, "topic": u't2', "replicas": [0, 1]}
        ]
    }

    # Verify validation failed
    assert validate_plan(assignment, base_assignment) is False


def test_validate_plan_duplicate_partition():
    # Invalid assignment: Duplicate partition
    assignment = {
        "version": 1,
        "partitions": [
            {"partition": 0, "topic": u't1', "replicas": [2, 1, 0]},
            {"partition": 0, "topic": u't1', "replicas": [0, 3]}
        ]
    }

    # Verify validation failed
    assert validate_plan(assignment) is False
