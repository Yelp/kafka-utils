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
from unittest import mock

import pytest

from kafka_utils.kafka_manual_throttle import main
from kafka_utils.util.zookeeper import ZK


BROKER_ID = 0


@pytest.fixture
def mock_zk():
    return mock.Mock(spec=ZK)


def test_apply_throttles(mock_zk):
    mock_zk.get_broker_config.return_value = {}

    brokers = [0, 1, 2]
    main.apply_throttles(mock_zk, brokers, 42, 24)

    assert mock_zk.set_broker_config.call_count == len(brokers)

    expected_config = {
        "config": {"leader.replication.throttled.rate": "42", "follower.replication.throttled.rate": "24"},
    }

    expected_set_calls = [
        mock.call(0, expected_config),
        mock.call(1, expected_config),
        mock.call(2, expected_config),
    ]

    assert mock_zk.set_broker_config.call_args_list == expected_set_calls


def test_clear_throttles(mock_zk):
    mock_zk.get_broker_config.return_value = {"config": {"leader.replication.throttled.rate": "42", "follower.replication.throttled.rate": "24"}}

    brokers = [0, 1, 2]
    main.clear_throttles(mock_zk, brokers)

    assert mock_zk.set_broker_config.call_count == len(brokers)

    expected_config = {"config": {}}
    expected_set_calls = [
        mock.call(0, expected_config),
        mock.call(1, expected_config),
        mock.call(2, expected_config),
    ]

    assert mock_zk.set_broker_config.call_args_list == expected_set_calls


@pytest.mark.parametrize(
    "current_config,leader,follower,expected_config", [
        ({}, None, None, {}),
        ({}, 42, None, {"leader.replication.throttled.rate": 42}),
        ({}, 42, 24, {"leader.replication.throttled.rate": 42, "follower.replication.throttled.rate": 24}),
        (
            {"leader.replication.throttled.rate": 42, "follower.replication.throttled.rate": 24},
            None,
            100,
            {"follower.replication.throttled.rate": 100},
        ),
        (
            {"other.config": "other"},
            42,
            24,
            {"other.config": "other", "leader.replication.throttled.rate": 42, "follower.replication.throttled.rate": 24}
        ),
    ]
)
def test_write_throttle(mock_zk, current_config, leader, follower, expected_config):
    mock_zk.get_broker_config.return_value = {"config": current_config}

    main.write_throttle(mock_zk, BROKER_ID, leader, follower)

    assert mock_zk.set_broker_config.call_count == 1

    expected_set_call = mock.call(BROKER_ID, {"config": expected_config})
    assert mock_zk.set_broker_config.call_args_list == [expected_set_call]
