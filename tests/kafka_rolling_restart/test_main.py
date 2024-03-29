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
import requests
from requests.exceptions import RequestException

from kafka_utils.kafka_rolling_restart import main
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.zookeeper import ZK


@mock.patch.object(main.FuturesSession, 'get', autospec=True)
def test_read_cluster_value_partitions(mock_get):
    response = mock.Mock(status_code=200, spec=requests.Response)
    response.json.return_value = {'value': 1}

    request = mock_get.return_value
    request.result.return_value = response

    p, b = main.read_cluster_status(["host1", "host2", "host3"], 80, "jolokia", None, None)

    assert p == 3   # 3 missing partitions
    assert b == 0   # 0 missing brokers


@mock.patch.object(main.FuturesSession, 'get', autospec=True)
def test_read_cluster_value_exit(mock_get):
    response = mock.Mock(status_code=404, spec=requests.Response)

    request = mock_get.return_value
    request.result.return_value = response

    with pytest.raises(SystemExit):
        p, b = main.read_cluster_status(["host1"], 80, "jolokia", None, None)


@mock.patch.object(main.FuturesSession, 'get', autospec=True)
def test_read_cluster_value_no_key(mock_get):
    response = mock.Mock(status_code=200, spec=requests.Response)
    response.json.return_value = {'wrong_key': 1}

    request = mock_get.return_value
    request.result.return_value = response

    p, b = main.read_cluster_status(["host1"], 80, "jolokia", None, None)

    assert p == 0   # 0 missing partitions
    assert b == 1   # 1 missing brokers


@mock.patch.object(main.FuturesSession, 'get', autospec=True)
def test_read_cluster_value_server_down(mock_get):
    request = mock_get.return_value
    request.result.side_effect = RequestException

    p, b = main.read_cluster_status(["host1"], 80, "jolokia", None, None)

    assert p == 0   # 0 missing partitions
    assert b == 1   # 1 missing brokers


def read_cluster_state_values(first_part, repeat):
    yield from first_part
    while True:
        yield repeat


@mock.patch.object(
    main,
    'read_cluster_status',
    side_effect=read_cluster_state_values([(100, 1), (0, 1), (100, 0)], (0, 0)),
    autospec=True,
)
@mock.patch('time.sleep', autospec=True)
def test_wait_for_stable_cluster_success(mock_sleep, mock_read):
    main.wait_for_stable_cluster([], 1, "", None, None, 5, 3, 100)

    assert mock_read.call_count == 6
    assert mock_sleep.mock_calls == [mock.call(5)] * 5


@mock.patch.object(
    main,
    'read_cluster_status',
    side_effect=read_cluster_state_values([], (100, 0)),
    autospec=True,
)
@mock.patch('time.sleep', autospec=True)
def test_wait_for_stable_cluster_timeout(mock_sleep, mock_read):
    with pytest.raises(main.WaitTimeoutException):
        main.wait_for_stable_cluster([], 1, "", None, None, 5, 3, 100)

    assert mock_read.call_count == 21
    assert mock_sleep.mock_calls == [mock.call(5)] * 20


cluster_config = ClusterConfig(
    type='mytype',
    name='some_cluster',
    broker_list='some_list',
    zookeeper='some_ip'
)


@mock.patch(
    'kafka_utils.util.zookeeper.KazooClient',
    autospec=True
)
@mock.patch.object(
    ZK,
    'get_brokers',
    side_effect=[{2: {'host': 'broker2'},
                  3: {'host': 'broker3'},
                  1: {'host': 'broker1'}}],
    autospec=True
)
def test_get_broker_list1(mock_client, mock_get_broker):
    p = main.get_broker_list(cluster_config, active_controller_for_last=False)
    assert p == [(1, 'broker1'), (2, 'broker2'), (3, 'broker3')]


@mock.patch(
    'kafka_utils.util.zookeeper.KazooClient',
    autospec=True
)
@mock.patch.object(
    ZK,
    'get_brokers',
    side_effect=[{2: {'host': 'broker2'},
                  3: {'host': 'broker3'},
                  1: {'host': 'broker1'}}],
    autospec=True
)
@mock.patch.object(
    ZK,
    'get_json',
    side_effect=[{}],
    autospec=True
)
def test_get_broker_list2(mock_client, mock_get_broker, mock_get_json):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        main.get_broker_list(cluster_config, active_controller_for_last=True)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


@mock.patch(
    'kafka_utils.util.zookeeper.KazooClient',
    autospec=True
)
@mock.patch.object(
    ZK,
    'get_brokers',
    side_effect=[{2: {'host': 'broker2'},
                  3: {'host': 'broker3'},
                  1: {'host': 'broker1'}}],
    autospec=True
)
@mock.patch.object(
    ZK,
    'get_json',
    side_effect=[{'brokerid': 2}],
    autospec=True
)
def test_get_broker_list3(mock_client, mock_get_broker, mock_get_json):
    p = main.get_broker_list(cluster_config, active_controller_for_last=True)
    assert p == [(1, 'broker1'), (3, 'broker3'), (2, 'broker2')]
