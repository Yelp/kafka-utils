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
import mock
import pytest

from kafka_utils.kafka_rolling_restart import precheck


@mock.patch(
    'kafka_utils.kafka_rolling_restart.precheck.check_preconditions',
    return_value=True,
    autospec=True,
)
@mock.patch('kafka_utils.kafka_rolling_restart.precheck.execute', autospec=True)
def test_prechecks_success(mock_execute, mock_precondition):
    precheck_conditions = precheck.prechecks(
        package_name="kafka",
        package_version="0.10.1.1",
        configs=None,
        config_file=None,
        install_cmd="dummy_cmd",
    )
    precheck.ensure_preconditions_before_executing_restart(
        precheck_conditions,
        "hostname",
    )

    assert mock_execute.call_count == 0
    assert mock_precondition.call_count == 1


@mock.patch(
    'kafka_utils.kafka_rolling_restart.precheck.check_preconditions',
    return_value=True,
    autospec=True,
)
@mock.patch('kafka_utils.kafka_rolling_restart.precheck.execute', autospec=True)
def test_prechecks_success_with_config_check(mock_execute, mock_precondition):
    precheck_conditions = precheck.prechecks(
        package_name=None,
        package_version=None,
        configs=['num.io.threads=5'],
        config_file=None,
        install_cmd=None,
    )
    precheck.ensure_preconditions_before_executing_restart(
        precheck_conditions,
        "hostname",
    )

    assert mock_execute.call_count == 0


@mock.patch(
    'kafka_utils.kafka_rolling_restart.precheck.check_preconditions',
    return_value=False,
    autospec=True,
)
@mock.patch('kafka_utils.kafka_rolling_restart.precheck.execute', autospec=True)
def test_prechecks_failure(mock_execute, mock_precondition):
    precheck_conditions = precheck.prechecks(
        package_name="kafka",
        package_version="0.10.1.1",
        install_cmd="dummy_cmd",
        configs=None,
        config_file=None,
    )
    with pytest.raises(precheck.PrecheckFailedException):
        precheck.ensure_preconditions_before_executing_restart(
            precheck_conditions,
            "hostname",
        )

    assert mock_execute.call_count == 1
    assert mock_precondition.call_count == 2
