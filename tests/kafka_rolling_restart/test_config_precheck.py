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

from kafka_utils.kafka_rolling_restart.config_precheck import ConfigPrecheck
from kafka_utils.kafka_rolling_restart.precheck import PrecheckFailedException


@mock.patch(
    'kafka_utils.kafka_rolling_restart.config_precheck.execute',
    autospec=True,
    return_value={'test_host': 'auto.create.topics.enable=true'},
)
def test_assert_config(mock_execute):
    args = '--ensure-configs auto.create.topics.enable=true'
    version_precheck = ConfigPrecheck(args)
    version_precheck._assert_configs_present('test_host')

    assert mock_execute.call_count == 1


@mock.patch(
    'kafka_utils.kafka_rolling_restart.config_precheck.execute',
    autospec=True,
    return_value={'test_host': 'auto.create.topics.enable=true'},
)
def test_assert_config_raises_exception(mock_execute):
    args = '--ensure-configs auto.create.topics.enable=false'
    version_precheck = ConfigPrecheck(args)
    with pytest.raises(PrecheckFailedException):
        version_precheck._assert_configs_present('test_host')

    assert mock_execute.call_count == 1
