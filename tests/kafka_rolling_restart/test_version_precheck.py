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

from kafka_utils.kafka_rolling_restart.precheck import PrecheckFailedException
from kafka_utils.kafka_rolling_restart.version_precheck import VersionPrecheck


@mock.patch(
    'kafka_utils.kafka_rolling_restart.version_precheck.execute',
    autospec=True,
    return_value={'test_host': False},
)
def test_assert_kafka_package_name_version(mock_execute):
    args = '--package-name kafka --package-version 0.9.0.2'
    version_precheck = VersionPrecheck(args)
    version_precheck._assert_kafka_package_name_version('test_host')

    assert mock_execute.call_count == 1


@mock.patch(
    'kafka_utils.kafka_rolling_restart.version_precheck.execute',
    autospec=True,
    return_value={'test_host': True},
)
def test_assert_kafka_package_name_version_raises_exception(mock_execute):
    args = '--package-name kafka --package-version 0.9.0.2'
    version_precheck = VersionPrecheck(args)
    with pytest.raises(PrecheckFailedException):
        version_precheck._assert_kafka_package_name_version('test_host')

    assert mock_execute.call_count == 1
