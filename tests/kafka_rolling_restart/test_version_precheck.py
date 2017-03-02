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
