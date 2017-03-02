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
