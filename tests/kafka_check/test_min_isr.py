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

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.commands.min_isr import MinIsrCmd


class TestMinIsrCommand(object):
    REGULAR_TOPIC_NAME0 = 'topic_0'
    REGULAR_TOPIC_NAME1 = 'topic_1'
    TOPICS_STATE = {
        REGULAR_TOPIC_NAME0: {
            "version": 1,
            "partitions": {
                "0": {
                    "controller_epoch": 64,
                    "leader_epoch": 54,
                    "replicas": [
                        170396635,
                        170398981
                    ],
                    "isr": [
                        170398981
                    ],
                    "version": 1,
                    "leader": 170396635
                }
            }
        },
        REGULAR_TOPIC_NAME1: {
            "version": 1,
            "partitions": {
                "0": {
                    "controller_epoch": 64,
                    "leader_epoch": 189,
                    "replicas": [
                        170396635,
                        170398981
                    ],
                    "isr": [
                        170396635,
                        170398981
                    ],
                    "version": 1,
                    "leader": 170396635
                }
            }
        },
    }

    @pytest.fixture
    def cmd(self):
        return MinIsrCmd()

    def _prepare_topics(self, cmd, topics, min_isr, default_min_isr=None):
        attrs = {'get_topics.return_value': topics}
        cmd.zk = mock.MagicMock(**attrs)
        cmd.get_min_isr = mock.MagicMock(return_value=min_isr)
        cmd.args = mock.MagicMock(**{'default_min_isr': default_min_isr})

    def test_get_min_isr_empty(self, cmd):
        TOPIC_CONFIG_WITHOUT_MIN_ISR = {'version': 1, 'config': {'retention.ms': '86400000'}}
        attrs = {'get_topic_config.return_value': TOPIC_CONFIG_WITHOUT_MIN_ISR}
        cmd.zk = mock.MagicMock(**attrs)

        min_isr = cmd.get_min_isr(self.REGULAR_TOPIC_NAME0)

        assert cmd.zk.method_calls == [mock.call.get_topic_config(self.REGULAR_TOPIC_NAME0)]
        assert min_isr is None

    def test_get_min_isr(self, cmd):
        TOPIC_CONFIG_WITH_MIN_ISR = {
            'version': 1,
            'config': {'retention.ms': '86400000'},
            'min.insync.replicas': '3',
        }
        attrs = {'get_topic_config.return_value': TOPIC_CONFIG_WITH_MIN_ISR}
        cmd.zk = mock.MagicMock(**attrs)

        min_isr = cmd.get_min_isr(self.REGULAR_TOPIC_NAME0)

        assert cmd.zk.method_calls == [mock.call.get_topic_config(self.REGULAR_TOPIC_NAME0)]
        assert min_isr == 3

    def test_run_command_empty(self, cmd):
        self._prepare_topics(cmd, topics={}, min_isr=0)
        code, msg = cmd.run_command()

        cmd.get_min_isr.assert_not_called()
        assert code == status_code.OK
        assert msg == "All replicas in sync."

    def test_run_command_all_ok(self, cmd):
        self._prepare_topics(cmd, topics=self.TOPICS_STATE, min_isr=1)
        code, msg = cmd.run_command()

        assert code == status_code.OK
        assert msg == "All replicas in sync."

    def test_run_command_all_ok_without_min_isr_in_zk(self, cmd):
        self._prepare_topics(cmd, topics=self.TOPICS_STATE, min_isr=None)
        code, msg = cmd.run_command()

        assert code == status_code.OK
        assert msg == "All replicas in sync."

    def test_run_command_with_default_min_isr(self, cmd):
        self._prepare_topics(cmd, topics=self.TOPICS_STATE, min_isr=None, default_min_isr=1)
        code, msg = cmd.run_command()

        assert code == status_code.OK
        assert msg == "All replicas in sync."

    def test_run_command_with_fail_with_default_min_isr(self, cmd):
        self._prepare_topics(cmd, topics=self.TOPICS_STATE, min_isr=None, default_min_isr=2)
        code, msg = cmd.run_command()

        assert code == status_code.CRITICAL
        assert msg == "1 partition(s) have the number of replicas in sync that is lower than the specified min ISR."

    def test_run_command_all_fail(self, cmd):
        self._prepare_topics(cmd, topics=self.TOPICS_STATE, min_isr=3)
        code, msg = cmd.run_command()

        assert code == status_code.CRITICAL
        assert msg == "2 partition(s) have the number of replicas in sync that is lower than the specified min ISR."
