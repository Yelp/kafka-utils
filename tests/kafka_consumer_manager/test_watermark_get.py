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

from kafka_utils.kafka_consumer_manager. \
    commands.watermark_get import WatermarkGet


class TestGetWatermark(object):

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.'
                'commands.watermark_get.KafkaToolClient',
                autospec=True,
        ) as mock_client:
            yield mock_client

    def test_get_watermark(self, client):
        topics = '__consumer_offsets'

        with mock.patch(
                'kafka_utils.util.monitoring.'
                'get_watermark_for_topic',
                return_value=topics,
                autospec=True,
        ) as get_watermark:
            WatermarkGet.get_watermarks(
                client,
                topics
            )
            get_watermark.call_count == 1