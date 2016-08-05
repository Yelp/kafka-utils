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

    def test_get_watermark_for_topic(self, client):
        topics = '__consumer_offsets'
        client.topic_partitions = {}
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.commands.'
            'watermark_get.get_watermark_for_topic',
            return_value={'test_topic': [1, 99, 3]},
            autospec=True,
        ) as mock_get_watermark:
            WatermarkGet.get_watermarks(
                client,
                topics,
                exact=True
            )
            assert mock_get_watermark.call_count == 1

    def test_get_watermark_for_regex(self, client):
        topics = '__consumer_*'
        client.topic_partitions = {}
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.commands.'
            'watermark_get.get_watermark_for_regex',
            return_value={'__consumer_1': [1, 99, 3],
                          '__consumer_2': [2, 100, 2]},
            autospec=True,
        ) as mock_get_watermark:
            WatermarkGet.get_watermarks(
                client,
                topics,
                exact=False
            )
            assert mock_get_watermark.call_count == 1
