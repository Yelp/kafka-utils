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
    commands.offset_get import OffsetGet


class TestOffsetGet(object):

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.'
                'commands.offset_get.KafkaToolClient',
                autospec=True,
        ) as mock_client:
            yield mock_client

    def test_get_offsets(self, client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'kafka_utils.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                client,
                consumer_group,
                topics,
                'zookeeper',
            )

            assert client.load_metadata_for_topics.call_count == 1
            assert client.send_offset_fetch_request.call_count == 1
            assert client.send_offset_fetch_request_kafka.call_count == 0

    def test_get_offsets_kafka(self, client):
        consumer_group = 'group1'
        topics = {'topic1': {0: 100}}

        with mock.patch(
            'kafka_utils.util.offsets._verify_topics_and_partitions',
            return_value=topics,
            autospec=True,
        ):
            OffsetGet.get_offsets(
                client,
                consumer_group,
                topics,
                'kafka',
            )

            assert client.load_metadata_for_topics.call_count == 1
            assert client.send_offset_fetch_request.call_count == 0
            assert client.send_offset_fetch_request_kafka.call_count == 1
