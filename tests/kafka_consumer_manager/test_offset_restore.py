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
from __future__ import absolute_import

import mock
import pytest

from kafka_utils.kafka_consumer_manager. \
    commands.offset_restore import OffsetRestore
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.monitoring import ConsumerPartitionOffsets


class TestOffsetRestore(object):

    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1, 2, 3],
        "topic3": [0, 1],
    }
    consumer_offsets_metadata = {
        'topic1':
        [
            ConsumerPartitionOffsets(topic='topic1', partition=0, current=20, highmark=655, lowmark=655),
            ConsumerPartitionOffsets(topic='topic1', partition=1, current=10, highmark=655, lowmark=655)
        ]
    }
    parsed_consumer_offsets = {'groupid': 'group1', 'offsets': {'topic1': {0: 10, 1: 20}}}
    new_consumer_offsets = {'topic1': {0: 10, 1: 20}}
    kafka_consumer_offsets = {'topic1': [
        ConsumerPartitionOffsets(topic='topic1', partition=0, current=30, highmark=40, lowmark=10),
        ConsumerPartitionOffsets(topic='topic1', partition=1, current=20, highmark=40, lowmark=10),
    ]}

    @pytest.fixture
    def mock_kafka_client(self):
        mock_kafka_client = mock.MagicMock(
            spec=KafkaToolClient
        )
        mock_kafka_client.get_partition_ids_for_topic. \
            side_effect = self.topics_partitions
        return mock_kafka_client

    def test_build_new_offsets(self, mock_kafka_client):
        new_offsets = OffsetRestore.build_new_offsets(
            mock_kafka_client,
            {'topic1': {0: 10, 1: 20}},
            {'topic1': [0, 1]},
            self.kafka_consumer_offsets,
        )

        assert new_offsets == self.new_consumer_offsets
