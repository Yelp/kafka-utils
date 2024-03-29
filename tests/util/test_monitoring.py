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
from unittest import mock

import pytest
from kafka.common import GroupCoordinatorNotAvailableError
from kafka.common import KafkaUnavailableError
from test_offsets import MyKafkaToolClient
from test_offsets import TestOffsetsBase

from kafka_utils.util.error import UnknownPartitions
from kafka_utils.util.error import UnknownTopic
from kafka_utils.util.monitoring import ConsumerPartitionOffsets
from kafka_utils.util.monitoring import get_consumer_offsets_metadata
from kafka_utils.util.monitoring import get_watermark_for_regex
from kafka_utils.util.monitoring import get_watermark_for_topic
from kafka_utils.util.monitoring import merge_offsets_metadata
from kafka_utils.util.monitoring import merge_partition_offsets


class TestMonitoring(TestOffsetsBase):

    def test_offset_metadata_invalid_arguments(self, kafka_client_mock):
        with pytest.raises(TypeError):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                "this won't even be consulted",
                "this should be a list or dict",
            )

    def test_offset_metadata_unknown_topic(self, kafka_client_mock):
        with pytest.raises(UnknownTopic):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                "this won't even be consulted",
                ["something that doesn't exist"],
            )

    def test_offset_metadata_unknown_topic_no_fail(self, kafka_client_mock):
        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            "this won't even be consulted",
            ["something that doesn't exist"],
            raise_on_error=False
        )
        assert not actual

    def test_offset_metadata_unknown_partitions(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                self.group,
                {'topic1': [99]},
            )

    def test_offset_metadata_unknown_partitions_no_fail(self, kafka_client_mock):
        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            self.group,
            {'topic1': [99]},
            raise_on_error=False
        )
        assert not actual

    def test_offset_metadata_invalid_partition_subset(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                self.group,
                {'topic1': [1, 99]},
            )

    def test_offset_metadata_invalid_partition_subset_no_fail(
        self,
        kafka_client_mock
    ):
        # Partition 99 does not exist, so we expect to have
        # offset metadata ONLY for partition 1.
        expected = [
            ConsumerPartitionOffsets('topic1', 1, 20, 30, 5)
        ]

        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            self.group,
            {'topic1': [1, 99]},
            raise_on_error=False
        )
        assert 'topic1' in actual
        assert actual['topic1'] == expected

    def test_get_metadata_kafka_error(self, kafka_client_mock):
        with mock.patch.object(
            MyKafkaToolClient,
            'load_metadata_for_topics',
            side_effect=KafkaUnavailableError("Boom!"),
            autospec=True
        ) as mock_func:
            with pytest.raises(KafkaUnavailableError):
                get_consumer_offsets_metadata(
                    kafka_client_mock,
                    self.group,
                    {'topic1': [99]},
                )
            assert mock_func.call_count == 2

    def test_merge_offsets_metadata_empty(self):
        zk_offsets = {}
        kafka_offsets = {}
        expected = {}

        result = merge_offsets_metadata([], zk_offsets, kafka_offsets)
        assert result == expected

    def test_merge_offsets_metadata(self):
        zk_offsets = {
            'topic1': {0: 6},
        }
        kafka_offsets = {
            'topic1': {0: 5},
        }
        expected = {
            'topic1': {0: 6},
        }

        topics = ['topic1']
        result = merge_offsets_metadata(topics, zk_offsets, kafka_offsets)
        assert result == expected

    def test_merge_partition_offsets(self):
        partition_offsets = [
            {0: 6},
            {0: 5},
        ]
        expected = {0: 6}

        result = merge_partition_offsets(*partition_offsets)
        assert result == expected

    def _has_no_partitions(self, offsets_metadata):
        return all(
            not partitions
            for partitions in offsets_metadata.values()
        )

    def test_offsets_kafka_empty(self, kafka_client_mock):
        with mock.patch.object(
            MyKafkaToolClient,
            'send_offset_fetch_request_kafka',
            return_value={},
            autospec=True,
        ) as mock_get_kafka:
            actual = get_consumer_offsets_metadata(
                kafka_client_mock,
                self.group,
                self.topics,
            )

            assert mock_get_kafka.call_count == 1
            assert self._has_no_partitions(actual)

    def test_offsets_kafka_error(self, kafka_client_mock):
        with mock.patch.object(
            MyKafkaToolClient,
            'send_offset_fetch_request_kafka',
            side_effect=GroupCoordinatorNotAvailableError('Boom!'),
            autospec=True,
        ) as mock_get_kafka:
            with pytest.raises(GroupCoordinatorNotAvailableError):
                get_consumer_offsets_metadata(
                    kafka_client_mock,
                    self.group,
                    self.topics,
                )

            assert mock_get_kafka.call_count == 1

    def test_get_watermark_for_topic(self, kafka_client_mock):
        with mock.patch(
                'kafka_utils.util.monitoring.'
                'get_topics_watermarks',
                return_value={'test_topic': [1, 99]},
                autospec=True,
        ):
            result = get_watermark_for_topic(
                kafka_client_mock,
                'test_topic')
            assert result['test_topic'][1] == 99

    def test_get_watermark_for_topic_regex(self, kafka_client_mock):
        with mock.patch(
                'kafka_utils.util.monitoring.'
                'get_topics_watermarks',
                return_value=[{'test_topic_1': [1, 99]},
                              {'test_topic_2': [1, 100]}],
                autospec=True,
        ):
            result = get_watermark_for_regex(
                kafka_client_mock,
                'test_topic*')
            assert result[0]['test_topic_1'][1] == 99
            assert result[1]['test_topic_2'][1] == 100
