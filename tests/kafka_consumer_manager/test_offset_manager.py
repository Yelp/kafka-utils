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
import contextlib
import sys

import mock
import pytest

from kafka_utils.kafka_consumer_manager. \
    commands.offset_manager import OffsetManagerBase
from kafka_utils.util.client import KafkaToolClient


class TestOffsetManagerBase(object):
    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1, 2, 3],
        "topic3": [0, 1],
    }

    def _get_partition_ids_for_topic(self, topic):
        try:
            return self.topics_partitions[topic]
        # Since we patch sys.exit, let's mask this exception since this call
        # is triggered after the call to sys.exit
        except KeyError:
            pass

    @pytest.fixture
    def mock_kafka_client(self):
        mock_kafka_client = mock.MagicMock(
            spec=KafkaToolClient
        )
        mock_kafka_client.get_partition_ids_for_topic. \
            side_effect = self._get_partition_ids_for_topic
        return mock_kafka_client

    @contextlib.contextmanager
    def mock_get_topics(self):
        with mock.patch.object(
            OffsetManagerBase,
            "get_topics_from_consumer_group_id",
            spec=OffsetManagerBase.get_topics_from_consumer_group_id,
            return_value=["topic1", "topic2", "topic3"],
        ) as mock_get_topics:
            yield mock_get_topics

    def test_preprocess_args_with_topics_and_partitions(self, mock_kafka_client):
        args = mock.Mock(
            groupid="some_group",
            topic="topic1",
            partitions=[0, 1]
        )
        expected_topics_dict = {"topic1": [0, 1]}
        with self.mock_get_topics() as mock_get_topics:
            topics_dict = OffsetManagerBase.preprocess_args(
                args.groupid,
                args.topic,
                args.partitions,
                mock.Mock(),
                mock_kafka_client
            )
            assert mock_get_topics.called
            assert topics_dict == expected_topics_dict

    def test_preprocess_args_with_topics_no_partitions(self, mock_kafka_client):
        args = mock.Mock(
            groupid="some_group",
            topic="topic1",
            partitions=None
        )
        expected_topics_dict = {"topic1": [0, 1, 2]}
        with self.mock_get_topics() as mock_get_topics:
            topics_dict = OffsetManagerBase.preprocess_args(
                args.groupid,
                args.topic,
                args.partitions,
                mock.Mock(),
                mock_kafka_client
            )
            assert mock_get_topics.called
            assert topics_dict == expected_topics_dict

    def test_preprocess_args_no_topics_no_partitions(self, mock_kafka_client):
        args = mock.Mock(
            groupid="some_group",
            topic=None,
            partitions=None
        )
        expected_topics_dict = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1, 2, 3],
            "topic3": [0, 1],
        }
        with self.mock_get_topics() as mock_get_topics:
            topics_dict = OffsetManagerBase.preprocess_args(
                args.groupid,
                args.topic,
                args.partitions,
                mock.Mock(),
                mock_kafka_client
            )
            assert mock_get_topics.called
            assert topics_dict == expected_topics_dict

    def test_preprocess_args_wrong_topic(self, mock_kafka_client):
        args = mock.Mock(
            groupid="some_group",
            topic="topic34",
            partitions=None
        )
        with self.mock_get_topics() as mock_get_topics, mock.patch.object(
            sys,
            "exit",
            autospec=True,
        ) as mock_exit:
            OffsetManagerBase.preprocess_args(
                args.groupid,
                args.topic,
                args.partitions,
                mock.Mock(),
                mock_kafka_client
            )
            assert mock_get_topics.called
            assert mock_exit.called

    def test_preprocess_args_partitions_without_topic(self, mock_kafka_client):
        args = mock.Mock(
            groupid="some_group",
            topic=None,
            partitions=[0, 1, 2]
        )

        with self.mock_get_topics(), mock.patch.object(
            sys,
            "exit",
            autospec=True,
        ) as mock_exit:
            OffsetManagerBase.preprocess_args(
                args.groupid,
                args.topic,
                args.partitions,
                mock.Mock(),
                mock_kafka_client
            )
            assert mock_exit.called
