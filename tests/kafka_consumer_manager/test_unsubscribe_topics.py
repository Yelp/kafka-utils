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

from kafka_utils.kafka_consumer_manager.commands. \
    unsubscribe_topics import KafkaUnsubscriber
from kafka_utils.kafka_consumer_manager.commands. \
    unsubscribe_topics import UnsubscribeTopics


class TestUnsubscribeTopics(object):

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.KafkaToolClient',
            autospec=True,
        ) as mock_client:
            yield mock_client

    @pytest.yield_fixture
    def zk(self):
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.ZK',
            autospec=True
        ) as mock_zk:
            yield mock_zk

    @pytest.yield_fixture
    def kafka_unsubscriber(self):
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.KafkaUnsubscriber',
            autospec=True,
        ) as mock_kafka_unsub:
            yield mock_kafka_unsub

    topics_partitions = {
        'topic1': [0, 1, 2],
        'topic2': [0, 1],
        'topic3': [0, 1, 2, 3],
    }

    cluster_config = mock.Mock(zookeeper='some_ip')

    def test_run_unsubscribe_kafka(self, client, zk, kafka_unsubscriber):
        with mock.patch.object(
            UnsubscribeTopics,
            'preprocess_args',
            spec=UnsubscribeTopics.preprocess_args,
            return_value=self.topics_partitions,
        ):
            args = mock.Mock(topic=None, topics=None)

            UnsubscribeTopics.run(args, self.cluster_config)

            kafka_obj = kafka_unsubscriber.return_value

            assert kafka_obj.unsubscribe_topics.call_count == 1

    def test_run_unsubscribe_only_topics(self, client, zk, kafka_unsubscriber):
        with mock.patch.object(
            UnsubscribeTopics,
            'preprocess_args',
            spec=UnsubscribeTopics.preprocess_args,
            return_value=self.topics_partitions,
        ):
            args = mock.Mock(topics=["topic1", "topic3"], topic=None, partitions=None)

            UnsubscribeTopics.run(args, self.cluster_config)

    def test_unsubscribe_topic(self, client):
        offsets = {'topic1': {0: 100}}
        new_offsets = {'topic1': {0: 0}}

        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.get_current_consumer_offsets',
            autospec=True,
            return_value=offsets,
        ) as mock_get, mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.set_consumer_offsets',
            autospec=True,
        ) as mock_set:
            unsubscriber = KafkaUnsubscriber(client)
            unsubscriber.delete_topic('some_group', 'topic1')

            assert mock_get.call_count == 1
            assert mock_set.call_count == 1
            assert mock_set.call_args_list == [
                mock.call(client, 'some_group', new_offsets),
            ]
