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

import mock
import pytest
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import ZookeeperError

from kafka_tools.kafka_consumer_manager.commands. \
    unsubscribe_topics import UnsubscribeTopics


@mock.patch('kafka_tools.kafka_consumer_manager.'
            'commands.unsubscribe_topics.KafkaClient')
class TestUnsubscribeTopics(object):
    topics_partitions = {
        "topic1": [0, 1, 2],
        "topic2": [0, 1],
    }

    cluster_config = mock.Mock(zookeeper='some_ip')

    @contextlib.contextmanager
    def mock_kafka_info(self):
        with mock.patch.object(
            UnsubscribeTopics,
            "preprocess_args",
            spec=UnsubscribeTopics.preprocess_args,
            return_value=self.topics_partitions,
        )as mock_writer_process_args, mock.patch(
            'kafka_tools.kafka_consumer_manager.'
            'commands.unsubscribe_topics.ZK',
            autospec=True
        ) as mock_ZK:
            mock_ZK.return_value.__enter__.return_value = mock_ZK.return_value
            yield mock_writer_process_args, mock_ZK

    def test_run_some_partitions_left(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            args = mock.Mock(
                groupid="some_group",
                topic="topic1",
                partitions=[0, 1, 2]
            )
            mock_ZK.return_value.get_my_subscribed_partitions.return_value = [3]

            UnsubscribeTopics.run(args, self.cluster_config)

            calls = [
                mock.call(
                    args.groupid,
                    "topic1",
                    [0, 1, 2]
                ),
            ]

            obj = mock_ZK.return_value
            assert obj.delete_topic_partitions.call_args_list == calls
            # Delete topic should not be called because the group is still
            # subscribed to some topic partitions
            assert not obj.delete_topic.called

    def test_run_wipe_all_partitions(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            args = mock.Mock(
                groupid="some_group",
                topic="topic1",
                partitions=[0, 1, 2]
            )
            mock_ZK.return_value.get_my_subscribed_partitions.return_value = []

            UnsubscribeTopics.run(args, self.cluster_config)

            calls = [
                mock.call(
                    args.groupid,
                    "topic1",
                    [0, 1, 2]
                ),
            ]

            obj = mock_ZK.return_value
            assert obj.delete_topic_partitions.call_args_list == calls
            assert obj.delete_topic.call_args_list == [
                mock.call(args.groupid, "topic1"),
            ]

    def test_run_wipe_default_partitions(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            args = mock.Mock(
                groupid="some_group",
                topic="topic1",
                partitions=None
            )

            UnsubscribeTopics.run(args, self.cluster_config)

            obj = mock_ZK.return_value
            assert obj.delete_topic_partitions.call_count == 0
            assert obj.delete_topic.call_args_list == [
                mock.call(args.groupid, "topic1"),
            ]

    def test_run_wipe_default_topics(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )

            UnsubscribeTopics.run(args, self.cluster_config)

            obj = mock_ZK.return_value
            assert obj.delete_topic_partitions.call_count == 0
            assert sorted(obj.delete_topic.call_args_list) == sorted(
                [
                    mock.call(args.groupid, "topic1"),
                    mock.call(args.groupid, "topic2"),
                ],
            )

    def test_run_no_node_error(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            obj = mock_ZK.return_value
            obj.delete_topic_partitions.side_effect = NoNodeError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                topic="topic1",
                partitions=[0, 1, 2]
            )

            UnsubscribeTopics.run(args, self.cluster_config)
            assert mock_ZK.return_value.delete_topic_partitions.called

    def test_run_any_other_exception(self, mock_client):
        with self.mock_kafka_info() as (mock_writer_process_args, mock_ZK):
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            obj.delete_topic_partitions.side_effect = ZookeeperError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                topic="topic1",
                partitions=[0, 1, 2]
            )

            with pytest.raises(ZookeeperError):
                UnsubscribeTopics.run(args, self.cluster_config)
