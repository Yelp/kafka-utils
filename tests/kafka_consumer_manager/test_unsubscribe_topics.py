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
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import ZookeeperError

from kafka_utils.kafka_consumer_manager.commands. \
    unsubscribe_topics import KafkaUnsubscriber
from kafka_utils.kafka_consumer_manager.commands. \
    unsubscribe_topics import UnsubscribeTopics
from kafka_utils.kafka_consumer_manager.commands. \
    unsubscribe_topics import ZookeeperUnsubscriber


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
    def zk_unsubscriber(self):
        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.unsubscribe_topics.ZookeeperUnsubscriber',
            autospec=True,
        ) as mock_zk_unsub:
            yield mock_zk_unsub

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
    }

    cluster_config = mock.Mock(zookeeper='some_ip')

    def test_run_unsubscribe_zk(self, client, zk, zk_unsubscriber, kafka_unsubscriber):
        with mock.patch.object(
                UnsubscribeTopics,
                'preprocess_args',
                spec=UnsubscribeTopics.preprocess_args,
                return_value=self.topics_partitions,
        ):
            args = mock.Mock(storage='zookeeper')

            UnsubscribeTopics.run(args, self.cluster_config)

            zk_obj = zk_unsubscriber.return_value
            kafka_obj = kafka_unsubscriber.return_value

            assert zk_obj.unsubscribe_topic.call_count == 1
            assert kafka_obj.unsubscribe_topic.call_count == 0

    def test_run_unsubscribe_kafka(self, client, zk, zk_unsubscriber, kafka_unsubscriber):
        with mock.patch.object(
                UnsubscribeTopics,
                'preprocess_args',
                spec=UnsubscribeTopics.preprocess_args,
                return_value=self.topics_partitions,
        ):
            args = mock.Mock(storage='kafka')

            UnsubscribeTopics.run(args, self.cluster_config)

            zk_obj = zk_unsubscriber.return_value
            kafka_obj = kafka_unsubscriber.return_value

            assert zk_obj.unsubscribe_topic.call_count == 0
            assert kafka_obj.unsubscribe_topic.call_count == 1

    def test_unsubscribe_some_partitions_left(self, zk):
        zk_obj = zk.return_value
        zk_obj.get_my_subscribed_partitions.return_value = [3]

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        unsubscriber.unsubscribe_topic(
            'some_group',
            'topic1',
            [0, 1, 2],
            self.topics_partitions,
        )

        calls = [
            mock.call(
                'some_group',
                'topic1',
                [0, 1, 2],
            ),
        ]

        assert zk_obj.delete_topic_partitions.call_args_list == calls
        # Delete topic should not be called because the group is still
        # subscribed to some topic partitions
        assert not zk_obj.delete_topic.called

    def test_unsubscribe_wipe_all_partitions(self, zk):
        zk_obj = zk.return_value
        zk_obj.get_my_subscribed_partitions.return_value = []

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        unsubscriber.unsubscribe_topic(
            'some_group',
            'topic1',
            [0, 1, 2],
            self.topics_partitions,
        )

        calls = [
            mock.call(
                'some_group',
                'topic1',
                [0, 1, 2],
            ),
        ]

        assert zk_obj.delete_topic_partitions.call_args_list == calls
        assert zk_obj.delete_topic.call_args_list == [
            mock.call('some_group', 'topic1'),
        ]

    def test_unsubscribe_wipe_default_partitions(self, zk):
        zk_obj = zk.return_value
        zk_obj.get_my_subscribed_partitions.return_value = []

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        unsubscriber.unsubscribe_topic(
            'some_group',
            'topic1',
            None,
            self.topics_partitions,
        )

        assert zk_obj.delete_topic_partitions.call_count == 0
        assert zk_obj.delete_topic.call_args_list == [
            mock.call('some_group', 'topic1'),
        ]

    def test_unsubscribe_wipe_default_topics(self, zk):
        zk_obj = zk.return_value
        zk_obj.get_my_subscribed_partitions.return_value = []

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        unsubscriber.unsubscribe_topic(
            'some_group',
            None,
            None,
            self.topics_partitions,
        )

        assert zk_obj.delete_topic_partitions.call_count == 0
        assert sorted(zk_obj.delete_topic.call_args_list) == sorted(
            [
                mock.call('some_group', 'topic1'),
                mock.call('some_group', 'topic2'),
            ],
        )

    def test_unsubscribe_no_node_error(self, zk):
        zk_obj = zk.return_value
        zk_obj.delete_topic_partitions.side_effect = NoNodeError("Boom!")

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        unsubscriber.unsubscribe_topic(
            'some_group',
            'topic1',
            [0, 1, 2],
            self.topics_partitions,
        )

        assert zk_obj.delete_topic_partitions.called

    def test_unsubscribe_any_other_exception(self, zk):
        zk_obj = zk.return_value
        zk_obj.delete_topic_partitions.side_effect = ZookeeperError("Boom!")

        unsubscriber = ZookeeperUnsubscriber(zk_obj)
        with pytest.raises(ZookeeperError):
            unsubscriber.unsubscribe_topic(
                'some_group',
                'topic1',
                [0, 1, 2],
                self.topics_partitions,
            )

    def test_unsubscribe_topic_kafka_storage(self, client):
        offsets = {'topic1': {0: 100}}
        new_offsets = {'topic1': {0: -1}}

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
                mock.call(
                    client,
                    'some_group',
                    new_offsets,
                    offset_storage='kafka',
                ),
            ]
