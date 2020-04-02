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

from collections import namedtuple

import mock
import pytest
from kazoo.exceptions import NoNodeError

from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.serialization import dump_json
from kafka_utils.util.zookeeper import ZK


MockGetTopics = namedtuple('MockGetTopics', ['ctime'])


@mock.patch(
    'kafka_utils.util.zookeeper.KazooClient',
    autospec=True
)
class TestZK(object):
    cluster_config = ClusterConfig(
        type='mytype',
        name='some_cluster',
        broker_list='some_list',
        zookeeper='some_ip'
    )

    def test_create(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.create(
                '/kafka/consumers/some_group/offsets'
            )
            zk.create(
                '/kafka/consumers/some_group/offsets',
                value='some_val',
                acl=None,
                ephemeral=True,
                sequence=True,
                makepath=True
            )
            mock_obj = mock.Mock()
            zk.create(
                '/kafka/consumers/some_group/offsets',
                value='some_val',
                acl=mock_obj,
            )

            call_list = [
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    '', None, False, False, False
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    'some_val', None, True, True, True
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    'some_val', mock_obj, False, False, False
                ),
            ]
            assert mock_client.return_value.create.call_args_list == call_list

    def test_set(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.set(
                'config/topics/some_topic',
                'some_val'
            )
            zk.set(
                'brokers/topics/some_topic',
                '{"name": "some_topic", "more": "properties"}'
            )
            call_list = [
                mock.call(
                    'config/topics/some_topic',
                    'some_val'
                ),
                mock.call(
                    'brokers/topics/some_topic',
                    '{"name": "some_topic", "more": "properties"}'
                )
            ]
            assert mock_client.return_value.set.call_args_list == call_list

    def test_delete(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.delete(
                '/kafka/consumers/some_group/offsets',
            )
            zk.delete(
                '/kafka/consumers/some_group/offsets',
                recursive=True
            )
            call_list = [
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    recursive=False
                ),
                mock.call(
                    '/kafka/consumers/some_group/offsets',
                    recursive=True
                ),
            ]
            assert mock_client.return_value.delete.call_args_list == call_list

    def test_delete_topic(self, _):
        with mock.patch.object(
            ZK,
            'delete',
            autospec=True
        ) as mock_delete:
            with ZK(self.cluster_config) as zk:
                zk.delete_topic(
                    'some_group',
                    'some_topic',
                )
                mock_delete.assert_called_once_with(
                    zk,
                    '/consumers/some_group/offsets/some_topic',
                    True,
                )

    def test_get_my_subscribed_partitions(self, _):
        with mock.patch.object(
            ZK,
            'get_children',
            autospec=True,
        ) as mock_children:
            with ZK(self.cluster_config) as zk:
                zk.get_my_subscribed_partitions(
                    'some_group',
                    'some_topic',
                )
                mock_children.assert_called_once_with(
                    zk,
                    '/consumers/some_group/offsets/some_topic',
                )

    def test_get_topic_config(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.zk.get = mock.Mock(
                return_value=(
                    b'{"version": 1, "config": {"cleanup.policy": "compact"}}',
                    "Random node info that doesn't matter"
                )
            )
            actual = zk.get_topic_config("some_topic")
            expected = {"version": 1, "config": {"cleanup.policy": "compact"}}
            assert actual == expected

    def test_get_topic_config_8(self, mock_client):
        """
        Test getting configuration for topics created in Kafa prior to 0.9.0.
        """

        with ZK(self.cluster_config) as zk:
            zk.zk.get = mock.Mock(side_effect=NoNodeError())
            zk.get_topics = mock.Mock(return_value={"some_topic": {}})
            actual = zk.get_topic_config("some_topic")
            expected = {"config": {}}
            assert actual == expected

    def test_get_nonexistent_topic_config(self, mock_client):
        """
        Test getting configuration for topics that don't exist.
        """

        with ZK(self.cluster_config) as zk:
            zk.zk.get = mock.Mock(side_effect=NoNodeError())
            zk.get_topics = mock.Mock(return_value={})
            with pytest.raises(NoNodeError):
                zk.get_topic_config("some_topic")

    def test_set_topic_config_kafka_10(self, mock_client):
        with mock.patch.object(
            ZK,
            'set',
            autospec=True
        ) as mock_set:
            with ZK(self.cluster_config) as zk:
                config = {"version": 1, "config": {"cleanup.policy": "compact"}}
                config_change = {"entity_path": "topics/some_topic", "version": 2}

                zk.set_topic_config(
                    "some_topic",
                    config,
                )

                serialized_config = dump_json(config)
                serialized_config_change = dump_json(config_change)

                mock_set.assert_called_once_with(
                    zk,
                    '/config/topics/some_topic',
                    serialized_config,
                )

                expected_create_call = mock.call(
                    '/config/changes/config_change_',
                    serialized_config_change,
                    None,
                    False,
                    True,
                    False
                )
                assert mock_client.return_value.create.call_args_list == [expected_create_call]

    def test_set_topic_config_kafka_9(self, mock_client):
        with mock.patch.object(
            ZK,
            'set',
            autospec=True
        ) as mock_set:
            with ZK(self.cluster_config) as zk:
                config = {"version": 1, "config": {"cleanup.policy": "compact"}}
                config_change = {"version": 1, "entity_type": "topics", "entity_name": "some_topic"}

                zk.set_topic_config(
                    "some_topic",
                    config,
                    (0, 9, 2)
                )

                serialized_config = dump_json(config)
                serialized_config_change = dump_json(config_change)

                mock_set.assert_called_once_with(
                    zk,
                    '/config/topics/some_topic',
                    serialized_config,
                )

                expected_create_call = mock.call(
                    '/config/changes/config_change_',
                    serialized_config_change,
                    None,
                    False,
                    True,
                    False
                )
                assert mock_client.return_value.create.call_args_list == [expected_create_call]

    def test_get_broker_config(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.zk.get = mock.Mock(
                return_value=(
                    b'{"version": 1, "config": {"leader.replication.throttled.rate": "42"}}',
                    "Random node info that doesn't matter"
                )
            )
            actual = zk.get_broker_config(0)
            expected = {"version": 1, "config": {"leader.replication.throttled.rate": "42"}}
            assert actual == expected

    def test_set_broker_config_kafka_10(self, mock_client):
        with mock.patch.object(
            ZK,
            'set',
            autospec=True
        ) as mock_set:
            with ZK(self.cluster_config) as zk:
                config = {"version": 1, "config": {"leader.replication.throttled.rate": "42"}}
                config_change = {"entity_path": "brokers/0", "version": 2}

                zk.set_broker_config(0, config)

                serialized_config = dump_json(config)
                serialized_config_change = dump_json(config_change)

                mock_set.assert_called_once_with(
                    zk,
                    '/config/brokers/0',
                    serialized_config,
                )

                expected_create_call = mock.call(
                    '/config/changes/config_change_',
                    serialized_config_change,
                    None,
                    False,
                    True,
                    False
                )
                assert mock_client.return_value.create.call_args_list == [expected_create_call]

    def test_get_topics(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.zk.get = mock.Mock(
                return_value=(
                    (
                        b'{"version": "1", "partitions": {"0": [1, 0]}}',
                        MockGetTopics(31000),
                    )
                )
            )

            zk._fetch_partition_state = mock.Mock(
                return_value=(
                    (
                        b'{"version": "2"}',
                        MockGetTopics(32000),
                    )
                )
            )

            actual_with_fetch_state = zk.get_topics("some_topic")
            expected_with_fetch_state = {
                'some_topic': {
                    'ctime': 31.0,
                    'partitions': {
                        '0': {
                            'replicas': [1, 0],
                            'ctime': 32.0,
                            'version': '2',
                        },
                    },
                    'version': '1',
                },
            }
            assert actual_with_fetch_state == expected_with_fetch_state

            zk._fetch_partition_info = mock.Mock(
                return_value=MockGetTopics(33000)
            )

            actual_without_fetch_state = zk.get_topics("some_topic", fetch_partition_state=False)
            expected_without_fetch_state = {
                'some_topic': {
                    'ctime': 31.0,
                    'partitions': {
                        '0': {
                            'replicas': [1, 0],
                            'ctime': 33.0,
                        },
                    },
                    'version': '1',
                },
            }
            assert actual_without_fetch_state == expected_without_fetch_state

    def test_get_topics_empty_cluster(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(side_effect=NoNodeError())
            actual_with_no_node_error = zk.get_topics()
            expected_with_no_node_error = {}
            zk.get_children.assert_called_with("/brokers/topics")
            assert actual_with_no_node_error == expected_with_no_node_error

    def test_get_brokers_names_only(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(
                return_value=[1, 2, 3],
            )
            expected = {1: None, 2: None, 3: None}
            actual = zk.get_brokers(names_only=True)
            zk.get_children.assert_called_with("/brokers/ids")
            assert actual == expected

    def test_get_brokers_with_metadata(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(
                return_value=[1, 2, 3],
            )
            zk.get_broker_metadata = mock.Mock(
                return_value='broker',
            )
            expected = {1: 'broker', 2: 'broker', 3: 'broker'}
            actual = zk.get_brokers()
            zk.get_children.assert_called_with("/brokers/ids")
            calls = zk.get_broker_metadata.mock_calls
            zk.get_broker_metadata.assert_has_calls(calls)
            assert actual == expected

    def test_get_brokers_empty_cluster(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(side_effect=NoNodeError())
            actual_with_no_node_error = zk.get_brokers()
            expected_with_no_node_error = {}
            zk.get_children.assert_called_with("/brokers/ids")
            assert actual_with_no_node_error == expected_with_no_node_error

    def test_get_brokers_with_metadata_for_ssl(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(
                return_value=[1],
            )

            zk.get = mock.Mock(
                return_value=(b'{"endpoints":["SSL://broker:9093"],"host":null}', None)
            )
            expected = {1: {'host': 'broker'}}
            actual = zk.get_brokers()
            assert actual[1]['host'] == expected[1]['host']

            zk.get = mock.Mock(
                return_value=(b'{"endpoints":["INTERNAL://broker:9093","EXTERNAL://broker:9093"],"host":null}', None)
            )
            expected = {1: {'host': 'broker'}}
            actual = zk.get_brokers()
            assert actual[1]['host'] == expected[1]['host']

    def test_get_brokers_with_metadata_for_sasl(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(
                return_value=[1],
            )

            zk.get = mock.Mock(
                return_value=(b'{"endpoints":["PLAINTEXTSASL://broker:9093"],"host":null}', None)
            )
            expected = {1: {'host': 'broker'}}
            actual = zk.get_brokers()
            assert actual[1]['host'] == expected[1]['host']

    def test_get_brokers_with_metadata_for_plaintext(self, mock_client):
        with ZK(self.cluster_config) as zk:
            zk.get_children = mock.Mock(
                return_value=[1],
            )

            zk.get = mock.Mock(
                return_value=(b'{"endpoints":[],"host":"broker"}', None)
            )
            expected = {1: {'host': 'broker'}}
            actual = zk.get_brokers()
            assert actual[1]['host'] == expected[1]['host']
