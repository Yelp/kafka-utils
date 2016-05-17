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
from kazoo.exceptions import ZookeeperError

from kafka_utils.kafka_consumer_manager.commands. \
    delete_group import DeleteGroup


class TestDeleteGroup(object):

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.'
                'commands.delete_group.KafkaToolClient',
                autospec=True,
        ) as mock_client:
            yield mock_client

    @pytest.yield_fixture
    def zk(self):
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.'
                'commands.delete_group.ZK',
                autospec=True
        ) as mock_zk:
            mock_zk.return_value.__enter__.return_value = mock_zk.return_value
            yield mock_zk

    @pytest.yield_fixture
    def offsets(self):
        yield {'topic1': {0: 100}}

    def test_run_wipe_delete_group(self, client, zk, offsets):
        with mock.patch.object(
                DeleteGroup,
                'preprocess_args',
                spec=DeleteGroup.preprocess_args,
                return_value=offsets,
        ):
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            DeleteGroup.run(args, cluster_config)

            obj = zk.return_value
            assert obj.delete_group.call_args_list == [
                mock.call(args.groupid),
            ]

    def test_run_wipe_delete_group_error(self, client, zk, offsets):
        with mock.patch.object(
                DeleteGroup,
                'preprocess_args',
                spec=DeleteGroup.preprocess_args,
                return_value=offsets,
        ):
            obj = zk.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            obj.delete_group.side_effect = ZookeeperError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            with pytest.raises(ZookeeperError):
                DeleteGroup.run(args, cluster_config)

    def test_delete_topic_kafka_storage(self, client, offsets):
        new_offsets = {'topic1': {0: -1}}

        with mock.patch(
            'kafka_utils.kafka_consumer_manager.'
            'commands.delete_group.set_consumer_offsets',
            autospec=True,
        ) as mock_set:
            DeleteGroup.delete_group_kafka(client, 'some_group', offsets)

            assert mock_set.call_count == 1
            assert mock_set.call_args_list == [
                mock.call(
                    client,
                    'some_group',
                    new_offsets,
                    offset_storage='kafka',
                ),
            ]
