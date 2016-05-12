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
from kazoo.exceptions import ZookeeperError

from kafka_tools.kafka_consumer_manager.commands. \
    delete_group import DeleteGroup


@mock.patch('kafka_tools.kafka_consumer_manager.'
            'commands.delete_group.KafkaClient')
class TestDeleteGroup(object):

    @contextlib.contextmanager
    def mock_kafka_info(self):
        with mock.patch(
            'kafka_tools.kafka_consumer_manager.'
            'commands.delete_group.ZK',
            autospec=True
        ) as mock_ZK:
            mock_ZK.return_value.__enter__.return_value = mock_ZK.return_value
            yield mock_ZK

    def test_run_wipe_delete_group(self, mock_client):
        with self.mock_kafka_info() as mock_ZK:
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            DeleteGroup.run(args, cluster_config)

            obj = mock_ZK.return_value
            assert obj.delete_group.call_args_list == [
                mock.call(args.groupid),
            ]

    def test_run_wipe_delete_group_error(self, mock_client):
        with self.mock_kafka_info() as mock_ZK:
            obj = mock_ZK.return_value.__enter__.return_value
            obj.__exit__.return_value = False
            obj.delete_group.side_effect = ZookeeperError("Boom!")
            args = mock.Mock(
                groupid="some_group",
                storage="zookeeper",
            )
            cluster_config = mock.Mock(zookeeper='some_ip')

            with pytest.raises(ZookeeperError):
                DeleteGroup.run(args, cluster_config)
