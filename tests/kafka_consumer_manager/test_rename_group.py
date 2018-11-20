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

import contextlib

import mock

from kafka_utils.kafka_consumer_manager. \
    commands.rename_group import RenameGroup


@mock.patch(
    'kafka_utils.kafka_consumer_manager.'
    'commands.rename_group.KafkaToolClient',
    autospec=True,
)
class TestRenameGroup(object):

    @contextlib.contextmanager
    def mock_kafka_info(self, topics_partitions):
        with mock.patch.object(
            RenameGroup,
            "preprocess_args",
            spec=RenameGroup.preprocess_args,
            return_value=topics_partitions,
        ) as mock_process_args, mock.patch(
            "kafka_utils.kafka_consumer_manager.util.prompt_user_input",
            autospec=True,
        ) as mock_user_confirm:
            yield mock_process_args, mock_user_confirm

    def test_run_with_kafka(self, mock_client):
        topics_partitions = {
            "topic1": [0, 1, 2],
            "topic2": [0, 1]
        }
        with self.mock_kafka_info(
            topics_partitions
        ) as (mock_process_args, mock_user_confirm),\
            mock.patch('kafka_utils.kafka_consumer_manager.commands.'
                       'rename_group.get_current_consumer_offsets',
                       autoapec=True) as mock_get_current_consumer_offsets,\
            mock.patch('kafka_utils.kafka_consumer_manager.commands.'
                       'rename_group.set_consumer_offsets',
                       autospec=True) as mock_set_consumer_offsets:
            cluster_config = mock.Mock(zookeeper='some_ip')
            args = mock.Mock(source_groupid='old_group', dest_groupid='new_group')
            RenameGroup.run(args, cluster_config)
            assert mock_set_consumer_offsets.call_count == 2
            assert mock_get_current_consumer_offsets.call_count == 1
