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

from kafka_utils.kafka_consumer_manager.commands. \
    delete_group import DeleteGroup


class TestDeleteGroup:

    @pytest.yield_fixture
    def client(self):
        with mock.patch(
                'kafka_utils.kafka_consumer_manager.'
                'commands.delete_group.KafkaToolClient',
                autospec=True,
        ) as mock_client:
            yield mock_client

    @pytest.yield_fixture
    def offsets(self):
        yield {'topic1': {0: 100}}

    def test_delete_topic(self, client, offsets):
        new_offsets = {'topic1': {0: 0}}

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
                ),
            ]
