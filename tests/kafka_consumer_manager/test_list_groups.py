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
from unittest import mock

from kafka_utils.kafka_consumer_manager. \
    commands.list_groups import ListGroups


class TestListGroups:

    @contextlib.contextmanager
    def mock_kafka_info(self):
        with mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "commands.list_groups.get_kafka_group_reader",
            autospec=True
        ) as mock_kafka_reader:
            yield mock_kafka_reader

    def test_get_kafka_groups(self):
        with self.mock_kafka_info() as (mock_kafka_reader):
            expected_groups = {
                'group_name': ['topic1', 'topic2'],
                'group2': ['topic3', 'topic4']
            }

            m = mock_kafka_reader.return_value
            m.read_groups.return_value = expected_groups

            cluster_config = mock.Mock(zookeeper='some_ip', type='some_cluster_type')
            cluster_config.configure_mock(name='some_cluster_name')

            result = ListGroups.get_kafka_groups(cluster_config)
            assert result == list(expected_groups.keys())
            assert m.read_groups.call_count == 1

    @mock.patch("kafka_utils.kafka_consumer_manager.commands.list_groups.print", create=True)
    def test_print_groups(self, mock_print):
        groups = ['group1', 'group2', 'group3']

        cluster_config = mock.Mock(zookeeper='some_ip', type='some_cluster_type')
        cluster_config.configure_mock(name='some_cluster_name')

        expected_print = [
            mock.call("Consumer Groups:"),
            mock.call("\tgroup1"),
            mock.call("\tgroup2"),
            mock.call("\tgroup3"),
            mock.call("3 groups found for cluster some_cluster_name "
                      "of type some_cluster_type"),
        ]

        ListGroups.print_groups(groups, cluster_config)
        assert mock_print.call_args_list == expected_print
