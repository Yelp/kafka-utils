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

import sys
from collections import defaultdict

import mock
import pytest

from kafka_utils.kafka_consumer_manager. \
    commands.offset_set import OffsetSet
from kafka_utils.util.error import OffsetCommitError


class TestOffsetSet(object):

    def test_topics_dict(self):
        offset_update_tuple = "topic1.23=1000"
        expected_topics_dict = {
            "topic1": {23: 1000},
        }
        OffsetSet.topics_dict(offset_update_tuple)
        assert OffsetSet.new_offsets_dict == expected_topics_dict

    def test_topics_dict_topic_with_period(self):
        OffsetSet.new_offsets_dict = defaultdict(dict)
        offset_update_tuple = "scribe.sfo2.ranger.12=200"
        expected_topics_dict = {
            "scribe.sfo2.ranger": {12: 200},
        }
        OffsetSet.topics_dict(offset_update_tuple)
        assert OffsetSet.new_offsets_dict == expected_topics_dict

    def test_topics_dict_invalid_input(self):
        offset_update_tuple = "topic1.23.1000"
        with mock.patch.object(sys, "exit", autospec=True) as mock_exit:
            OffsetSet.topics_dict(offset_update_tuple)
            mock_exit.assert_called_once_with(1)

        offset_update_tuple = "topic1.garbage=garbage"
        with mock.patch.object(sys, "exit", autospec=True) as mock_exit:
            OffsetSet.topics_dict(offset_update_tuple)
            mock_exit.assert_called_once_with(1)

    @mock.patch(
        'kafka_utils.kafka_consumer_manager.'
        'commands.offset_set.KafkaToolClient',
        autospec=True,
    )
    def test_run(self, mock_client):
        OffsetSet.new_offsets_dict = {
            "topic1": {
                0: 1000,
                1: 2000,
                2: 3000,
            },
            "topic2": {
                0: 100,
                1: 200,
            },
        }

        with mock.patch.object(
            OffsetSet,
            'get_topics_from_consumer_group_id',
            spec=OffsetSet.get_topics_from_consumer_group_id,
        ), mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "commands.offset_set.set_consumer_offsets",
            return_value=[],
            autospec=True
        ) as mock_set_offsets:
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock()
            OffsetSet.run(args, cluster_config)

            mock_client.return_value.load_metadata_for_topics. \
                assert_called_once_with()
            mock_client.return_value.close.assert_called_once_with()
            ordered_args, _ = mock_set_offsets.call_args
            assert ordered_args[1] == args.groupid
            assert ordered_args[2] == OffsetSet.new_offsets_dict

    @mock.patch(
        'kafka_utils.kafka_consumer_manager.'
        'commands.offset_set.KafkaToolClient',
        autospec=True,
    )
    def test_run_error_committing_offsets(self, mock_client):
        OffsetSet.new_offsets_dict = {
            "topic1": {
                0: 1000,
                1: 2000,
            },
            "topic2": {
                0: 100,
            },
        }

        with mock.patch.object(
            OffsetSet,
            'get_topics_from_consumer_group_id',
            spec=OffsetSet.get_topics_from_consumer_group_id,
        ), mock.patch(
            "kafka_utils.kafka_consumer_manager."
            "commands.offset_set.set_consumer_offsets",
            return_value=[
                OffsetCommitError("topic1", 1, "my_error 1"),
                OffsetCommitError("topic2", 0, "my_error 2"),
            ],
            autospec=True
        ) as mock_set_offsets, mock.patch.object(
            sys,
            "exit",
            autospec=True,
        ) as mock_exit:
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock()
            OffsetSet.run(args, cluster_config)

            mock_client.return_value.load_metadata_for_topics. \
                assert_called_once_with()
            mock_client.return_value.close.assert_called_once_with()
            ordered_args, _ = mock_set_offsets.call_args
            assert ordered_args[1] == args.groupid
            assert ordered_args[2] == OffsetSet.new_offsets_dict
            mock_exit.assert_called_with(1)

    @mock.patch(
        'kafka_utils.kafka_consumer_manager.'
        'commands.offset_set.KafkaToolClient',
        autospec=True,
    )
    def test_run_bad_topics_dict(self, mock_client):
        OffsetSet.new_offsets_dict = {
            "topic1": 23,
            "topic2": 32,
        }
        with mock.patch.object(
            OffsetSet,
            'get_topics_from_consumer_group_id',
            spec=OffsetSet.get_topics_from_consumer_group_id,
        ):
            args = mock.Mock(
                groupid="some_group",
                topic=None,
                partitions=None
            )
            cluster_config = mock.Mock()
            with pytest.raises((TypeError, AttributeError)):
                OffsetSet.run(args, cluster_config)

    def test_merge_current_new_offsets_dict(self):
        new_offsets = {
            "topic1": {1: 23},
            "topic2": {4: 56},
        }

        current_offsets = {
            "topic1": {1: 99},
            "topic2": {2: 22, 4: 12},
            "topic3": {0: 33},
        }

        expected = {
            "topic1": {1: 23},
            "topic2": {4: 56, 2: 22},
            "topic3": {0: 33},
        }

        actual = OffsetSet.merge_current_new_offsets_dict(current_offsets, new_offsets)
        assert expected == actual

    def test_merge_current_new_offsets_dict_all_new(self):
        new_offsets = {
            "topic1": {1: 23},
            "topic2": {4: 56},
        }

        current_offsets = {}

        expected = {
            "topic1": {1: 23},
            "topic2": {4: 56},
        }

        actual = OffsetSet.merge_current_new_offsets_dict(current_offsets, new_offsets)
        assert expected == actual

    def test_merge_current_new_offsets_dict_all_current(self):
        new_offsets = {}

        current_offsets = {
            "topic1": {1: 99},
            "topic2": {2: 22, 4: 12},
            "topic3": {0: 33},
        }

        expected = {
            "topic1": {1: 99},
            "topic2": {2: 22, 4: 12},
            "topic3": {0: 33},
        }

        actual = OffsetSet.merge_current_new_offsets_dict(current_offsets, new_offsets)
        assert expected == actual

    def test_split_zero_nonzero_offsets_dict(self):
        new_offsets_dict = {
            "topic1": {
                0: 1000,
                1: 2000,
                2: 3000,
                3: 0,
                4: 0,
            },
            "topic2": {
                0: 100,
                1: 200,
            },
            "topic3": {
                0: 0,
            },
        }

        expected_zero = {
            "topic1": {
                3: 0,
                4: 0,
            },
            "topic3": {
                0: 0,
            },
        }

        expected_nonzero = {
            "topic1": {
                0: 1000,
                1: 2000,
                2: 3000,
            },
            "topic2": {
                0: 100,
                1: 200,
            },
        }

        actual_zero, actual_nonzero = OffsetSet.split_zero_nonzero_offsets_dict(new_offsets_dict)

        assert expected_zero == actual_zero
        assert expected_nonzero == actual_nonzero

    def test_split_zero_nonzero_offsets_dict_all_zero(self):
        expected_zero = {
            "topic1": {
                0: 0,
                1: 0,
            },
        }
        expected_nonzero = {}
        actual_zero, actual_nonzero = OffsetSet.split_zero_nonzero_offsets_dict(expected_zero)
        assert expected_zero == actual_zero
        assert expected_nonzero == actual_nonzero

    def test_split_zero_nonzero_offsets_dict_all_nonzero(self):
        expected_nonzero = {
            "topic1": {
                0: 100,
                1: 200,
            },
        }
        expected_zero = {}
        actual_zero, actual_nonzero = OffsetSet.split_zero_nonzero_offsets_dict(expected_nonzero)
        assert expected_zero == actual_zero
        assert expected_nonzero == actual_nonzero

    @mock.patch("kafka_utils.kafka_consumer_manager.commands.offset_set.get_current_consumer_offsets", autospec=True)
    @mock.patch.object(OffsetSet, "get_topics_from_consumer_group_id", spec=OffsetSet.get_topics_from_consumer_group_id)
    @mock.patch("kafka_utils.kafka_consumer_manager.commands.offset_set.KafkaToolClient", autospec=True)
    @mock.patch(
        "kafka_utils.kafka_consumer_manager.commands.offset_set.set_consumer_offsets",
        autospec=True,
        return_value=[],
    )
    def test_run_zero_offsets(self, mock_set_offsets, mock_client, mock_get_topics, mock_get_current):
        OffsetSet.new_offsets_dict = {
            "topic1": {
                0: 1000,
                1: 2000,
                2: 3000,
                3: 0,
                4: 0,
            },
            "topic2": {
                0: 100,
                1: 200,
            },
            "topic3": {
                0: 0,
            },
        }

        args = mock.Mock(
            groupid="some_group",
            topic=None,
            partitions=None
        )
        cluster_config = mock.Mock()
        mock_get_topics.return_value = ["topic1", "topic2", "topic3", "topic4"]
        mock_get_current.return_value = {
            "topic1": {
                0: 0,
                3: 3000,
                5: 5000,
            },
            "topic3": {
                0: 0,
            },
        }
        expected_zero_offsets_dict = {
            "topic1": {
                3: 0,
                4: 0,
            },
            "topic3": {
                0: 0,
            },
        }
        expected_nonzero_offsets_dict = {
            "topic1": {
                0: 1000,
                1: 2000,
                2: 3000,
                5: 5000,
            },
            "topic2": {
                0: 100,
                1: 200,
            },
        }
        OffsetSet.run(args, cluster_config)

        mock_client.return_value.load_metadata_for_topics.assert_called_once_with()
        mock_client.return_value.close.assert_called_once_with()
        # Check we filtered out topics that we aren't commiting zero offsets to
        assert mock_get_current.call_args[0][2] == ["topic1", "topic3"]
        # Check we first called set_offsets with zero offsets, and then with nonzero
        assert mock_set_offsets.call_count == 2
        first_call, _ = mock_set_offsets.call_args_list[0]
        second_call, _ = mock_set_offsets.call_args_list[1]

        assert first_call[1] == args.groupid
        assert first_call[2] == expected_zero_offsets_dict

        assert second_call[1] == args.groupid
        assert second_call[2] == expected_nonzero_offsets_dict
