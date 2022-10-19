# Copyright 2019 Yelp Inc.
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
from kafka_utils.kafka_check.commands.replica_unavailability import _prepare_output


UNAVAILABLE_REPLICAS = [
    ('Topic0', 0),
    ('Topic0', 1),
    ('Topic13', 13),
]

UNAVAILABLE_BROKERS = [123456, 987456]


def test_prepare_output_ok_no_verbose():
    expected = {
        'message': "All replicas available for communication.",
        'raw': {
            'replica_unavailability_count': 0,
        }
    }
    assert _prepare_output([], [], False, -1) == expected


def test_prepare_output_ok_verbose():
    expected = {
        'message': "All replicas available for communication.",
        'raw': {
            'replica_unavailability_count': 0,
            'partitions': [],
        }
    }
    assert _prepare_output([], [], True, -1) == expected


def test_prepare_output_critical_verbose():
    expected = {
        'message': "3 replicas unavailable for communication. Unavailable Brokers: 123456, 987456",
        'verbose': (
            "Partitions:\n"
            "Topic0:0\n"
            "Topic0:1\n"
            "Topic13:13"
        ),
        'raw': {
            'replica_unavailability_count': 3,
            'partitions': [
                {'partition': 0, 'topic': 'Topic0'},
                {'partition': 1, 'topic': 'Topic0'},
                {'partition': 13, 'topic': 'Topic13'},
            ],
        }
    }
    assert _prepare_output(UNAVAILABLE_REPLICAS, UNAVAILABLE_BROKERS, True, -1) == expected


def test_prepare_output_critical_verbose_with_head():
    expected = {
        'message': "3 replicas unavailable for communication. Unavailable Brokers: 123456, 987456",
        'verbose': (
            "Top 2 partitions:\n"
            "Topic0:0\n"
            "Topic0:1"
        ),
        'raw': {
            'replica_unavailability_count': 3,
            'partitions': [
                {'partition': 0, 'topic': 'Topic0'},
                {'partition': 1, 'topic': 'Topic0'},
            ],
        }
    }
    assert _prepare_output(UNAVAILABLE_REPLICAS, UNAVAILABLE_BROKERS, True, 2) == expected
