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

from kafka.common import PartitionMetadata
from pytest import fixture

from kafka_utils.kafka_check.commands.replication_factor import _find_topics_with_wrong_rp
from kafka_utils.kafka_check.commands.replication_factor import _prepare_output


TOPICS_STATE = {
    'topic_0': {
        0: PartitionMetadata(
            topic='topic_0',
            partition=0,
            leader=170396635,
            replicas=(170396635, 170398981, 170396665),
            isr=(170398981, 170396635),
            error=0,
        ),
    },
    'topic_1': {
        0: PartitionMetadata(
            topic='topic_1',
            partition=0,
            leader=170396635,
            replicas=(170396635, 170398981),
            isr=(170396635, 170398981),
            error=0,
        ),
    },
}

TOPICS_WITH_WRONG_RP = [
    {
        'min_isr': 3,
        'topic': 'topic_0',
        'replication_factor': 3,
    },
    {
        'min_isr': 3,
        'topic': 'topic_1',
        'replication_factor': 2,
    },
]


@fixture
def mock_zk():
    return mock.Mock()


def test_find_topics_with_wrong_rp_empty():
    result = _find_topics_with_wrong_rp(
        topics={},
        zk=None,
        default_min_isr=None,
    )

    assert result == []


@mock.patch(
    'kafka_utils.kafka_check.commands.replication_factor.get_min_isr',
    return_value=1,
    autospec=True,
)
def test_find_topics_with_wrong_rp_ok(mock_min_isr, mock_zk):
    result = _find_topics_with_wrong_rp(
        topics=TOPICS_STATE,
        zk=mock_zk,
        default_min_isr=None,
    )

    calls = [mock.call(mock_zk, 'topic_0'), mock.call(mock_zk, 'topic_1')]
    mock_min_isr.assert_has_calls(calls, any_order=True)
    assert result == []


@mock.patch(
    'kafka_utils.kafka_check.commands.replication_factor.get_min_isr',
    return_value=None,
    autospec=True,
)
def test_find_topics_with_wrong_rp_without_min_isr_in_zk_use_default(mock_min_isr, mock_zk):
    result = _find_topics_with_wrong_rp(
        topics=TOPICS_STATE,
        zk=mock_zk,
        default_min_isr=1,
    )

    calls = [mock.call(mock_zk, 'topic_0'), mock.call(mock_zk, 'topic_1')]
    mock_min_isr.assert_has_calls(calls, any_order=True)
    assert result == []


@mock.patch(
    'kafka_utils.kafka_check.commands.replication_factor.get_min_isr',
    return_value=None,
    autospec=True,
)
def test_find_topics_with_wrong_rp_not_empty_with_default_min_isr(mock_min_isr, mock_zk):
    result = _find_topics_with_wrong_rp(
        topics=TOPICS_STATE,
        zk=mock_zk,
        default_min_isr=2,
    )
    topic1 = {
        'replication_factor': 2,
        'min_isr': 2,
        'topic': 'topic_1',
    }

    calls = [mock.call(mock_zk, 'topic_0'), mock.call(mock_zk, 'topic_1')]
    mock_min_isr.assert_has_calls(calls, any_order=True)
    assert result == [topic1]


@mock.patch(
    'kafka_utils.kafka_check.commands.replication_factor.get_min_isr',
    return_value=3,
    autospec=True,
)
def test_find_topics_with_wrong_rp_returns_all_topics(mock_min_isr, mock_zk):
    result = _find_topics_with_wrong_rp(
        topics=TOPICS_STATE,
        zk=mock_zk,
        default_min_isr=1,
    )

    calls = [mock.call(mock_zk, 'topic_0'), mock.call(mock_zk, 'topic_1')]
    mock_min_isr.assert_has_calls(calls, any_order=True)

    def dict_comparator(d):
        return sorted(d.items())

    assert sorted(result, key=dict_comparator) == sorted(TOPICS_WITH_WRONG_RP, key=dict_comparator)


def test_prepare_output_ok_no_verbose():
    expected = {
        'message': 'All topics have proper replication factor.',
        'raw': {
            'topics_with_wrong_replication_factor_count': 0,
        }
    }
    assert _prepare_output([], False, -1) == expected


def test_prepare_output_ok_verbose():
    expected = {
        'message': 'All topics have proper replication factor.',
        'raw': {
            'topics_with_wrong_replication_factor_count': 0,
            'topics': [],
        }
    }
    assert _prepare_output([], True, -1) == expected


def test_prepare_output_critical_no_verbose():
    expected = {
        'message': '2 topic(s) have replication factor lower than specified min ISR + 1.',
        'raw': {
            'topics_with_wrong_replication_factor_count': 2,
        }
    }
    assert _prepare_output(TOPICS_WITH_WRONG_RP, False, -1) == expected


def test_prepare_output_critical_verbose():
    expected = {
        'message': '2 topic(s) have replication factor lower than specified min ISR + 1.',
        'verbose': (
            "Topics:\n"
            "replication_factor=3 is lower than min_isr=3 + 1 for topic_0\n"
            "replication_factor=2 is lower than min_isr=3 + 1 for topic_1"
        ),
        'raw': {
            'topics_with_wrong_replication_factor_count': 2,
            'topics': [
                {
                    'min_isr': 3,
                    'topic': 'topic_0',
                    'replication_factor': 3,
                },
                {
                    'min_isr': 3,
                    'topic': 'topic_1',
                    'replication_factor': 2,
                }
            ],
        }
    }
    assert _prepare_output(TOPICS_WITH_WRONG_RP, True, -1) == expected


def test_prepare_output_critical_verbose_with_head_limit():
    expected = {
        'message': '2 topic(s) have replication factor lower than specified min ISR + 1.',
        'verbose': (
            "Top 1 topics:\n"
            "replication_factor=3 is lower than min_isr=3 + 1 for topic_0"
        ),
        'raw': {
            'topics_with_wrong_replication_factor_count': 2,
            'topics': [
                {
                    'min_isr': 3,
                    'topic': 'topic_0',
                    'replication_factor': 3,
                },
            ],
        }
    }
    assert _prepare_output(TOPICS_WITH_WRONG_RP, True, 1) == expected
