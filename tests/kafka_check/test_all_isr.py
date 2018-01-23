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

from kafka.common import PartitionMetadata

from kafka_utils.kafka_check.commands.all_isr import _prepare_output
from kafka_utils.kafka_check.commands.all_isr import _process_metadata_response


TOPICS_STATE = {
    'topic_0': {
        0: PartitionMetadata(
            topic='topic_0',
            partition=0,
            leader=170396635,
            replicas=(170396635, 170398981),
            isr=(170398981,),
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

NOT_IN_SYNC_PARTITIONS = [
    {
        'isr': 1,
        'replicas': 2,
        'topic': 'topic_0',
        'partition': 0,
    },
]


def test_process_metadata_response_empty():
    result = _process_metadata_response(
        topics={},
    )

    assert result == []


def test_process_metadata_out_of_sync():
    result = _process_metadata_response(
        topics=TOPICS_STATE,
    )

    assert result == NOT_IN_SYNC_PARTITIONS


def test_prepare_output_ok_no_verbose():
    origin = {
        'message': "All configured replicas in sync.",
        'raw': {
            'not_enough_replicas_count': 0,
        }
    }
    assert _prepare_output([], False) == origin


def test_prepare_output_ok_verbose():
    origin = {
        'message': "All configured replicas in sync.",
        'raw': {
            'not_enough_replicas_count': 0,
            'partitions': [],
        }
    }
    assert _prepare_output([], True) == origin


def test_prepare_output_critical_no_verbose():
    origin = {
        'message': (
            "1 partition(s) have configured replicas that "
            "are not in sync."
        ),
        'raw': {
            'not_enough_replicas_count': 1,
        }
    }
    assert _prepare_output(NOT_IN_SYNC_PARTITIONS, False) == origin


def test_prepare_output_critical_verbose():
    origin = {
        'message': (
            "1 partition(s) have configured replicas that "
            "are not in sync."
        ),
        'verbose': (
            "Partitions:\n"
            "isr=1 is lower than replicas=2 for topic_0:0"
        ),
        'raw': {
            'not_enough_replicas_count': 1,
            'partitions': [
                {
                    'isr': 1,
                    'replicas': 2,
                    'partition': 0,
                    'topic': 'topic_0'
                },
            ],
        }
    }
    assert _prepare_output(NOT_IN_SYNC_PARTITIONS, True) == origin
