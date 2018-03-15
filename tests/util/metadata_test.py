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

import mock
from kafka.common import PartitionMetadata

from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.metadata import get_topic_partition_with_error
from kafka_utils.util.metadata import LEADER_NOT_AVAILABLE_ERROR
from kafka_utils.util.metadata import REPLICA_NOT_AVAILABLE_ERROR


METADATA_RESPONSE_ALL_FINE = {
    'topic0': {
        0: PartitionMetadata(
            topic='topic0',
            partition=0,
            leader=13,
            replicas=(13, 100),
            isr=(13, 100),
            error=0,
        ),
        1: PartitionMetadata(
            topic='topic0',
            partition=1,
            leader=100,
            replicas=(13, 100),
            isr=(13, 100),
            error=0,
        ),
    },
    'topic1': {
        0: PartitionMetadata(
            topic='topic1',
            partition=0,
            leader=666,
            replicas=(300, 500, 666),
            isr=(300, 500, 666),
            error=0,
        ),
        1: PartitionMetadata(
            topic='topic1',
            partition=1,
            leader=300,
            replicas=(300, 500, 666),
            isr=(300, 500, 666),
            error=0,
        ),
    },
}


METADATA_RESPONSE_WITH_ERRORS = {
    'topic0': {
        0: PartitionMetadata(
            topic='topic0',
            partition=0,
            leader=13,
            replicas=(13, 100),
            isr=(13, 100),
            error=9,
        ),
        1: PartitionMetadata(
            topic='topic0',
            partition=1,
            leader=100,
            replicas=(13, 100),
            isr=(13, 100),
            error=5,
        ),
    },
    'topic1': {
        0: PartitionMetadata(
            topic='topic1',
            partition=0,
            leader=666,
            replicas=(300, 500, 666),
            isr=(300, 500, 666),
            error=0,
        ),
        1: PartitionMetadata(
            topic='topic1',
            partition=1,
            leader=300,
            replicas=(300, 500, 666),
            isr=(300, 500, 666),
            error=9,
        ),
    },
}


@mock.patch(
    'kafka_utils.util.metadata.get_topic_partition_metadata',
)
class TestMetadata(object):
    cluster_config = ClusterConfig(
        type='mytype',
        name='some_cluster',
        broker_list='some_list',
        zookeeper='some_ip'
    )

    def test_get_topic_partition_metadata_empty(self, mock_get_metadata):
        mock_get_metadata.return_value = {}

        with mock.patch(
            'kafka_utils.util.metadata.ZK',
        ):
            actual = get_topic_partition_with_error(self.cluster_config, 5)
        expected = set([])
        assert actual == expected
        mock_get_metadata.asserd_called_wity('some_list')

    def test_get_topic_partition_metadata_no_errors(self, mock_get_metadata):
        mock_get_metadata.return_value = METADATA_RESPONSE_ALL_FINE

        with mock.patch(
            'kafka_utils.util.metadata.ZK',
        ):
            actual = get_topic_partition_with_error(self.cluster_config, 5)
        expected = set([])
        assert actual == expected

    def test_get_topic_partition_metadata_replica_not_available(self, mock_get_metadata):
        mock_get_metadata.return_value = METADATA_RESPONSE_WITH_ERRORS

        with mock.patch(
            'kafka_utils.util.metadata.ZK',
        ):
            actual = get_topic_partition_with_error(self.cluster_config, REPLICA_NOT_AVAILABLE_ERROR)
        expected = {('topic0', 0), ('topic1', 1)}
        assert actual == expected

    def test_get_topic_partition_metadata_leader_not_available(self, mock_get_metadata):
        mock_get_metadata.return_value = METADATA_RESPONSE_WITH_ERRORS

        with mock.patch(
            'kafka_utils.util.metadata.ZK',
        ):
            actual = get_topic_partition_with_error(self.cluster_config, LEADER_NOT_AVAILABLE_ERROR)
        expected = set([('topic0', 1)])
        assert actual == expected
