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
import pytest

from .helper import broker_range
from kafka_utils.kafka_cluster_manager.cluster_info.cluster_topology \
    import ClusterTopology
from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition
from kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer \
    import PartitionMeasurer
from kafka_utils.kafka_cluster_manager.cluster_info.topic import Topic


@pytest.fixture
def create_partition():
    """Fixture to create a partition and attach it to a topic"""
    topics = {}

    def _add_partition(topic_id, partition_id, replication_factor=1):
        if topic_id not in topics:
            topics[topic_id] = Topic(topic_id, replication_factor)
        topic = topics[topic_id]
        partition = Partition(topic, partition_id)
        topic.add_partition(partition)
        return partition

    return _add_partition


@pytest.fixture
def default_assignment():
    return {
        (u'T0', 0): ['1', '2'],
        (u'T0', 1): ['2', '3'],
        (u'T1', 0): ['0', '1', '2', '3'],
        (u'T1', 1): ['0', '1', '2', '3'],
        (u'T2', 0): ['2'],
        (u'T3', 0): ['0', '1', '2'],
        (u'T3', 1): ['0', '1', '4'],
    }


@pytest.fixture
def default_brokers():
    return broker_range(5)


@pytest.fixture
def default_broker_rg():
    return {
        '0': 'rg1',
        '1': 'rg1',
        '2': 'rg2',
        '3': 'rg2',
        '4': 'rg1',
        '5': 'rg3',
        '6': 'rg4',
        '7': 'rg1',
    }


@pytest.fixture
def default_get_replication_group_id(default_broker_rg):
    def get_replication_group_id(broker):
        try:
            return default_broker_rg[broker.id]
        except KeyError:
            return None
    return get_replication_group_id


@pytest.fixture
def default_partition_weight():
    return {
        (u'T0', 0): 2,
        (u'T0', 1): 3,
        (u'T1', 0): 4,
        (u'T1', 1): 5,
        (u'T2', 0): 6,
        (u'T3', 0): 7,
        (u'T3', 1): 8,
    }


@pytest.fixture
def default_partition_size():
    return {
        (u'T0', 0): 3,
        (u'T0', 1): 4,
        (u'T1', 0): 5,
        (u'T1', 1): 6,
        (u'T2', 0): 7,
        (u'T3', 0): 8,
        (u'T3', 1): 9,
    }


@pytest.fixture
def default_partition_measurer(
        default_partition_weight,
        default_partition_size,
):
    class TestMeasurer(PartitionMeasurer):

        def __init__(self):
            super(TestMeasurer, self).__init__(None, None, None, None)

        def get_weight(self, partition_name):
            try:
                return default_partition_weight[partition_name]
            except KeyError:
                return 1

        def get_size(self, partition_name):
            try:
                return default_partition_size[partition_name]
            except KeyError:
                return 1

    return TestMeasurer()


@pytest.fixture
def create_cluster_topology(
        default_assignment,
        default_brokers,
        default_get_replication_group_id,
        default_partition_measurer,
):
    def build_cluster_topology(
            assignment=None,
            brokers=None,
            get_replication_group_id=None,
            partition_measurer=None
    ):
        assignment = assignment or default_assignment
        brokers = brokers or default_brokers
        get_replication_group_id = \
            get_replication_group_id or default_get_replication_group_id
        partition_measurer = partition_measurer or default_partition_measurer

        return ClusterTopology(
            assignment,
            brokers,
            get_replication_group_id,
            partition_measurer,
        )

    return build_cluster_topology
