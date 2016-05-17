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

from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition
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
