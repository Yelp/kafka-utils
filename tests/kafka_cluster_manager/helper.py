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
from kafka_utils.kafka_cluster_manager.cluster_info.broker import Broker
from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition


def create_broker(broker_id, partitions):
    b = Broker(broker_id, partitions=set(partitions))
    for p in partitions:
        p.add_replica(b)
    return b


def create_and_attach_partition(topic, partition_id):
    partition = Partition(topic, partition_id)
    topic.add_partition(partition)
    return partition


def broker_range(n):
    """Return list of brokers with broker ids ranging from 0 to n-1."""
    return {str(x): {"host": "host%s" % x} for x in range(n)}
