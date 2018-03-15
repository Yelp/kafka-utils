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

from kafka.structs import PartitionMetadata

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.zookeeper import ZK


LEADER_NOT_AVAILABLE_ERROR = 5
REPLICA_NOT_AVAILABLE_ERROR = 9


def get_topic_partition_metadata(hosts):
    """Returns topic-partition metadata from Kafka broker.

    kafka-python 1.3+ doesn't include partition metadata information in
    topic_partitions so we extract it from metadata ourselves.
    """
    kafka_client = KafkaToolClient(hosts, timeout=10)
    kafka_client.load_metadata_for_topics()
    topic_partitions = kafka_client.topic_partitions
    resp = kafka_client.send_metadata_request()

    for _, topic, partitions in resp.topics:
        for partition_error, partition, leader, replicas, isr in partitions:
            if topic_partitions.get(topic, {}).get(partition) is not None:
                topic_partitions[topic][partition] = PartitionMetadata(topic, partition, leader,
                                                                       replicas, isr, partition_error)
    return topic_partitions


def get_unavailable_brokers(zk, partition_metadata):
    """Returns the set of unavailable brokers from the difference of replica
    set of given partition to the set of available replicas.
    """
    topic_data = zk.get_topics(partition_metadata.topic)
    topic = partition_metadata.topic
    partition = partition_metadata.partition
    expected_replicas = set(topic_data[topic]['partitions'][str(partition)]['replicas'])
    available_replicas = set(partition_metadata.replicas)
    return expected_replicas - available_replicas


def get_topic_partition_with_error(cluster_config, error, fetch_unavailable_brokers=False):
    """Fetches the metadata from the cluster and returns the set of
    (topic, partition) tuples containing all the topic-partitions
    currently affected by the specified error. It also fetches unavailable-broker list
    if required."""

    metadata = get_topic_partition_metadata(cluster_config.broker_list)
    affected_partitions = set()
    if fetch_unavailable_brokers:
        unavailable_brokers = set()
    with ZK(cluster_config) as zk:
        for partitions in metadata.values():
            for partition_metadata in partitions.values():
                if int(partition_metadata.error) == error:
                    if fetch_unavailable_brokers:
                        unavailable_brokers |= get_unavailable_brokers(zk, partition_metadata)
                    affected_partitions.add((partition_metadata.topic, partition_metadata.partition))

    if fetch_unavailable_brokers:
        return affected_partitions, unavailable_brokers
    else:
        return affected_partitions
