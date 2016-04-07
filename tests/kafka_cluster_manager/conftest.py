import pytest

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.topic import Topic


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
