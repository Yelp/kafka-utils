from mock import Mock, sentinel
import pytest

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.topic import Topic
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition


class TestTopic(object):

    @pytest.fixture
    def topic(self):
        return Topic('t0', 2, [sentinel.p1, sentinel.p2])

    def test_id(self, topic):
        assert topic.id == 't0'

    def test_replication_factor(self, topic):
        assert topic.replication_factor == 2

    def test_partitions(self, topic):
        assert topic.partitions == [sentinel.p1, sentinel.p2]

    def test_add_partition(self):
        mock_partitions = [
            Mock(
                spec=Partition,
                replicas=[sentinel.r1, sentinel.r2],
            ),
            Mock(
                spec=Partition,
                replicas=[sentinel.r4, sentinel.r3],
            ),
        ]
        topic = Topic('t0', 2, mock_partitions)
        new_partition = Mock(spec=Partition, replicas=[sentinel.r2])
        topic.add_partition(new_partition)
        assert topic.partitions == [
            mock_partitions[0],
            mock_partitions[1],
            new_partition,
        ]
