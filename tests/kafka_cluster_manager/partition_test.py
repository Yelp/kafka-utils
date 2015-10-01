from mock import sentinel
import pytest

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition


class TestPartition(object):
    @pytest.fixture
    def partition(self):
        mock_topic = sentinel.t1
        mock_topic.id = 't1'
        return Partition(
            mock_topic,
            0,
            [sentinel.r1, sentinel.r2],
        )

    def test_name(self, partition):
        assert partition.name == ('t1', 0)

    def test_topic(self, partition):
        assert partition.topic == sentinel.t1

    def test_replicas(self, partition):
        assert partition.replicas == [sentinel.r1, sentinel.r2]

    def test_leader(self, partition):
        assert partition.leader == sentinel.r1

    def test_replication_factor(self, partition):
        assert partition.replication_factor == 2

    def test_partition_id(self, partition):
        assert partition.partition_id == 0

    def test_add_replica(self, partition):
        new_broker = sentinel.new_r
        partition.add_replica(new_broker)
        assert partition.replicas == [sentinel.r1, sentinel.r2, sentinel.new_r]

    def test_swap_leader(self, partition):
        b = sentinel.r2
        old_replicas = partition.replicas
        partition.swap_leader(b)

        # Verify leader changed to b
        assert partition.leader == b
        # Verify that replica set remains same
        assert sorted(old_replicas) == sorted(partition.replicas)

    def test_followers_1(self, partition):

        # Case:1 With followers
        assert partition.followers == [sentinel.r2]

    def test_followers_2(self):
        # Case:2 No-followers
        mock_topic = sentinel.t1
        mock_topic.id = 't1'
        p2 = Partition(mock_topic, 0, [sentinel.r1])

        assert p2.followers == []
