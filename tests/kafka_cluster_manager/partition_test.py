from mock import sentinel
import pytest

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.partition import Partition


class TestPartition(object):
    @pytest.fixture
    def partition(self):
        mock_topic = sentinel.t1
        mock_topic.id = 't1'
        p_id = 0
        return Partition(
            mock_topic,
            p_id,
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

    def test_non_leaders(self, partition):
        non_leaders = partition.non_leaders

        assert non_leaders == [sentinel.r2]
