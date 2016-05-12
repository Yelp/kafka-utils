from kafka_tools.kafka_cluster_manager.cluster_info.broker import Broker
from kafka_tools.kafka_cluster_manager.cluster_info.partition import Partition


def create_broker(broker_id, partitions):
    b = Broker(broker_id, partitions=set(partitions))
    for p in partitions:
        p.add_replica(b)
    return b


def create_and_attach_partition(topic, partition_id):
    partition = Partition(topic, partition_id)
    topic.add_partition(partition)
    return partition
