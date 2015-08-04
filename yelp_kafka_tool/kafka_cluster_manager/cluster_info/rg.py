"""This file incorporates handling partitions over replication-groups
(Availability-zones) in our case.
"""

from yelp_kafka_tool.kafka_cluster_manager.reassign.rg_rebalance import (
    move_partition,
)


class ReplicationGroup(object):
    """Represent attributes and functions specific to replication-groups
    (Availability zones) abbreviated as rg.
    """
    def __init__(self, id, brokers=None):
        self._id = id
        self._brokers = brokers or []

    @property
    def id(self):
        """Return name of replication-groups."""
        return self._id

    @property
    def brokers(self):
        """Return list of brokers."""
        return self._brokers

    def add_broker(self, broker):
        """Add broker to current broker-list."""
        if broker not in self.brokers:
            self._brokers.append(broker)
        else:
            print(
                '[WARNING] Broker {broker_id} already present in '
                'replication-group {rg_id}'.format(
                    broker_id=broker.id,
                    rg_id=self.id,
                )
            )

    @property
    def partitions(self):
        """Evaluate and return set of all partitions in replication-group."""
        return [
            partition
            for broker in self._brokers
            for partition in broker.partitions
        ]

    def move_partition(
        self,
        victim_partition,
        rg_destination,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        move_partition(
            self,
            rg_destination,
            victim_partition,
            total_brokers_cluster,
            total_partitions_cluster,
        )
