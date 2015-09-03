"""This file incorporates handling partitions over replication-groups
(Availability-zones) in our case.
"""
import logging


class ReplicationGroup(object):
    """Represent attributes and functions specific to replication-groups
    (Availability zones) abbreviated as rg.
    """
    def __init__(self, id, brokers=None):
        self._id = id
        self._brokers = brokers or set()
        self.log = logging.getLogger(self.__class__.__name__)

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
        if broker not in self._brokers:
            self._brokers.add(broker)
        else:
            self.log.warning(
                '[WARNING] Broker {broker_id} already present in '
                'replication-group {rg_id}'.format(
                    broker_id=broker.id,
                    rg_id=self.id,
                )
            )

    @property
    def partitions(self):
        """Evaluate and return set of all partitions in replication-group.
        rtype: list, replicas of partitions can reside in this group
        """
        return [
            partition
            for broker in self._brokers
            for partition in broker.partitions
        ]

    def count_replica(self, partition):
        """Return count of replicas of given partition."""
        return self.partitions.count(partition)

    def move_partition(
            self,
            rg_destination,
            victim_partition,
    ):
        """Move partition(victim) from current replication-group to destination
        replication-group.

        Step 1: Evaluate source and destination broker
        Step 2: Move partition from source-broker to destination-broker
        """
        # Select best-fit source and destination brokers for partition
        # Best-fit is based on partition-count and presence/absence of
        # Same topic-partition over brokers
        broker_source, broker_destination = self._select_broker(
            rg_destination,
            victim_partition,
        )

        # Actual-movement of victim-partition
        broker_source.move_partition(victim_partition, broker_destination)

    def _select_broker(
        self,
        rg_destination,
        victim_partition,
    ):
        """Select best-fit source and destination brokers based on partition
        count and presence of partition over the broker.

        * Get overloaded and underloaded brokers
        Best-fit Selection Criteria:
        Source broker: Select broker containing the victim-partition with
        maximum partitions.
        Destination broker: NOT containing the victim-partition with minimum
        partitions. If no such broker found, return first broker.

        This helps in ensuring:-
        * Topic-partitions are distributed across brokers.
        * Partition-count is balanced across replication-groups.
        """
        # Get overloaded brokers in source replication-group
        over_loaded_brokers = self._select_over_loaded_brokers(
            victim_partition,
        )
        # Get underloaded brokers in destination replication-group
        under_loaded_brokers = rg_destination._select_under_loaded_brokers(
            victim_partition,
        )
        broker_source = self._elect_source_broker(over_loaded_brokers)
        broker_destination = self._elect_dest_broker(
            under_loaded_brokers,
            victim_partition,
        )
        return broker_source, broker_destination

    def _select_over_loaded_brokers(
        self,
        victim_partition,
    ):
        """Get over-loaded brokers as sorted broker in partition-count and
        containing victim-partition.
        """
        over_loaded_brokers = [
            broker
            for broker in self._brokers
            if victim_partition in broker.partitions
        ]
        return sorted(
            over_loaded_brokers,
            key=lambda b: len(b.partitions),
            reverse=True,
        )

    def _select_under_loaded_brokers(
        self,
        victim_partition,
    ):
        """Get brokers in ascending sorted order of partition-count
        not containing victim-partition.
        """
        under_loaded_brokers = [
            broker
            for broker in self._brokers
            if victim_partition not in broker.partitions
        ]
        return sorted(under_loaded_brokers, key=lambda b: len(b.partitions))

    def _elect_source_broker(self, over_loaded_brokers):
        """Select first broker from given brokers having victim_partition."""
        return over_loaded_brokers[0]

    def _elect_dest_broker(self, under_loaded_brokers, victim_partition):
        """Select first broker from under_loaded_brokers preferring not having
        partition of same topic as victim partition.
        """
        # Pick broker having least partitions of the given topic
        broker_topic_partition_cnt = [
            (broker, broker.count_partitions(victim_partition.topic))
            for broker in under_loaded_brokers
        ]
        min_count_pair = min(
            broker_topic_partition_cnt,
            key=lambda ele: ele[1],
        )
        return min_count_pair[0]
