"""This file incorporates handling partitions over replication-groups
(Availability-zones) in our case.
"""


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
        """Return list of brokers ."""
        return self._brokers

    def add_broker(self, broker):
        self._brokers.append(broker)

    @property
    def partitions(self):
        """Evaluate and return set of all partitions in replication-group."""
        partitions = []
        for broker in self._brokers:
            partitions += broker.partitions
        return partitions

    def move_partition(self, partition, destination_rg):
        """Move partition from current replication-group to destination
        replication-group.

        Select a replica for given partition and move it to selected
        partition in given destination placement-group.

        Decide source broker and destination broker to move the partition.
        """
        # Assert given partition belongs to current replication-group
        assert(partition in self.partitions)
        over_loaded_brokers = self.get_over_loaded_brokers()
        under_loaded_brokers = destination_pg.get_under_loaded_brokers()

        # In descending order
        broker_from, broker_to = self.get_target_brokers(
            over_loaded_brokers,
            under_loaded_brokers,
            partition,
        )
        assert(broker_from in self.brokers)
        assert(broker_to in destination_rg.brokers)

        # Moving partition
        broker_from.remove_partition(partition)
        broker_to.add_partition(partition)

    def get_over_loaded_brokers(self):
        """Get list of overloaded brokers."""
        total_partitions_cluster = get_total_partitions()
        total_brokers_cluster = get_total_brokers()
        opt_partition_count = total_partitions_cluster // total_brokers_cluster
        over_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() > opt_partition_count + 1
        ]
        # Pick-broker with maximum partition, if no over-loaded-brokers found
        if not over_loaded_brokers:
            max_partition_count = 0
            # TODO: list-comprehension
            for broker in self._brokers:
                if len(broker.partions) > max_partition_count:
                    over_loaded_brokers = [broker]
                    max_partition_count = len(broker.partitions)
        return over_loaded_broker

    def get_under_loaded_brokers(self):
        """Get list of underloaded brokers."""
        # TODO: Get total counts
        total_partitions_cluster = get_total_partitions()
        total_brokers_cluster = get_total_brokers()
        opt_partition_count = total_partitions_cluster / total_brokers_cluster
        under_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() < opt_partition_count
        ]
        # Pick broker with minimum partitions if no under-loaded brokers found
        if not over_loaded_brokers:
            min_partition_count = -1
            # TODO: list-comprehension
            for broker in self._brokers:
                if len(broker.partions) < min_partition_count \
                        or min_partition_count == -1:
                    under_loaded_brokers = [broker]
                    min_partition_count = len(broker.partitions)
        return under_loaded_brokers

    def get_target_brokers(
            self,
            over_loaded_brokers,
            under_loaded_brokers,
            partition,
    ):
        """Select valid source broker with partition and destination broker
        from under loaded brokers not having partition.
        """
        broker_from = None
        broker_to = None
        # Decision factor-2: Balancing #partition-count over brokers
        # Get broker having maximum partitions and given partition
        for broker in over_loaded_brokers:
            if partition in broker.partitions:
                broker_from = broker
        # Decision-factor 3: Pick broker not having topic in that broker
        # Helps in load balancing
        # Decision-factor 2: Pick broker with least #partition-count
        for broker in under_loaded_brokers:
            topic_ids = [partition.topic_id for partition in broker.partitions]
            if partition[0] not in topic_ids:
                broker_to = broker
        # If no valid broker found
        if not broker_to:
            print("No valid broker with no duplicate partition found, picking first broker")
            broker_to = under_loaded_brokers[0]
        assert(broker_from)
        assert(broker_to)
        assert(broker_from != broker_to)
        return broker_from, broker_to
