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
        """Add broker to current list."""
        self._brokers.append(broker)

    @property
    def partitions(self):
        """Evaluate and return set of all partitions in replication-group."""
        partitions = []
        for broker in self._brokers:
            partitions += broker.partitions
        return partitions

    def move_partition(
        self,
        victim_partition,
        rg_destination,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        """Move partition from current replication-group to destination
        replication-group.

        Step 1: Get overloaded and underloaded brokers
        Step 2: Evaluate source and destination broker
        Step 3: Move partition from source-broker to destination-broker

        Decide source broker and destination broker to move the partition.
        """
        # Get overloaded and underloaded brokers
        # This ensures partiton-count balancing implicitly
        over_loaded_brokers = self.get_over_loaded_brokers(
            total_brokers_cluster,
            total_partitions_cluster,
            victim_partition,
        )
        # If no over-loaded_brokers found, pick broker with maximum partitions
        if not over_loaded_brokers:
            over_loaded_brokers = sorted(
                self.brokers,
                key=lambda b: len(b.partitions),
                reverse=True,
            )

        under_loaded_brokers = rg_destination.get_under_loaded_brokers(
            total_brokers_cluster,
            total_partitions_cluster,
        )
        # Pick broker with minimum partitions if no under-loaded brokers found
        if not under_loaded_brokers:
            under_loaded_brokers = sorted(
                rg_destination.brokers, key=lambda b: len(b.partitions)
            )

        # Select best-fit source and destination brokers for partition
        assert(over_loaded_brokers and under_loaded_brokers)
        broker_source, broker_destination = self.broker_selection(
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
        )
        assert(victim_partition.name in [p.name for p in broker_source.partitions])
        assert(broker_source in self.brokers)
        assert(broker_destination in rg_destination.brokers)
        assert(rg_destination.id != self.id)

        # Moving partition
        broker_source.move_partition(victim_partition, broker_destination)

    def get_over_loaded_brokers(
        self,
        total_brokers,
        total_partitions,
        partition=None,
    ):
        """Get list of overloaded brokers in sorted order containing
        the more partitions than optimal count.
        """
        opt_partition_count = total_partitions // total_brokers
        over_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() > opt_partition_count + 1
        ]

        if partition:
            result = [
                broker for broker in over_loaded_brokers
                if partition.name in [p.name for p in broker.partitions]
            ]
        else:
            result = over_loaded_brokers
        return sorted(
            result,
            key=lambda b: len(b.partitions),
            reverse=True,
        )

    def get_under_loaded_brokers(
        self,
        total_brokers_cluster,
        total_partitions_cluster,
        partition=None,
    ):
        """Get list of brokers with lesser partitions than optimal amount for
        each broker containing the given partition.
        """
        opt_partition_count = total_partitions_cluster // total_brokers_cluster
        under_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() < opt_partition_count
        ]
        if partition:
            result = [
                broker for broker in under_loaded_brokers
                if partition.name in [p.name for p in broker.partitions]
            ]
        else:
            result = under_loaded_brokers
        return sorted(result, key=lambda b: len(b.partitions))

    def broker_selection(
            self,
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
    ):
        """Select best-fit source and destination brokers based on partition
        count and presense of partition over the broker.

        Best-fit Selection Criteria:
        Source broker: Select broker containing the victim-partition with
        maximum partitions.
        Destination broker: NOT containing the victim-partition with minimum
        partitions. If no such broker found, return first broker.

        This helps in ensuring:-
        * Topic-partitions are distributed across brokers.
        * Partition-count is balanced across replication-groups.
        """
        broker_source = None
        broker_destination = None
        for broker in over_loaded_brokers:
            partition_ids = [p.name for p in broker.partitions]
            if victim_partition.name in partition_ids:
                broker_source = broker
                break

        # Pick broker not having topic in that broker
        for broker in under_loaded_brokers:
            topic_ids = [partition.topic.id for partition in broker.partitions]
            if victim_partition.topic.id not in topic_ids:
                preferred_destination = broker
                break
        # If no valid broker found pick broker with minimum partitions
        if preferred_destination:
            broker_destination = preferred_destination
        else:
            broker_destination = under_loaded_brokers[0]

        assert(
            broker_source
            and broker_destination
            and broker_destination != broker_source
        )
        return broker_source, broker_destination

    def replica_count(self, partition):
        """Return the count of replicas of given partitions."""
        return len(
            [broker for broker in partition.replicas if broker in self._brokers]
        )
