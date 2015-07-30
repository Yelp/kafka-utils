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

    def move_partition(
        self,
        victim_partition,
        rg_destination,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        """Move partition from current replication-group to destination
        replication-group.

        Select a replica for given partition and move it to selected
        partition in given destination placement-group.

        Decide source broker and destination broker to move the partition.
        """
        # This ensures partiton-count balancing implicitly
        over_loaded_brokers = self.get_over_loaded_brokers(
            victim_partition,
            total_brokers_cluster,
            total_partitions_cluster,
            victim_partition,
        )
        # If no over-loaded_brokers found, pick the broker with maximum
        if not over_loaded_brokers:
            # Pick-broker with maximum partition, if no over-loaded-brokers found
            valid_brokers = [
                broker
                for broker in self.brokers
                if victim_partition.name in [p.name for p in broker.partitions]
            ]
            over_loaded_brokers = [
                max(valid_brokers, key=lambda b:len(b.partitions))
            ]

        under_loaded_brokers = rg_destination.get_under_loaded_brokers(
            total_brokers_cluster,
            total_partitions_cluster,
        )
        # Pick broker with minimum partitions if no under-loaded brokers found
        if not under_loaded_brokers:
            sorted_brokers = sorted(
                rg_destination.brokers, key=lambda b: len(b.partitions)
            )
            # Try getting destination broker not containing the partition of
            # same topic
            preferred_broker = None
            for broker in sorted_brokers:
                topic_ids = [p.topic.id for p in broker.partitions]
                if victim_partition.topic.id not in topic_ids:
                    preferred_broker = broker
                    print('found preferred-broker')
                    break
            if preferred_broker:
                under_loaded_brokers = [preferred_broker]
                if preferred_broker == sorted_brokers[0]:
                    print('hurry, preferred broker concept worked!')
            else:
                under_loaded_brokers = [sorted_brokers[0]]

        # Get source and destination brokers for partition
        broker_source, broker_destination = self.broker_selection(
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
        )
        assert(victim_partition.name in [p.name for p in broker_source.partitions])
        assert(broker_destination in rg_destination.brokers)
        assert(rg_destination.id != self.id)

        # Moving partition
        broker_source.move_partition(victim_partition, broker_destination)

    def get_over_loaded_brokers(
        self,
        victim_partition,
        total_brokers,
        total_partitions,
        partition=None,
    ):
        """Get list of overloaded brokers containing the partition."""
        # Get total partitions across cluster (including replicas)
        opt_partition_count = total_partitions // total_brokers
        over_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() > opt_partition_count + 1
        ]

        # Get over loaded brokers containing the given partition
        if partition:
            result = [
                broker for broker in over_loaded_brokers
                if partition.name in [p.name for p in broker.partitions]
            ]
        else:
            result = over_loaded_brokers
        return result

        assert(over_loaded_brokers)
        return over_loaded_brokers

    def get_under_loaded_brokers(
        self,
        total_brokers_cluster,
        total_partitions_cluster,
        partition=None,
    ):
        """Get list of brokers with lesser partitions than optimal amount for
        each broker containing the given partition
        """
        opt_partition_count = total_partitions_cluster // total_brokers_cluster
        under_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() < opt_partition_count
        ]
        if partition:
            result = [
                broker for broker in over_loaded_brokers
                if partition.name in [p.name for p in broker.partitions]
            ]
        else:
            result = under_loaded_brokers
        return result

    def broker_selection(
            self,
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
    ):
        """Select valid source broker with partition and destination broker
        from under loaded brokers not having partition.

        This helps in ensuring that topic-partitions are distributed across
        brokers
        """
        broker_source = None
        broker_destination = None
        # Decision factor-2: Balancing #partition-count over brokers
        # Get broker having maximum partitions and given partition
        for broker in over_loaded_brokers:
            partition_ids = [p.name for p in broker.partitions]
            if victim_partition.name in partition_ids:
                broker_source = broker

        # Decision-factor 3: Pick broker not having topic in that broker
        # Helps in load balancing
        # Decision-factor 2: Pick broker with least #partition-count
        for broker in under_loaded_brokers:
            topic_ids = [partition.topic.id for partition in broker.partitions]
            if partition.topic.id not in topic_ids:
                broker_destination = broker
        # If no valid broker found
        if not broker_destination:
            broker_destination = under_loaded_brokers[0]
        assert(broker_source and broker_destination and broker_destination != broker_source)
        return broker_source, broker_destination

    def replica_count(self, partition):
        """Return the count of replicas of given partitions."""
        return len(
            [broker for broker in partition.replicas if broker in self._brokers]
        )
