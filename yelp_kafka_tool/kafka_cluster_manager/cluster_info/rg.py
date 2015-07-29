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
        destination_rg,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        """Move partition from current replication-group to destination
        replication-group.

        Select a replica for given partition and move it to selected
        partition in given destination placement-group.

        Decide source broker and destination broker to move the partition.
        """
        # Assert given partition belongs to current replication-group
        print('victim-partition-in move', victim_partition.name)
        partition_ids = [p.name for p in self.partitions]
        print('partitions in rg', self.id, ':', len(partition_ids))
        assert(victim_partition.name in partition_ids)
        over_loaded_brokers = self.get_over_loaded_brokers(
            victim_partition,
            total_brokers_cluster,
            total_partitions_cluster,
        )
        for broker in over_loaded_brokers:
            # over-loaded-brokers should be part of current replication-group
            assert(broker in self.brokers)
        under_loaded_brokers = destination_rg.get_under_loaded_brokers(
            total_brokers_cluster,
            total_partitions_cluster
        )

        # In descending order
        broker_from, broker_to = self.get_target_brokers(
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
        )
        assert(broker_from in self.brokers)
        print('partition:', victim_partition.name)
        print('broker_from:', broker_from.id)
        print('partitions in broker_from', [p.name for p in broker_from.partitions])
        assert(victim_partition.name in [p.name for p in broker_from.partitions])
        assert(broker_to in destination_rg.brokers)
        assert(destination_rg.id != self.id)

        # Moving partition
        broker_from.remove_partition(victim_partition)
        # Remove broker from partitions's replica list
        victim_partition.replicas.remove(broker_from)
        broker_to.add_partition(victim_partition)
        # Add broker from partitions's replica list
        victim_partition.replicas.append(broker_to)
        print('partition', victim_partition.name, 'moved')

    def get_over_loaded_brokers(
        self,
        victim_partition,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        """Get list of overloaded brokers."""
        # Get total partitions across cluster (including replicas)
        opt_partition_count = total_partitions_cluster // total_brokers_cluster
        print('opt partition count', opt_partition_count)
        over_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() > opt_partition_count + 1 and
            victim_partition.name in [p.name for p in broker.partitions]
        ]
        # Pick-broker with maximum partition, if no over-loaded-brokers found
        if not over_loaded_brokers:
            max_partition_count = 0
            # TODO: list-comprehension
            valid_brokers = [broker for broker in self.brokers if victim_partition.name in [p.name for p in broker.partitions]]
            for broker in valid_brokers:
                if len(broker.partitions) > max_partition_count:
                    over_loaded_brokers = [broker]
                    max_partition_count = len(broker.partitions)
        assert(over_loaded_brokers)
        return over_loaded_brokers

    def get_under_loaded_brokers(
        self,
        total_brokers_cluster,
        total_partitions_cluster,
    ):
        """Get list of underloaded brokers."""
        # TODO: Get total counts
        opt_partition_count = total_partitions_cluster // total_brokers_cluster
        under_loaded_brokers = [
            broker for broker in self._brokers
            if broker.partition_count() < opt_partition_count
        ]
        # Pick broker with minimum partitions if no under-loaded brokers found
        if not under_loaded_brokers:
            min_partition_count = -1
            # TODO: list-comprehension
            for broker in self._brokers:
                if len(broker.partitions) < min_partition_count \
                        or min_partition_count == -1:
                    under_loaded_brokers = [broker]
                    min_partition_count = len(broker.partitions)
        return under_loaded_brokers

    def get_target_brokers(
            self,
            over_loaded_brokers,
            under_loaded_brokers,
            victim_partition,
    ):
        """Select valid source broker with partition and destination broker
        from under loaded brokers not having partition.
        """
        print('victim-partition', victim_partition.name)
        broker_from = None
        broker_to = None
        # Decision factor-2: Balancing #partition-count over brokers
        # Get broker having maximum partitions and given partition
        for broker in over_loaded_brokers:
            partition_ids = [p.name for p in broker.partitions]
            if victim_partition.name in partition_ids:
                print('broker-from decided', broker.id, 'for partition', victim_partition.name)
                broker_from = broker

        # Decision-factor 3: Pick broker not having topic in that broker
        # Helps in load balancing
        # Decision-factor 2: Pick broker with least #partition-count
        for broker in under_loaded_brokers:
            topic_ids = [partition.topic.id for partition in broker.partitions]
            if partition.topic.id not in topic_ids:
                broker_to = broker
        # If no valid broker found
        if not broker_to:
            print("No valid broker with no duplicate partition found, picking first broker")
            broker_to = under_loaded_brokers[0]
        assert(broker_from)
        assert(broker_to)
        assert(broker_from != broker_to)
        return broker_from, broker_to

    def replica_count(self, partition):
        """Return the count of replicas of given partitions."""
        return len(
            [broker for broker in partition.replicas if broker in self._brokers]
        )
