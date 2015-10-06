import logging


class Broker(object):
    """Broker class object, consisting of following attributes
        -id: Id of broker
        -partitions: partitions under a given broker
    """
    def __init__(self, id, partitions=None):
        self._id = id
        self._partitions = partitions or set()
        self.log = logging.getLogger(self.__class__.__name__)

    def get_hostname(self, zk):
        """Get hostname of broker from zookeeper."""
        try:
            hostname = zk.get_brokers(self._id)
            result = hostname[self._id]['host']
        except KeyError:
            self.log.warning(
                'Unknown host for broker {broker}. Returning as'
                ' localhost'.format(broker=self._id)
            )
            result = 'localhost'
        return result

    @property
    def partitions(self):
        return self._partitions

    @property
    def id(self):
        return self._id

    @property
    def topics(self):
        """Return the set of topics current in broker."""
        return set([partition.topic for partition in self._partitions])

    def remove_partition(self, partition):
        """Remove partition from partition list."""
        if partition in self._partitions:
            # Remove partition from set
            self._partitions.remove(partition)
            # Remove broker from replica list of partition
            partition.replicas.remove(self)
        else:
            raise ValueError(
                'Partition: {topic_id}:{partition_id} not found in broker '
                '{broker_id}'.format(
                    topic_id=partition.topic.id,
                    partition_id=partition.partition_id,
                    broker_id=self._id,
                )
            )

    def add_partition(self, partition):
        """Add partition to partition list."""
        assert(partition not in self._partitions)
        # Add partition to existing set
        self._partitions.add(partition)
        # Add broker to replica list
        partition.replicas.append(self)

    def move_partition(self, partition, broker_destination):
        """Move partition to destination broker and adjust replicas."""
        self.remove_partition(partition)
        broker_destination.add_partition(partition)

    def count_partitions(self, topic):
        """Return count of partitions for given topic."""
        return sum([
            1
            for p in self._partitions
            if p.topic == topic
        ])

    def count_preferred_replica(self):
        """Return number of times broker is set as preferred leader."""
        return sum(
            [1 for partition in self.partitions if partition.leader == self],
        )

    def decrease_leader_count(self, partitions, leaders_per_broker, opt_count):
        """Re-order eligible replicas to balance preferred leader assignment.

        :params:
        partitions:         Set of all partitions in the cluster.
        leaders_per_broker: Broker-as-leader-count per broker.
        opt_count:          Optimal value for each broker to act as leader.
        """
        # Generate a list of partitions for which we can change the leader.
        # Filter out partitions with one replica (Replicas cannot be changed).
        # self is current-leader
        possible_partitions = [
            partition
            for partition in partitions
            if self == partition.leader and len(partition.replicas) > 1
        ]
        for possible_victim_partition in possible_partitions:
            for possible_new_leader in possible_victim_partition.followers:
                if (leaders_per_broker[possible_new_leader] <= opt_count and
                        leaders_per_broker[self] -
                        leaders_per_broker[possible_new_leader] > 1):
                    victim_partition = possible_victim_partition
                    new_leader = possible_new_leader
                    victim_partition.swap_leader(new_leader)
                    leaders_per_broker[new_leader] += 1
                    leaders_per_broker[self] -= 1
                    break
            if leaders_per_broker[self] == opt_count:
                return

    def get_preferred_partition(self, broker):
        """Get partition from given source-partitions with least siblings in
        given destination broker-partitions and sibling count.

        :key_term:
        siblings: Partitions belonging to same topic
        source-partitions: Partitions of current object

        :params:
        broker:   Destination broker where siblings for given source
                  partitions are analysed
        """
        # Only partitions not having replica in broker are valid
        # Get best fit partition, based on avoiding partition from same topic
        # and partition with least siblings in destination-broker.
        # TODO: will multiple partition with minimum value lead to non-determinism?
        eligible_partitions = self.partitions - broker.partitions
        if eligible_partitions:
            pref_partition = min(
                eligible_partitions,
                key=lambda source_partition:
                    source_partition.count_siblings(broker.partitions),
            )
            return pref_partition
        else:
            return None
