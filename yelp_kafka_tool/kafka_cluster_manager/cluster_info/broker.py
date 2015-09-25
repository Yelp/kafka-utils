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

    def decrease_leader_count(
        self,
        partitions,
        leaders_per_broker,
        opt_count,
    ):
        """Re-order eligible replicas to balance preferred leader assignment."""
        # Generate a list of partitions for which we can change the leader.
        # Filter out partitions with only one replica (Replicas cannot be changed).
        curr_leader = self
        possible_partitions = [
            partition
            for partition in partitions
            if curr_leader == partition.leader and len(partition.replicas) > 1
        ]
        for possible_victim_partition in possible_partitions:
            brokers = possible_victim_partition.non_leaders
            for possible_new_leader in brokers:
                if (leaders_per_broker[possible_new_leader] <= opt_count and
                        leaders_per_broker[curr_leader] -
                        leaders_per_broker[possible_new_leader] > 1):
                    victim_partition = possible_victim_partition
                    new_leader = possible_new_leader
                    victim_partition.swap_leader(new_leader)
                    leaders_per_broker[new_leader] += 1
                    leaders_per_broker[curr_leader] -= 1
                    break
            if leaders_per_broker[curr_leader] == opt_count:
                return
