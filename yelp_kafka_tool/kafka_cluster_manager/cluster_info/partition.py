class Partition(object):
    """Class representing the partition object.
    It contains topic-partition_id tuple as name, topic and replicas
    (list of brokers).
    """
    def __init__(self, topic, id, replicas=None):
        # Every partition name has (topic, partition) tuple
        self._name = (topic.id, id)
        self._replicas = replicas or []
        self._topic = topic

    @property
    def name(self):
        "Name of partition, consisting of (topic_id, partition_id) tuple."""
        return self._name

    @property
    def partition_id(self):
        """Partition id component of the partition-tuple."""
        return int(self._name[1])

    @property
    def topic(self):
        return self._topic

    @property
    def replicas(self):
        """List of brokers in partition."""
        return self._replicas

    @property
    def leader(self):
        """Leader broker for the partition."""
        return self._replicas[0]

    @property
    def replication_factor(self):
        return len(self._replicas)

    def add_replica(self, broker):
        """Add broker to existing set of replicas."""
        self._replicas.append(broker)

    def swap_leader(self, new_leader):
        """Change the preferred leader with one of
        given replicas.

        Note: Leaders for all the replicas of current
        partition needs to be changed.
        """
        # Replica set cannot be changed
        assert(new_leader in self._replicas)
        idx = self._replicas.index(new_leader)
        new_replicas = self._replicas[:]
        new_replicas[0] = self._replicas[idx]
        new_replicas[idx] = self._replicas[0]
        self._replicas = new_replicas
        return

    @property
    def non_leaders(self):
        """Return list of brokers not as preferred leader
        for a particular partition.
        """
        # Empty list is returned in case no non-leaders found
        return self._replicas[1:]
