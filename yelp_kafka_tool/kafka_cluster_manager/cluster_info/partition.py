class Partition(object):
    """Class representing the partition object.
    It contains topic-partition_id tuple as name, topic and replicas
    (list of brokers).
    """
    def __init__(self, name, topic, replicas=None):
        # Every partition name has (topic, partition) tuple
        assert(len(name) == 2)
        self._name = name
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
