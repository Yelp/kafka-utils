from collections import Counter
from socket import gethostbyaddr, herror


class Broker(object):
    """Broker class object, consisting of following attributes
        -id: Id of broker
        -partitions: partitions under a given broker
    """
    def __init__(self, id, partitions=None):
        self._id = id
        self._partitions = partitions or []

    @property
    def hostname(self):
        """Get hostname of broker."""
        try:
            result = gethostbyaddr(str(self._id))[0]
        except herror:
            print(
                '[WARNING] Unknown host for broker {broker}'.format(
                    broker=self._id
                )
            )
            print('Returning as localhost.')
            result = gethostbyaddr('localhost')[0]
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
            self._partitions.remove(partition)
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
        self._partitions.append(partition)

    def partition_count(self):
        """Total partitions in broker."""
        return len(self._partitions)

    def get_per_topic_partitions_count(self):
        """Return partition-count of each topic."""
        return Counter((partition.topic for partition in self._partitions))
