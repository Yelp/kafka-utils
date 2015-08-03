from socket import gethostbyaddr, herror
import socket
import sys


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
        """Remove partition from partition list with same partition-name."""
        # Get valid partition
        remove_partition = None
        if partition in self.partitions:
            remove_partition = partition
        else:
            valid_partitions = [
                p for p in self.partitions if p.name == partition.name
            ]
            if valid_partitions:
                remove_partition = valid_partitions[0]
            else:
                print(
                    "[ERROR] partition {partition} not found in broker {broker}"
                    .format(partition=partition.name, broker=self.id)
                )
                sys.exit(1)
        self._partitions.remove(remove_partition)

    def add_partition(self, partition):
        """Add partition to partition list."""
        if partition.name in [p.name for p in self._partitions]:
            print(
                '[WARNING] Partition {id} already present in broker {broker}'
                .format(id=partition.name, broker=self._id)
            )
        else:
            self._partitions.append(partition)

    def partition_count(self):
        """Total partitions in broker."""
        return len(self._partitions)

    def move_partition(self, partition, broker_destination):
        """Move partition to destination broker and adjust replicas."""
        # Remove partition and broker from replicas
        self.remove_partition(partition)
        partition.replicas.remove(self)

        # Add partition and broker in replicas
        broker_destination.add_partition(partition)
        partition.replicas.append(broker_destination)
