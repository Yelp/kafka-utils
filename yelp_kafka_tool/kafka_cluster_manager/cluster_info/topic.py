"""This class contains information for a topic object.

Useful as part of reassignment project when deciding upon moving
partitions of same topic over different brokers.
"""
import logging


class Topic(object):
    """Information of a topic object.

    @params
        id:                 Name of the given topic
        replication_factor: replication factor of a given topic
        weight:             Relative load of topic compared to other topics
                            For future use
        partitions:         List of Partition objects
    """

    def __init__(self, id, replication_factor=0, partitions=None, weight=1.0):
        self._id = id
        self._replication_factor = replication_factor
        self._partitions = partitions or []
        self.weight = weight
        logging.basicConfig()
        self.log = logging.getLogger(self.__class__.__name__)

    @property
    def id(self):
        return self._id

    @property
    def replication_factor(self):
        return self._replication_factor

    @property
    def partitions(self):
        return self._partitions

    def add_partition(self, partition):
        replication_factor_partition = len(partition.replicas)
        if self._replication_factor == 0:
            self._replication_factor = replication_factor_partition
        else:
            if (self._replication_factor != replication_factor_partition):
                self.log.warning(
                    'Replication factor for topic {topic} is not '
                    'consistent for its partitions. Replication-factor of topic'
                    ' is {repl_factor1} , Replication-factor of partition with '
                    'id:{partition} is {repl_factor2}'.format(
                        topic=self._id,
                        repl_factor1=self._replication_factor,
                        partition=partition.partition_id,
                        repl_factor2=replication_factor_partition,
                    )
                )
            pass
        self._partitions.append(partition)

    @property
    def partition_count(self):
        return len(self._partitions)
