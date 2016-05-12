"""This class contains information for a topic object.

Useful as part of reassignment project when deciding upon moving
partitions of same topic over different brokers.
"""
import logging


class Topic(object):
    """Information of a topic object.

    :params
        id:                 Name of the given topic
        replication_factor: replication factor of a given topic
        weight:             Relative load of topic compared to other topics
                            For future use
        partitions:         List of Partition objects
    """

    def __init__(self, id, replication_factor=0, partitions=None, weight=1.0):
        self._id = id
        self._replication_factor = replication_factor
        self._partitions = partitions or set([])
        self.weight = weight
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
        self._partitions.add(partition)

    def __str__(self):
        return "{0}".format(self._id)

    def __repr__(self):
        return "{0}".format(self)
