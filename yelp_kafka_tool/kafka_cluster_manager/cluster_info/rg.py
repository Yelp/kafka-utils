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
        return [
            partition
            for broker in self._brokers
            for partition in broker.partitions
        ]
