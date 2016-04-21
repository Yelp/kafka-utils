class InvalidBrokerIdError(Exception):
    """Raised when a broker id doesn't exist in the cluster"""
    pass


class InvalidPartitionError(Exception):
    """Raised when a partition tuple (topic, partition) doesn't exist in the cluster"""
    pass


class EmptyReplicationGroupError(Exception):
    """Raised when there are no brokers in a replication group"""
    pass


class BrokerDecommissionError(Exception):
    """Raised it is not possible to move partition out from decommissioned brokers"""
    pass
