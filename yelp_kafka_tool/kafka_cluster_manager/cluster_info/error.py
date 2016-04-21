from yelp_kafka_tool.util.error import KafkaToolError


class InvalidBrokerIdError(KafkaToolError):
    """Raised when a broker id doesn't exist in the cluster."""
    pass


class EmptyReplicationGroupError(KafkaToolError):
    """Raised when there are no brokers in a replication group."""
    pass


class BrokerDecommissionError(KafkaToolError):
    """Raised if it is not possible to move partition out
    from decommissioned brokers.
    """
    pass
