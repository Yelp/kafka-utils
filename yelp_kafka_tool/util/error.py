class KafkaToolError(Exception):
    """Base class for kafka tool exceptions."""
    pass


class ConfigurationError(KafkaToolError):
    """Error in configuration. For example: missing configuration file
    or misformatted configuration."""
    pass


class InvalidOffsetStorageError(KafkaToolError):
    """Unknown source of offsets."""
    pass


class UnknownTopic(KafkaToolError):
    """Topic does not exist in kafka."""
    pass


class UnknownPartitions(KafkaToolError):
    """Partition doesn't exist in kafka."""
    pass


class OffsetCommitError(KafkaToolError):
    """Error during offset commit."""

    def __init__(self, topic, partition, error):
        self.topic = topic
        self.partition = partition
        self.error = error

    def __eq__(self, other):
        if all([
            self.topic == other.topic,
            self.partition == other.partition,
            self.error == other.error,
        ]):
            return True
        return False
