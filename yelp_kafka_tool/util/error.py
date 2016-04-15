class InvalidOffsetStorageError(Exception):
    """Unknown source of offsets."""
    pass


class UnknownTopic(Exception):
    pass


class UnknownPartitions(Exception):
    pass


class OffsetCommitError(Exception):

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
