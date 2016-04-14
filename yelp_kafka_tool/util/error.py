class KafkaToolError(Exception):
    """Base class for kafka tool exceptions"""
    pass


class ConfigurationError(KafkaToolError):
    """Error in configuration. For example. Missing configuration file
    or misformatted configuration."""
    pass
