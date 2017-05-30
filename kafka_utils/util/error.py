# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class KafkaToolError(Exception):
    """Base class for kafka tool exceptions."""
    pass


class ConfigurationError(KafkaToolError):
    """Error in configuration. For example: missing configuration file
    or misformatted configuration."""
    pass


class MissingConfigurationError(ConfigurationError):
    """Missing configuration file."""
    pass


class InvalidConfigurationError(ConfigurationError):
    """Invalid configuration file."""
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


class MaxConnectionAttemptsError(KafkaToolError):
    """Exceeded max connection attempts."""
    pass
