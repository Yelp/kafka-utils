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
from kafka_tools.util.error import KafkaToolError


class InvalidBrokerIdError(KafkaToolError):
    """Raised when a broker id doesn't exist in the cluster."""
    pass


class InvalidPartitionError(KafkaToolError):
    """Raised when a partition tuple (topic, partition) doesn't exist in the cluster"""
    pass


class EmptyReplicationGroupError(KafkaToolError):
    """Raised when there are no brokers in a replication group."""
    pass


class BrokerDecommissionError(KafkaToolError):
    """Raised if it is not possible to move partition out
    from decommissioned brokers.
    """
    pass


class NotEligibleGroupError(KafkaToolError):
    """Raised when there are no brokers eligible to acquire a certain partition
    in a replication group.
    """
    pass


class RebalanceError(KafkaToolError):
    """Raised when a rebalance operation is not possible."""
    pass
