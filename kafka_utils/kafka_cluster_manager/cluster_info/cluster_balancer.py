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


class ClusterBalancer(object):
    """Interface that is used to implement any cluster partition balancing approach.

    :param cluster_topology: The ClusterTopology object that should be acted on.
    :param args: The program arguments.
    """

    def __init__(self, cluster_topology, args):
        self.cluster_topology = cluster_topology
        self.args = args

    def rebalance(self):
        """Rebalance partitions across the brokers in the cluster."""
        raise NotImplementedError("Implement in subclass")

    def decommission_brokers(self, broker_ids):
        """Decommission a broker and balance all of its partitions across the cluster.

        :param broker_ids: A list of strings representing valid broker ids in the cluster.
        :raises InvalidBrokerIdError: A broker id is invalid.
        """
        raise NotImplementedError("Implement in subclass")

    def add_replica(self, partition_id, count=1):
        """Add replicas of a partition to the cluster, while maintaining the cluster's balance.

        :param partition_id: The id of the partition to add replicas of.
        :param count: The number of replicas to add.

        :raises InvalidReplicationFactorError: The resulting replication factor is invalid.
        """
        raise NotImplementedError("Implement in subclass")

    def remove_replica(self, osr, partition_id, count=1):
        """Remove replicas of a partition from the cluster, while maintaining the cluster's balance.

        :param partition_id: The id of the partition to add replicas of.
        :param osr: The set of out-of-sync replicas for this partition.
        :param count: The number of replicas to add.

        :raises InvalidReplicationFactorError: The resulting replication factor is invalid.
        """
        raise NotImplementedError("Implement in subclass")
