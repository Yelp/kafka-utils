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
import logging

from kafka_utils.kafka_cluster_manager.cluster_info.cluster_balancer \
    import ClusterBalancer


class GeneticBalancer(ClusterBalancer):
    """An implementation of cluster rebalancing that tries to achieve balance
    using a genetic algorithm.

    :param cluster_topology: The ClusterTopology object that should be acted
        on.
    :param args: The program arguments.
    """

    def __init__(self, cluster_topology, args):
        super(GeneticBalancer, self).__init__(cluster_topology, args)
        self.log = logging.getLogger(self.__class__.__name__)

    def rebalance(self):
        raise NotImplementedError("Not implemented.")

    def decommission_brokers(self, broker_ids):
        raise NotImplementedError("Not implemented.")

    def add_replica(self, partition_name, count=1):
        raise NotImplementedError("Not implemented.")

    def remove_replica(self, partition_name, osr, count=1):
        raise NotImplementedError("Not implemented.")
