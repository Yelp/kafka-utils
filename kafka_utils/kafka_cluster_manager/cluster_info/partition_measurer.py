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
from __future__ import absolute_import

import itertools
import shlex


class PartitionMeasurer(object):
    """An interface used to gather metrics about a partition.

    :param cluster_config: ClusterConfig for the cluster.
    :param brokers: List of the cluster's brokers.
    :param assignment: The cluster's assignment.
    :param args: Namespace containing the command line arguments.
    """

    def __init__(
            self,
            cluster_config,
            brokers,
            assignment,
            args,
    ):
        self.cluster_config = cluster_config
        self.brokers = brokers
        self.assignment = assignment
        self.args = args
        if hasattr(args, 'measurer_args'):
            self.parse_args(list(itertools.chain.from_iterable(
                shlex.split(arg) for arg in args.measurer_args
            )))
        else:
            self.parse_args([])

    def parse_args(self, _measurer_args):
        """Parse partition measurer command line arguments.

        :param _measurer_args: The list of arguments as strings.
        """
        pass

    def get_weight(self, partition_name):
        """Return a positive number representing the relative weight of this
        partition compared to the other partitions in the cluster. The weight
        is a measure of how much load this partition will place on any broker
        that it is assigned to.

        :param partition_name: A tuple with the topic id and partition id as the first and second elements respectively.
        """
        raise NotImplementedError("Implement in subclass.")

    def get_size(self, partition_name):
        """Return a positive number representing the size of this partition.
        The size is a measure of how expensive it is to move this partition
        from one broker to another.

        :param partition_name: A tuple with the topic id and partition id as the first and second elements respectively.
        """
        raise NotImplementedError("Implement in subclass.")


class UniformPartitionMeasurer(PartitionMeasurer):
    """An implementation of PartitionMeasurer that provides identital metrics
    for all partitions.
    """

    def get_weight(self, _):
        return 1.0

    def get_size(self, _):
        return 1.0
