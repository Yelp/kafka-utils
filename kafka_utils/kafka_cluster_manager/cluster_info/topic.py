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
"""This class contains information for a topic object.

Useful as part of reassignment project when deciding upon moving
partitions of same topic over different brokers.
"""
from __future__ import absolute_import

import logging


class Topic(object):
    """Information of a topic object.

    :params
        id:                 Name of the given topic
        replication_factor: replication factor of a given topic
        partitions:         List of Partition objects
    """

    def __init__(self, id, replication_factor=0, partitions=None):
        self._id = id
        self._replication_factor = replication_factor
        self._partitions = partitions or set([])
        self.log = logging.getLogger(self.__class__.__name__)

    @property
    def id(self):
        return self._id

    @property
    def replication_factor(self):
        return self._replication_factor

    @property
    def partitions(self):
        return self._partitions

    @property
    def weight(self):
        return sum(
            partition.weight * partition.replication_factor
            for partition in self._partitions
        )

    def add_partition(self, partition):
        self._partitions.add(partition)

    def __str__(self):
        return "{0}".format(self._id)

    def __repr__(self):
        return "{0}".format(self)
