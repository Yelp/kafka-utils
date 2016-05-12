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


class Partition(object):
    """Class representing the partition object.
    It contains topic-partition_id tuple as name, topic and replicas
    (list of brokers).
    """

    def __init__(self, topic, id, replicas=None):
        # Every partition name has (topic, partition) tuple
        self._name = (topic.id, id)
        self._replicas = replicas or []
        self._topic = topic

    @property
    def name(self):
        "Name of partition, consisting of (topic_id, partition_id) tuple."""
        return self._name

    @property
    def partition_id(self):
        """Partition id component of the partition-tuple."""
        return int(self._name[1])

    @property
    def topic(self):
        return self._topic

    @property
    def replicas(self):
        """List of brokers in partition."""
        return self._replicas

    @property
    def leader(self):
        """Leader broker for the partition."""
        return self._replicas[0]

    @property
    def replication_factor(self):
        return len(self._replicas)

    @property
    def followers(self):
        """Return list of brokers not as preferred leader
        for a particular partition.
        """
        return self._replicas[1:]

    def add_replica(self, broker):
        """Add broker to existing set of replicas."""
        self._replicas.append(broker)

    def swap_leader(self, new_leader):
        """Change the preferred leader with one of
        given replicas.

        Note: Leaders for all the replicas of current
        partition needs to be changed.
        """
        # Replica set cannot be changed
        assert(new_leader in self._replicas)
        curr_leader = self.leader
        idx = self._replicas.index(new_leader)
        self._replicas[0], self._replicas[idx] = \
            self._replicas[idx], self._replicas[0]
        return curr_leader

    def replace(self, source, dest):
        """Replace source broker with destination broker in replica set if found."""
        for i, broker in enumerate(self.replicas):
            if broker == source:
                self.replicas[i] = dest
                return

    def count_siblings(self, partitions):
        """Count siblings of partition in given partition-list.

        :key-term:
        sibling:    partitions with same topic
        """
        count = sum(
            int(self.topic == partition.topic)
            for partition in partitions
        )
        return count

    def __str__(self):
        return "{name}".format(name=self._name)

    def __repr__(self):
        return "{0}".format(self)
