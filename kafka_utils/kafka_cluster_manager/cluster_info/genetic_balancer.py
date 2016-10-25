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
import random
from copy import copy

from kafka_utils.kafka_cluster_manager.cluster_info.cluster_balancer \
    import ClusterBalancer
from kafka_utils.kafka_cluster_manager.cluster_info.stats \
    import coefficient_of_variation


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
        self._num_gens = 100
        self._max_pop = 25
        self._exploration_attempts = 1000
        self._max_movement_size = 0.1 * sum(
            partition.size * partition.replication_factor
            for partition in cluster_topology.partitions.itervalues()
        )

    def rebalance(self):
        """The genetic rebalancing algorithm runs for a fixed number of
        generations. Each generation has two phases: exploration and pruning.
        In exploration, a large set of possible states are found by randomly
        applying assignment changes to the existing states. In pruning, each
        state is given a score based on the balance of the cluster and the
        states with the highest scores are chosen as the starting states for
        the next generation.
        """
        state = _State(self.cluster_topology)
        pop = set([state])
        for i in xrange(self._num_gens):
            pop_candidates = self._explore(pop)
            pop = self._prune(pop_candidates)
            self.log.debug(
                "Generation %d: keeping %d of %d assignment(s)",
                i,
                len(pop),
                len(pop_candidates),
            )
        state = sorted(pop, key=self._score, reverse=True)[0]
        self.log.debug("Total movement size: %f", state.movement_size)

        self.cluster_topology.update_cluster_topology(state.assignment)

    def decommission_brokers(self, broker_ids):
        raise NotImplementedError("Not implemented.")

    def add_replica(self, partition_name, count=1):
        raise NotImplementedError("Not implemented.")

    def remove_replica(self, partition_name, osr, count=1):
        raise NotImplementedError("Not implemented.")

    def _explore(self, pop):
        """Exploration phase: Find a set of candidate states based on
        the current population.

        :param pop: The starting population for this generation.
        """
        new_pop = set(pop)
        exploration_per_state = self._exploration_attempts // len(pop)
        for state in pop:
            for _ in xrange(exploration_per_state):
                new_state = self._move_partition(state)
                if new_state:
                    new_pop.add(new_state)

        return new_pop or pop

    def _move_partition(self, state):
        """Return a new state that is the result of randomly moving a single
        partition from one broker to another.

        :param state: The starting state.
        """
        partition = random.randint(0, len(self.cluster_topology.partitions) - 1)
        if state.partition_weights[partition] == 0:
            return None
        source = random.choice(state.replicas[partition])
        dest = random.randint(0, len(self.cluster_topology.brokers) - 1)
        if dest in state.replicas[partition]:
            return None
        return state.move(partition, source, dest)

    def _prune(self, pop_candidates):
        """Choose a subset of the candidate states to continue on to the next
        generation.

        :param pop_candidates: The set of candidate states.
        """
        return set(
            sorted(pop_candidates, key=self._score, reverse=True)
            [:self._max_pop]
        )

    def _score(self, state):
        """Score a state based on how balanced it is. A higher score represents
        a more balanced state.

        :param state: The state to score.
        """
        return (
            -1 * state.broker_weight_cv +
            -1 * state.movement_size / self._max_movement_size
        )


class _State(object):
    """An internal representation of a cluster's state used in GeneticBalancer.
    This representation stores precomputed sums and values that make
    calculating the score of the state much faster. The state refers to
    partitions, topics, brokers, and replication-groups by their index in a
    list rather than their object to make comparisons and lookups faster.

    :param cluster_topology: The ClusterTopology that this state should model.
    """

    def __init__(self, cluster_topology):
        self.cluster_topology = cluster_topology
        self.partitions = cluster_topology.partitions.values()
        self.brokers = cluster_topology.brokers.values()

        # A list mapping a partition index to the list of replicas for that
        # partition.
        self.replicas = [
            [
                self.brokers.index(broker)
                for broker in partition.replicas
            ]
            for partition in self.partitions
        ]

        # A list mapping a partition index to the weight of that partition.
        self.partition_weights = [
            partition.weight for partition in self.partitions
        ]

        # A list mapping a broker index to the weight of that broker.
        self.broker_weights = [
            broker.weight for broker in self.brokers
        ]

        # A list mapping a partition index to the size of that partition.
        self.partition_sizes = [
            partition.size for partition in self.partitions
        ]

        # The total size of the partitions that have been moved to reach this
        # state.
        self.movement_size = 0

    def move(self, partition, source, dest):
        """Return a new state that is the result of moving a single partition.

        :param partition: The partition index of the partition to move.
        :param source: The broker index of the broker to move the partition
            from.
        :param dest: The broker index of the broker to move the partition to.
        """
        new_state = copy(self)

        # Update the partition replica list
        new_state.replicas = self.replicas[:]
        new_state.replicas[partition] = self.replicas[partition][:]
        source_index = new_state.replicas[partition].index(source)
        new_state.replicas[partition][source_index] = dest

        # Update the broker weights
        partition_weight = self.partition_weights[partition]
        new_state.broker_weights = self.broker_weights[:]
        new_state.broker_weights[source] -= partition_weight
        new_state.broker_weights[dest] += partition_weight

        # Update the movement sizes
        new_state.movement_size += self.partition_sizes[partition]

        return new_state

    @property
    def assignment(self):
        """Return the partition assignment that this state represents."""
        return {
            partition.name: [
                self.brokers[bid].id for bid in self.replicas[pid]
            ]
            for pid, partition in enumerate(self.partitions)
        }

    @property
    def broker_weight_cv(self):
        """Return the coefficient of variation of the weight of the brokers."""
        return coefficient_of_variation(self.broker_weights)
