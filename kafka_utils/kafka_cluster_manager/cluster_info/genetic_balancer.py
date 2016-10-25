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
from math import ceil

from .error import BrokerDecommissionError
from .error import InvalidBrokerIdError
from .error import InvalidPartitionError
from .error import InvalidReplicationFactorError
from .util import compute_optimum
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
        self._max_leader_movement_count = ceil(
            0.1 * len(cluster_topology.partitions)
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
        self.rebalance_replicas()

        state = _State(
            self.cluster_topology,
            brokers=self.cluster_topology.active_brokers.values()
        )
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

        assignment = state.assignment
        active_brokers = self.cluster_topology.active_brokers.values()
        inactive_brokers = [
            broker
            for broker in self.cluster_topology.brokers.values()
            if broker not in active_brokers
        ]
        for partition_name, replicas in assignment:
            for broker in inactive_brokers:
                if broker in self.cluster_topology.partitions[partition_name].replicas:
                    replicas.append(broker.id)

        self.cluster_topology.update_cluster_topology(assignment)

    def decommission_brokers(self, broker_ids):
        """Decommissioning brokers is done by removing all partitions from
        the decommissioned brokers and adding them, one-by-one, back to the
        cluster.

        :param broker_ids: List of broker ids that should be decommissioned.
        """
        decommission_brokers = []
        for broker_id in broker_ids:
            try:
                broker = self.cluster_topology.brokers[broker_id]
                broker.mark_decommissioned()
                decommission_brokers.append(broker)
            except KeyError:
                raise InvalidBrokerIdError(
                    "No broker found with id {broker_id}".format(broker_id=broker_id)
                )

        partitions = {}

        for broker in decommission_brokers:
            broker_partitions = list(broker.partitions)
            for partition in broker_partitions:
                broker.remove_partition(partition)
                partitions.setdefault(partition.name, 0)
                partitions[partition.name] += 1

        active_brokers = self.cluster_topology.active_brokers.values()

        for partition_name, count in partitions.iteritems():
            partition = self.cluster_topology.partitions[partition_name]
            try:
                self.add_replica(partition_name, count)
            except InvalidReplicationFactorError:
                raise BrokerDecommissionError(
                    "Not enough active brokers in the cluster. "
                    "Partition {partition} has replication-factor {rf}, "
                    "but only {brokers} brokers remain."
                    .format(
                        partition=partition_name,
                        rf=partition.replication_factor + count,
                        brokers=len(active_brokers)
                    )
                )

    def add_replica(self, partition_name, count=1):
        """Adding a replica is done by trying to add the replica to every
        broker in the cluster and choosing the resulting state with the
        highest fitness score.

        :param partition_name: (topic_id, partition_id) of the partition to add replicas of.
        :param count: The number of replicas to add.
        """
        try:
            partition = self.cluster_topology.partitions[partition_name]
        except KeyError:
            raise InvalidPartitionError(
                "Partition name {name} not found.".format(name=partition_name),
            )

        active_brokers = self.cluster_topology.active_brokers.values()

        if partition.replication_factor + count > len(active_brokers):
            raise InvalidReplicationFactorError(
                "Cannot increase replication factor from {0} to {1}."
                "There are only {2} active brokers."
                .format(
                    partition.replication_factor,
                    partition.replication_factor + count,
                    len(active_brokers),
                )
            )

        for _ in xrange(count):
            non_full_rgs = [
                rg for rg in self.cluster_topology.rgs.itervalues()
                if rg.count_replica(partition) < len(rg.active_brokers)
            ]
            total_replicas = sum(
                rg.count_replica(partition)
                for rg in non_full_rgs
            )
            opt_replicas, _ = compute_optimum(
                len(non_full_rgs),
                total_replicas,
            )
            under_replicated_rgs = [
                rg for rg in non_full_rgs
                if rg.count_replica(partition) < opt_replicas
            ] or non_full_rgs

            state = _State(
                self.cluster_topology,
                brokers=active_brokers
            )
            partition_index = state.partitions.index(partition)
            new_states = []

            for rg in under_replicated_rgs:
                for broker in rg.active_brokers:
                    if broker not in partition.replicas:
                        broker_index = state.brokers.index(broker)
                        new_states.append(
                            state.add_replica(partition_index, broker_index)
                        )

            state = sorted(new_states, key=self._score)[0]
            self.cluster_topology.update_cluster_topology(state.assignment)

    def remove_replica(self, partition_name, osr_broker_ids, count=1):
        """Adding a replica is done by trying to remove each replica and
        choosing the resulting state with the highest fitness score.
        Out-of-sync replicas will always be removed before in-sync replicas.

        :param partition_name: (topic_id, partition_id) of the partition to remove replicas of.
        :param osr_broker_ids: A list of the partition's out-of-sync broker ids.
        :param count: The number of replicas to remove.
        """
        try:
            partition = self.cluster_topology.partitions[partition_name]
        except KeyError:
            raise InvalidPartitionError(
                "Partition name {name} not found.".format(name=partition_name),
            )

        if partition.replication_factor - count < 1:
            raise InvalidReplicationFactorError(
                "Cannot decrease replication factor from {0} to {1}."
                "Replication factor must be at least 1."
                .format(
                    partition.replication_factor,
                    partition.replication_factor - count,
                )
            )

        osr = [
            self.cluster_topology.brokers[bid] for bid in osr_broker_ids
            if any(broker.id == bid for broker in partition.replicas)
        ]

        for _ in xrange(count):
            non_empty_rgs = [
                rg for rg in self.cluster_topology.rgs.itervalues()
                if rg.count_replica(partition) > 0
            ]
            rgs_with_osr = [
                rg for rg in non_empty_rgs
                if any(b in osr for b in rg.brokers)
            ]
            candidate_rgs = rgs_with_osr or non_empty_rgs
            total_replicas = sum(
                rg.count_replica(partition)
                for rg in candidate_rgs
            )
            opt_replicas, _ = compute_optimum(
                len(candidate_rgs),
                total_replicas,
            )
            over_replicated_rgs = [
                rg for rg in candidate_rgs
                if rg.count_replica(partition) > opt_replicas
            ] or candidate_rgs
            candidate_rgs = over_replicated_rgs or candidate_rgs

            state = _State(self.cluster_topology)
            partition_index = state.partitions.index(partition)
            new_states = []

            for rg in candidate_rgs:
                osr_brokers = [
                    broker for broker in rg.brokers
                    if broker in osr
                ]
                candidate_brokers = osr_brokers or rg.brokers
                for broker in candidate_brokers:
                    if broker in partition.replicas:
                        broker_index = state.brokers.index(broker)
                        new_states.append(
                            state.remove_replica(partition_index, broker_index)
                        )

            state = sorted(new_states, key=self._score)[0]
            self.cluster_topology.update_cluster_topology(state.assignment)
            osr = [b for b in osr if b in partition.replicas]

    def _explore(self, pop):
        """Exploration phase: Find a set of candidate states based on
        the current population.

        :param pop: The starting population for this generation.
        """
        new_pop = set(pop)
        exploration_per_state = self._exploration_attempts // len(pop)
        for state in pop:
            for _ in xrange(exploration_per_state):
                new_state = random.choice([
                    self._move_partition,
                    self._move_leadership,
                ])(state)
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
        source_rg = state.broker_rg[source]
        dest_rg = state.broker_rg[dest]
        if source_rg != dest_rg:
            source_rg_replicas = state.rg_replicas[source_rg][partition]
            dest_rg_replicas = state.rg_replicas[dest_rg][partition]
            if source_rg_replicas <= dest_rg_replicas:
                return None
        partition_size = state.partition_sizes[partition]
        if state.movement_size + partition_size > self._max_movement_size:
            return None
        return state.move(partition, source, dest)

    def _move_leadership(self, state):
        """Return a new state that is the result of randomly reassigning the
        leader for a single partition.

        :param state: The starting state.
        """
        partition = random.randint(0, len(self.cluster_topology.partitions) - 1)
        if state.partition_weights[partition] == 0:
            return None
        if len(state.replicas[partition]) <= 1:
            return None
        dest_index = random.randint(1, len(state.replicas[partition]) - 1)
        dest = state.replicas[partition][dest_index]
        if state.leader_movement_count >= self._max_leader_movement_count:
            return None
        return state.move_leadership(partition, dest)

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
            -1 * state.movement_size / self._max_movement_size +
            -1 * state.broker_leader_weight_cv +
            -1 * state.leader_movement_count / self._max_leader_movement_count +
            -1 * state.weighted_topic_broker_imbalance
        )


class _State(object):
    """An internal representation of a cluster's state used in GeneticBalancer.
    This representation stores precomputed sums and values that make
    calculating the score of the state much faster. The state refers to
    partitions, topics, brokers, and replication-groups by their index in a
    list rather than their object to make comparisons and lookups faster.

    :param cluster_topology: The ClusterTopology that this state should model.
    :param brokers: A subset of the brokers in cluster_topology that should be
        modeled. Default: all brokers in the cluster.
    """

    def __init__(self, cluster_topology, brokers=None):
        self.cluster_topology = cluster_topology
        self.partitions = cluster_topology.partitions.values()
        self.topics = cluster_topology.topics.values()
        self.brokers = brokers or cluster_topology.brokers.values()
        self.rgs = cluster_topology.rgs.values()

        # A list mapping a partition index to the list of replicas for that
        # partition.
        self.replicas = [
            [
                self.brokers.index(broker)
                for broker in partition.replicas
                if broker in self.brokers
            ]
            for partition in self.partitions
        ]

        # A list mapping a partition index to the partition's topic index.
        self.partition_topic = [
            self.topics.index(partition.topic)
            for partition in self.partitions
        ]

        # A list mapping a partition index to the weight of that partition.
        self.partition_weights = [
            partition.weight for partition in self.partitions
        ]

        # A list mapping a topic index to the weight of that topic.
        self.topic_weights = [
            topic.weight for topic in self.topics
        ]

        # A list mapping a broker index to the weight of that broker.
        self.broker_weights = [
            broker.weight for broker in self.brokers
        ]

        # A list mapping a broker index to the leader weight of that broker.
        self.broker_leader_weights = [
            broker.leader_weight for broker in self.brokers
        ]

        # The total weight of all partition replicas on the cluster.
        self.total_weight = sum(
            partition.weight * partition.replication_factor
            for partition in self.partitions
        )

        # A list mapping a partition index to the size of that partition.
        self.partition_sizes = [
            partition.size for partition in self.partitions
        ]

        # A list mapping a topic index to the number of replicas of the
        # topic's partitions.
        self.topic_replica_count = [
            sum(partition.replication_factor for partition in topic.partitions)
            for topic in self.topics
        ]

        # A list mapping a topic index to a list. That list is a map from a
        # broker index to the number of partitions of the topic on the broker.
        self.topic_broker_count = [
            [
                sum(
                    1 for partition in topic.partitions
                    if broker in partition.replicas and broker in self.brokers
                )
                for broker in self.brokers
            ]
            for topic in self.topics
        ]

        # A list mapping a topic index to the number of partition movements
        # required to have all partitions of that topic optimally balanced
        # across all brokers in the cluster.
        self.topic_broker_imbalance = [
            self._calculate_topic_imbalance(topic)
            for topic in xrange(len(self.topics))
        ]

        # A list mapping a broker index to the index of the replication group
        # that the broker belongs to.
        self.broker_rg = [
            self.rgs.index(broker.replication_group) for broker in self.brokers
        ]

        # A list mapping a replication group index to a list. That list is a
        # map from a partition index to the number of replicas of that
        # partition in the replication group.
        self.rg_replicas = [
            [
                sum(
                    1 for broker in rg.brokers
                    if broker in partition.replicas and broker in self.brokers
                )
                for partition in self.partitions
            ]
            for rg in self.rgs
        ]

        # The total size of the partitions that have been moved to reach this
        # state.
        self.movement_size = 0

        # The number of the leadership changes that have been made to reach
        # this state.
        self.leader_movement_count = 0

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

        # Update the broker leader weights
        if source_index == 0:
            new_state.broker_leader_weights = self.broker_leader_weights[:]
            new_state.broker_leader_weights[source] -= partition_weight
            new_state.broker_leader_weights[dest] += partition_weight

        # Update the topic broker counts
        topic = self.partition_topic[partition]
        new_state.topic_broker_count = self.topic_broker_count[:]
        new_state.topic_broker_count[topic] = self.topic_broker_count[topic][:]
        new_state.topic_broker_count[topic][source] -= 1
        new_state.topic_broker_count[topic][dest] += 1

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = self.topic_broker_imbalance[:]
        new_state.topic_broker_imbalance[topic] = \
            new_state._calculate_topic_imbalance(topic)

        # Update the replication group replica counts
        source_rg = self.broker_rg[source]
        dest_rg = self.broker_rg[dest]
        if source_rg != dest_rg:
            new_state.rg_replicas = self.rg_replicas[:]
            new_state.rg_replicas[source_rg] = self.rg_replicas[source_rg][:]
            new_state.rg_replicas[dest_rg] = self.rg_replicas[dest_rg][:]
            new_state.rg_replicas[source_rg][partition] -= 1
            new_state.rg_replicas[dest_rg][partition] += 1

        # Update the movement sizes
        new_state.movement_size += self.partition_sizes[partition]
        new_state.leader_movement_count += 1

        return new_state

    def move_leadership(self, partition, new_leader):
        """Return a new state that is the result of changing the leadership of
        a single partition.

        :param partition: The partition index of the partition to change the
            leadership of.
        :param new_leader: The broker index of the new leader replica.
        """
        new_state = copy(self)

        # Update the partition replica list
        new_state.replicas = self.replicas[:]
        new_state.replicas[partition] = self.replicas[partition][:]
        source = new_state.replicas[partition][0]
        new_leader_index = new_state.replicas[partition].index(new_leader)
        (
            new_state.replicas[partition][0],
            new_state.replicas[partition][new_leader_index],
        ) = (
            new_state.replicas[partition][new_leader_index],
            new_state.replicas[partition][0],
        )

        # Update the broker leader weight list
        partition_weight = self.partition_weights[partition]
        new_state.broker_leader_weights = self.broker_leader_weights[:]
        new_state.broker_leader_weights[source] -= partition_weight
        new_state.broker_leader_weights[new_leader] += partition_weight

        # Update the total leader movement size
        new_state.leader_movement_count += 1

        return new_state

    def add_replica(self, partition, broker):
        new_state = copy(self)

        # Add replica to partition replica list
        new_state.replicas = self.replicas[:]
        new_state.replicas[partition] = self.replicas[partition][:]
        new_state.replicas[partition].append(broker)

        # Update the broker weight
        partition_weight = self.partition_weights[partition]
        new_state.broker_weights = self.broker_weights[:]
        new_state.broker_weights[broker] += partition_weight

        # Update the topic broker counts
        topic = self.partition_topic[partition]
        new_state.topic_broker_count = self.topic_broker_count[:]
        new_state.topic_broker_count[topic] = self.topic_broker_count[topic][:]
        new_state.topic_broker_count[topic][broker] += 1

        # Update topic replica count
        self.topic_replica_count[topic] += 1

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = self.topic_broker_imbalance[:]
        new_state.topic_broker_imbalance[topic] = \
            new_state._calculate_topic_imbalance(topic)

        # Update the replication group replica counts
        rg = self.broker_rg[broker]
        new_state.rg_replicas = self.rg_replicas[:]
        new_state.rg_replicas[rg] = self.rg_replicas[rg][:]
        new_state.rg_replicas[rg][partition] += 1

        return new_state

    def remove_replica(self, partition, broker):
        new_state = copy(self)

        # Add replica to partition replica list
        new_state.replicas = self.replicas[:]
        new_state.replicas[partition] = self.replicas[partition][:]
        new_state.replicas[partition].remove(broker)

        # Update the broker weight
        partition_weight = self.partition_weights[partition]
        new_state.broker_weights = self.broker_weights[:]
        new_state.broker_weights[broker] -= partition_weight

        # Update the topic broker counts
        topic = self.partition_topic[partition]
        new_state.topic_broker_count = self.topic_broker_count[:]
        new_state.topic_broker_count[topic] = self.topic_broker_count[topic][:]
        new_state.topic_broker_count[topic][broker] -= 1

        # Update topic replica count
        self.topic_replica_count[topic] -= 1

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = self.topic_broker_imbalance[:]
        new_state.topic_broker_imbalance[topic] = \
            new_state._calculate_topic_imbalance(topic)

        # Update the replication group replica counts
        rg = self.broker_rg[broker]
        new_state.rg_replicas = self.rg_replicas[:]
        new_state.rg_replicas[rg] = self.rg_replicas[rg][:]
        new_state.rg_replicas[rg][partition] -= 1

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

    @property
    def broker_leader_weight_cv(self):
        return coefficient_of_variation(self.broker_leader_weights)

    @property
    def weighted_topic_broker_imbalance(self):
        return sum(
            self.topic_weights[topic] * imbalance
            for topic, imbalance in enumerate(self.topic_broker_imbalance)
        ) / self.total_weight

    def _calculate_topic_imbalance(self, topic):
        topic_optimum, _ = compute_optimum(
            len(self.brokers),
            self.topic_replica_count[topic],
        )
        return max(
            sum(
                topic_optimum - count
                for count in self.topic_broker_count[topic]
                if count < topic_optimum
            ),
            sum(
                count - topic_optimum - 1
                for count in self.topic_broker_count[topic]
                if count > topic_optimum
            ),
        )
