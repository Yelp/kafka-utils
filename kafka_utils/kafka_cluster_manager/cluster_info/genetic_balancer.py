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
from __future__ import division

import argparse
import logging
import random
import time
from collections import defaultdict
from copy import copy
from math import sqrt

import six
from six.moves import range

from .error import BrokerDecommissionError
from .error import InvalidBrokerIdError
from .error import InvalidPartitionError
from .error import InvalidReplicationFactorError
from .util import compute_optimum
from kafka_utils.kafka_cluster_manager.cluster_info.cluster_balancer \
    import ClusterBalancer
from kafka_utils.kafka_cluster_manager.cluster_info.stats \
    import coefficient_of_variation
from kafka_utils.util import positive_float
from kafka_utils.util import positive_int
from kafka_utils.util import tuple_alter
from kafka_utils.util import tuple_remove
from kafka_utils.util import tuple_replace

RANDOM_SEED = 0xcafca
DEFAULT_NUM_GENS = 100
DEFAULT_MAX_POP = 50
DEFAULT_MAX_EXPLORATION = 10000

# In practice, overall weight is more important than leader weight which is
# more important than topic-broker imbalance so different weights are used
# to adjust for this.
DEFAULT_PARTITION_WEIGHT_CV_SCORE_WEIGHT = 0.6
DEFAULT_LEADER_WEIGHT_CV_SCORE_WEIGHT = 0.3
DEFAULT_TOPIC_BROKER_IMBALANCE_SCORE_WEIGHT = 0.1
# Movement size is included in the scoring function to prevent fewer large
# movements from being favored over more smaller movements (since only one
# partition is moved each generation). This weight is smaller than the others
# because a larger weight can prevent movement altogether (since only a small
# improvement in score is expected most of the time).
DEFAULT_MOVEMENT_SIZE_SCORE_WEIGHT = 0.01
DEFAULT_LEADER_CHANGE_SCORE_WEIGHT = 0.001


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

    def _set_arg_default(self, arg, value):
        if not hasattr(self.args, arg):
            setattr(self.args, arg, value)

    def parse_args(self, balancer_args):
        # If the command being run is not rebalance, then these arguments
        # won't exist
        self._set_arg_default('replication_groups', False)
        self._set_arg_default('brokers', False)
        self._set_arg_default('leaders', False)
        self._set_arg_default('max_partition_movements', None)
        self._set_arg_default('max_movement_size', None)
        self._set_arg_default('max_leader_changes', None)

        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            description='Perform cluster rebalancing using a genetic algorithm.',
        )
        parser.add_argument(
            '--num-gens',
            type=positive_int,
            default=DEFAULT_NUM_GENS,
            help='Number of generations of the genetic algorithm to perform. '
            'Should be at leasn max-partition-movements + max-leader-changes.'
            ' Default: %(default)',
        )
        parser.add_argument(
            '--max-pop',
            type=positive_int,
            default=DEFAULT_MAX_POP,
            help='Maximum population carried over between generations. '
            'Default: %(default)',
        )
        parser.add_argument(
            '--max-exploration',
            type=positive_int,
            default=DEFAULT_MAX_EXPLORATION,
            help='Maximum exploration attempts to make each generation. '
            'Default: %(default)',
        )
        parser.add_argument(
            '--partition-weight-cv-score-weight',
            type=positive_float,
            default=DEFAULT_PARTITION_WEIGHT_CV_SCORE_WEIGHT,
            help='How much to value partition weight imbalance when scoring'
            'assignments during the genetic algorithm. Default: %(default)',
        )
        parser.add_argument(
            '--leader-weight-cv-score-weight',
            type=positive_float,
            default=DEFAULT_LEADER_WEIGHT_CV_SCORE_WEIGHT,
            help='How much to value preferred leader imbalance when scoring'
            'assignments during the genetic algorithm. Default: %(default)',
        )
        parser.add_argument(
            '--topic-broker-imbalance-score-weight',
            type=positive_float,
            default=DEFAULT_TOPIC_BROKER_IMBALANCE_SCORE_WEIGHT,
            help='How much to value topic broker imbalance when scoring'
            'assignments during the genetic algorithm. Default: %(default)',
        )
        parser.add_argument(
            '--movement-size-score-weight',
            type=positive_float,
            default=DEFAULT_MOVEMENT_SIZE_SCORE_WEIGHT,
            help='How much to value movement size when scoring assignments'
            ' during the genetic algorithm. Default: %(default)',
        )
        parser.add_argument(
            '--leader_change-score-weight',
            type=positive_float,
            default=DEFAULT_LEADER_CHANGE_SCORE_WEIGHT,
            help='How much to value leader changes when scoring assignments'
            ' during the genetic algorithm. Default: %(default)',
        )
        parser.parse_args(balancer_args, self.args)

    def rebalance(self):
        """The genetic rebalancing algorithm runs for a fixed number of
        generations. Each generation has two phases: exploration and pruning.
        In exploration, a large set of possible states are found by randomly
        applying assignment changes to the existing states. In pruning, each
        state is given a score based on the balance of the cluster and the
        states with the highest scores are chosen as the starting states for
        the next generation.
        """
        if self.args.num_gens < self.args.max_partition_movements:
            self.log.warning(
                "num-gens ({num_gens}) is less than max-partition-movements"
                " ({max_partition_movements}). max-partition-movements will"
                " never be reached.".format(
                    num_gens=self.args.num_gens,
                    max_partition_movements=self.args.max_partition_movements,
                )
            )

        if self.args.replication_groups:
            self.log.info("Rebalancing replicas across replication groups...")
            rg_movement_count, rg_movement_size = self.rebalance_replicas(
                max_movement_count=self.args.max_partition_movements,
                max_movement_size=self.args.max_movement_size,
            )
            self.log.info(
                "Done rebalancing replicas. %d partitions moved.",
                rg_movement_count,
            )
        else:
            rg_movement_size = 0
            rg_movement_count = 0

        # Use a fixed random seed to make results reproducible.
        random.seed(RANDOM_SEED)

        # NOTE: only active brokers are considered when rebalancing
        state = _State(
            self.cluster_topology,
            brokers=self.cluster_topology.active_brokers
        )
        state.movement_size = rg_movement_size
        pop = {state}

        do_rebalance = self.args.brokers or self.args.leaders

        # Cannot rebalance when all partitions have zero weight because the
        # score function is undefined.
        if do_rebalance and not state.total_weight:
            self.log.error(
                "Rebalance impossible. All partitions have zero weight.",
            )
            do_rebalance = False

        if do_rebalance:
            self.log.info("Rebalancing with genetic algorithm.")
            # Run the genetic algorithm for a fixed number of generations.
            for i in range(self.args.num_gens):
                start = time.time()
                pop_candidates = self._explore(pop)
                pop = self._prune(pop_candidates)
                end = time.time()
                self.log.debug(
                    "Generation %d: keeping %d of %d assignment(s) in %f seconds",
                    i,
                    len(pop),
                    len(pop_candidates),
                    end - start,
                )

        # Choose the state with the greatest score.
        state = sorted(pop, key=self._score, reverse=True)[0]
        self.log.info(
            "Done rebalancing. %d partitions moved.",
            state.movement_count,
        )
        self.log.info("Total movement size: %f", state.movement_size)
        assignment = state.assignment

        # Since only active brokers are considered when rebalancing, inactive
        # brokers need to be added back to the new assignment.
        all_brokers = set(self.cluster_topology.brokers.values())
        inactive_brokers = all_brokers - set(state.brokers)
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

        partitions = defaultdict(int)

        # Remove all partitions from decommissioned brokers.
        for broker in decommission_brokers:
            broker_partitions = list(broker.partitions)
            for partition in broker_partitions:
                broker.remove_partition(partition)
                partitions[partition.name] += 1

        active_brokers = self.cluster_topology.active_brokers

        # Add partition replicas to active brokers one-by-one.
        for partition_name, count in six.iteritems(partitions):
            partition = self.cluster_topology.partitions[partition_name]
            try:
                self.add_replica(partition_name, count)
            except InvalidReplicationFactorError:
                raise BrokerDecommissionError(
                    "Not enough active brokers in the cluster. "
                    "Partition {partition} has replication-factor {rf}, "
                    "but only {brokers} active brokers remain."
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

        active_brokers = self.cluster_topology.active_brokers

        if partition.replication_factor + count > len(active_brokers):
            raise InvalidReplicationFactorError(
                "Cannot increase replication factor from {rf} to {new_rf}."
                " There are only {brokers} active brokers."
                .format(
                    rf=partition.replication_factor,
                    new_rf=partition.replication_factor + count,
                    brokers=len(active_brokers),
                )
            )

        # Create state from current cluster topology.
        state = _State(self.cluster_topology, brokers=active_brokers)
        partition_index = state.partitions.index(partition)

        for _ in range(count):
            # Find eligible replication-groups.
            non_full_rgs = [
                rg for rg in six.itervalues(self.cluster_topology.rgs)
                if rg.count_replica(partition) < len(rg.active_brokers)
            ]
            # Since replicas can only be added to non-full rgs, only consider
            # replicas on those rgs when determining which rgs are
            # under-replicated.
            replica_count = sum(
                rg.count_replica(partition)
                for rg in non_full_rgs
            )
            opt_replicas, _ = compute_optimum(
                len(non_full_rgs),
                replica_count,
            )
            under_replicated_rgs = [
                rg for rg in non_full_rgs
                if rg.count_replica(partition) < opt_replicas
            ] or non_full_rgs

            # Add the replica to every eligible broker.
            new_states = []
            for rg in under_replicated_rgs:
                for broker in rg.active_brokers:
                    if broker not in partition.replicas:
                        broker_index = state.brokers.index(broker)
                        new_states.append(
                            state.add_replica(partition_index, broker_index)
                        )

            # Update cluster topology with highest scoring state.
            state = sorted(new_states, key=self._score, reverse=True)[0]
            self.cluster_topology.update_cluster_topology(state.assignment)

    def remove_replica(self, partition_name, osr_broker_ids, count=1):
        """Removing a replica is done by trying to remove a replica from every
        broker and choosing the resulting state with the highest fitness score.
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
                "Cannot decrease replication factor from {rf} to {new_rf}."
                "Replication factor must be at least 1."
                .format(
                    rf=partition.replication_factor,
                    new_rf=partition.replication_factor - count,
                )
            )

        osr = {
            broker for broker in partition.replicas
            if broker.id in osr_broker_ids
        }

        # Create state from current cluster topology.
        state = _State(self.cluster_topology)
        partition_index = state.partitions.index(partition)

        for _ in range(count):
            # Find eligible replication groups.
            non_empty_rgs = [
                rg for rg in six.itervalues(self.cluster_topology.rgs)
                if rg.count_replica(partition) > 0
            ]
            rgs_with_osr = [
                rg for rg in non_empty_rgs
                if any(b in osr for b in rg.brokers)
            ]
            candidate_rgs = rgs_with_osr or non_empty_rgs
            # Since replicas will only be removed from the candidate rgs, only
            # count replicas on those rgs when determining which rgs are
            # over-replicated.
            replica_count = sum(
                rg.count_replica(partition)
                for rg in candidate_rgs
            )
            opt_replicas, _ = compute_optimum(
                len(candidate_rgs),
                replica_count,
            )
            over_replicated_rgs = [
                rg for rg in candidate_rgs
                if rg.count_replica(partition) > opt_replicas
            ] or candidate_rgs
            candidate_rgs = over_replicated_rgs or candidate_rgs

            # Remove the replica from every eligible broker.
            new_states = []
            for rg in candidate_rgs:
                osr_brokers = {
                    broker for broker in rg.brokers
                    if broker in osr
                }
                candidate_brokers = osr_brokers or rg.brokers
                for broker in candidate_brokers:
                    if broker in partition.replicas:
                        broker_index = state.brokers.index(broker)
                        new_states.append(
                            state.remove_replica(partition_index, broker_index)
                        )

            # Update cluster topology with highest scoring state.
            state = sorted(new_states, key=self._score, reverse=True)[0]
            self.cluster_topology.update_cluster_topology(state.assignment)
            osr = {b for b in osr if b in partition.replicas}

    def score(self):
        return self._score(_State(self.cluster_topology), score_movement=False)

    def _explore(self, pop):
        """Exploration phase: Find a set of candidate states based on
        the current population.

        :param pop: The starting population for this generation.
        """
        new_pop = set(pop)
        exploration_per_state = self.args.max_exploration // len(pop)

        mutations = []
        if self.args.brokers:
            mutations.append(self._move_partition)
        if self.args.leaders:
            mutations.append(self._move_leadership)

        for state in pop:
            for _ in range(exploration_per_state):
                new_state = random.choice(mutations)(state)
                if new_state:
                    new_pop.add(new_state)

        return new_pop

    def _move_partition(self, state):
        """Attempt to move a random partition to a random broker. If the
        chosen movement is not possible, None is returned.

        :param state: The starting state.

        :return: The resulting State object if a movement is found. None if
            no movement is found.
        """
        partition = random.randint(0, len(self.cluster_topology.partitions) - 1)

        # Moving zero weight partitions will not improve balance for any of the
        # balance criteria. Disallow these movements here to avoid wasted
        # effort.
        if state.partition_weights[partition] == 0:
            return None

        # Choose distinct source and destination brokers.
        source = random.choice(state.replicas[partition])
        dest = random.randint(0, len(self.cluster_topology.brokers) - 1)
        if dest in state.replicas[partition]:
            return None
        source_rg = state.broker_rg[source]
        dest_rg = state.broker_rg[dest]

        # Ensure replicas remain balanced across replication groups.
        if source_rg != dest_rg:
            source_rg_replicas = state.rg_replicas[source_rg][partition]
            dest_rg_replicas = state.rg_replicas[dest_rg][partition]
            if source_rg_replicas <= dest_rg_replicas:
                return None

        # Ensure movement size capacity is not surpassed
        partition_size = state.partition_sizes[partition]
        if (self.args.max_movement_size is not None and
                state.movement_size + partition_size >
                self.args.max_movement_size):
            return None

        return state.move(partition, source, dest)

    def _move_leadership(self, state):
        """Attempt to move a random partition to a random broker. If the
        chosen movement is not possible, None is returned.

        :param state: The starting state.

        :return: The resulting State object if a leader change is found. None
            if no change is found.
        """
        partition = random.randint(0, len(self.cluster_topology.partitions) - 1)

        # Moving zero weight partitions will not improve balance for any of the
        # balance criteria. Disallow these movements here to avoid wasted
        # effort.
        if state.partition_weights[partition] == 0:
            return None
        if len(state.replicas[partition]) <= 1:
            return None
        dest_index = random.randint(1, len(state.replicas[partition]) - 1)
        dest = state.replicas[partition][dest_index]
        if (self.args.max_leader_changes is not None and
                state.leader_movement_count >= self.args.max_leader_changes):
            return None

        return state.move_leadership(partition, dest)

    def _prune(self, pop_candidates):
        """Choose a subset of the candidate states to continue on to the next
        generation.

        :param pop_candidates: The set of candidate states.
        """
        return set(
            sorted(pop_candidates, key=self._score, reverse=True)
            [:self.args.max_pop]
        )

    def _score(self, state, score_movement=True):
        """Score a state based on how balanced it is. A higher score represents
        a more balanced state.

        :param state: The state to score.
        """
        score = 0
        max_score = 0
        if state.total_weight:
            # Coefficient of variance is a value between 0 and the sqrt(n)
            # where n is the length of the series (the number of brokers)
            # so those parameters are scaled by (1 / sqrt(# or brokers)) to
            # get a value between 0 and 1.
            #
            # Since smaller imbalance values are preferred use 1 - x so that
            # higher scores correspond to more balanced states.
            score += self.args.partition_weight_cv_score_weight * \
                (1 - state.broker_weight_cv / sqrt(len(state.brokers)))
            score += self.args.leader_weight_cv_score_weight * \
                (1 - state.broker_leader_weight_cv / sqrt(len(state.brokers)))
            score += self.args.topic_broker_imbalance_score_weight * \
                (1 - state.weighted_topic_broker_imbalance)
            max_score += self.args.partition_weight_cv_score_weight
            max_score += self.args.leader_weight_cv_score_weight
            max_score += self.args.topic_broker_imbalance_score_weight

        if self.args.max_movement_size is not None and score_movement:
            score += self.args.movement_size_score_weight * \
                (1 - state.movement_size / self.args.max_movement_size)
            max_score += self.args.movement_size_score_weight

        if self.args.max_leader_changes is not None and score_movement:
            score += self.args.leader_change_score_weight * \
                (1 - state.leader_movement_count / self.args.max_leader_changes)
            max_score += self.args.leader_change_score_weight

        return score / max_score


class _State(object):
    """An internal representation of a cluster's state used in GeneticBalancer.
    This representation stores precomputed sums and values that make
    calculating the score of the state much faster. The state refers to
    partitions, topics, brokers, and replication-groups by their index in a
    tuple rather than their object to make comparisons and lookups faster.

    :param cluster_topology: The ClusterTopology that this state should model.
    :param brokers: A subset of the brokers in cluster_topology that should be
        modeled. Default: all brokers in the cluster.
    """

    def __init__(self, cluster_topology, brokers=None):
        # Use tuples instead of lists to store all state so that shallow copies
        # can be performed without the danger of accidentally mutating the
        # original object. Since dict.values() has an arbitrary order, the
        # lists are sorted so that results are reproducible.
        self.partitions = tuple(sorted(
            list(cluster_topology.partitions.values()),
            key=lambda p: p.name,
        ))
        self.topics = tuple(sorted(
            list(cluster_topology.topics.values()),
            key=lambda t: t.id,
        ))
        self.brokers = tuple(sorted(
            brokers or list(cluster_topology.brokers.values()),
            key=lambda b: b.id
        ))

        self.rgs = tuple(sorted(
            [rg for rg in cluster_topology.rgs.values() if rg.id],
            key=lambda r: r.id
        ))

        # A tuple mapping a partition index to the tuple of replicas for that
        # partition.
        self.replicas = tuple(
            tuple(
                self.brokers.index(broker)
                for broker in partition.replicas
                if broker in self.brokers
            )
            for partition in self.partitions
        )

        # A tuple mapping a partition index to the partition's topic index.
        self.partition_topic = tuple(
            self.topics.index(partition.topic)
            for partition in self.partitions
        )

        # A tuple mapping a partition index to the weight of that partition.
        self.partition_weights = tuple(
            partition.weight for partition in self.partitions
        )

        # A tuple mapping a topic index to the weight of that topic.
        self.topic_weights = tuple(
            topic.weight for topic in self.topics
        )

        # A tuple mapping a broker index to the weight of that broker.
        self.broker_weights = tuple(
            broker.weight for broker in self.brokers
        )

        # A tuple mapping a broker index to the leader weight of that broker.
        self.broker_leader_weights = tuple(
            broker.leader_weight for broker in self.brokers
        )

        # The total weight of all partition replicas on the cluster.
        self.total_weight = sum(
            partition.weight
            for broker in self.brokers
            for partition in broker.partitions
        )

        # A tuple mapping a partition index to the size of that partition.
        self.partition_sizes = tuple(
            partition.size for partition in self.partitions
        )

        # A tuple mapping a topic index to the number of replicas of the
        # topic's partitions.
        self.topic_replica_count = tuple(
            sum(partition.replication_factor for partition in topic.partitions)
            for topic in self.topics
        )

        # A tuple mapping a topic index to a tuple. That tuple is a map from a
        # broker index to the number of partitions of the topic on the broker.
        self.topic_broker_count = tuple(
            tuple(
                sum(
                    1 for partition in topic.partitions
                    if broker in partition.replicas and broker in self.brokers
                )
                for broker in self.brokers
            )
            for topic in self.topics
        )

        # A tuple mapping a topic index to the number of partition movements
        # required to have all partitions of that topic optimally balanced
        # across all brokers in the cluster.
        self.topic_broker_imbalance = tuple(
            self._calculate_topic_imbalance(topic)
            for topic in range(len(self.topics))
        )

        # A weighted sum of the imbalance values in topic_broker_imbalance.
        self._weighted_topic_broker_imbalance = sum(
            self.topic_weights[topic] * imbalance
            for topic, imbalance in enumerate(self.topic_broker_imbalance)
        )

        # A tuple mapping a broker index to the index of the replication group
        # that the broker belongs to.
        self.broker_rg = tuple(
            self.rgs.index(broker.replication_group) for broker in self.brokers
        )

        # A tuple mapping a replication group index to a tuple. That tuple is a
        # map from a partition index to the number of replicas of that
        # partition in the replication group.
        self.rg_replicas = tuple(
            tuple(
                sum(
                    1 for broker in rg.brokers
                    if broker in partition.replicas and broker in self.brokers
                )
                for partition in self.partitions
            )
            for rg in self.rgs
        )

        # The total size and count of the partitions that have been moved to
        # reach this state.
        self.movement_size = 0
        self.movement_count = 0

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

        # Update the partition replica tuple
        source_index = self.replicas[partition].index(source)
        new_state.replicas = tuple_alter(
            self.replicas,
            (partition, lambda replicas: tuple_replace(
                replicas,
                (source_index, dest),
            )),
        )

        # Update the broker weights
        partition_weight = self.partition_weights[partition]

        new_state.broker_weights = tuple_alter(
            self.broker_weights,
            (source, lambda broker_weight: broker_weight - partition_weight),
            (dest, lambda broker_weight: broker_weight + partition_weight),
        )

        # Update the broker leader weights
        if source_index == 0:
            new_state.broker_leader_weights = tuple_alter(
                self.broker_leader_weights,
                (source, lambda lw: lw - partition_weight),
                (dest, lambda lw: lw + partition_weight),
            )
            new_state.leader_movement_count += 1

        # Update the topic broker counts
        topic = self.partition_topic[partition]

        new_state.topic_broker_count = tuple_alter(
            self.topic_broker_count,
            (topic, lambda broker_count: tuple_alter(
                broker_count,
                (source, lambda count: count - 1),
                (dest, lambda count: count + 1),
            )),
        )

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = tuple_replace(
            self.topic_broker_imbalance,
            (topic, new_state._calculate_topic_imbalance(topic)),
        )

        new_state._weighted_topic_broker_imbalance = (
            self._weighted_topic_broker_imbalance +
            self.topic_weights[topic] * (
                new_state.topic_broker_imbalance[topic] -
                self.topic_broker_imbalance[topic]
            )
        )

        # Update the replication group replica counts
        source_rg = self.broker_rg[source]
        dest_rg = self.broker_rg[dest]
        if source_rg != dest_rg:
            new_state.rg_replicas = tuple_alter(
                self.rg_replicas,
                (source_rg, lambda replica_counts: tuple_alter(
                    replica_counts,
                    (partition, lambda replica_count: replica_count - 1),
                )),
                (dest_rg, lambda replica_counts: tuple_alter(
                    replica_counts,
                    (partition, lambda replica_count: replica_count + 1),
                )),
            )

        # Update the movement sizes
        new_state.movement_size += self.partition_sizes[partition]
        new_state.movement_count += 1

        return new_state

    def move_leadership(self, partition, new_leader):
        """Return a new state that is the result of changing the leadership of
        a single partition.

        :param partition: The partition index of the partition to change the
            leadership of.
        :param new_leader: The broker index of the new leader replica.
        """
        new_state = copy(self)

        # Update the partition replica tuple
        source = new_state.replicas[partition][0]
        new_leader_index = self.replicas[partition].index(new_leader)
        new_state.replicas = tuple_alter(
            self.replicas,
            (partition, lambda replicas: tuple_replace(
                replicas,
                (0, replicas[new_leader_index]),
                (new_leader_index, replicas[0]),
            )),
        )

        # Update the broker leader weights
        partition_weight = self.partition_weights[partition]
        new_state.broker_leader_weights = tuple_alter(
            self.broker_leader_weights,
            (source, lambda leader_weight: leader_weight - partition_weight),
            (new_leader, lambda leader_weight: leader_weight + partition_weight),
        )

        # Update the total leader movement size
        new_state.leader_movement_count += 1

        return new_state

    def add_replica(self, partition, broker):
        new_state = copy(self)

        # Add replica to partition replica tuple
        new_state.replicas = tuple_alter(
            self.replicas,
            (partition, lambda replicas: replicas + (broker, )),
        )

        # Update the broker weight
        partition_weight = self.partition_weights[partition]
        new_state.broker_weights = tuple_alter(
            self.broker_weights,
            (broker, lambda broker_weight: broker_weight + partition_weight),
        )

        # Update the topic weights
        topic = new_state.partition_topic[partition]
        new_state.topic_weights = tuple_alter(
            self.topic_weights,
            (topic, lambda topic_weight: topic_weight + partition_weight)
        )

        # Update the total weight
        new_state.total_weight = self.total_weight + partition_weight

        # Update the topic broker counts
        topic = self.partition_topic[partition]
        new_state.topic_broker_count = tuple_alter(
            self.topic_broker_count,
            (topic, lambda broker_counts: tuple_alter(
                broker_counts,
                (broker, lambda count: count + 1),
            )),
        )

        # Update topic replica count
        new_state.topic_replica_count = tuple_alter(
            self.topic_replica_count,
            (topic, lambda replica_count: replica_count + 1),
        )

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = tuple_replace(
            self.topic_broker_imbalance,
            (topic, new_state._calculate_topic_imbalance(topic)),
        )

        new_state._weighted_topic_broker_imbalance = (
            self._weighted_topic_broker_imbalance -
            self.topic_weights[topic] * self.topic_broker_imbalance[topic] +
            new_state.topic_weights[topic] * new_state.topic_broker_imbalance[topic]
        )

        # Update the replication group replica counts
        rg = self.broker_rg[broker]
        new_state.rg_replicas = tuple_alter(
            self.rg_replicas,
            (rg, lambda replica_counts: tuple_alter(
                replica_counts,
                (partition, lambda count: count + 1),
            )),
        )

        return new_state

    def remove_replica(self, partition, broker):
        new_state = copy(self)

        # Add replica to partition replica tuple
        new_state.replicas = tuple_alter(
            self.replicas,
            (partition, lambda replicas: tuple_remove(replicas, broker)),
        )

        # Update the broker weight
        partition_weight = self.partition_weights[partition]
        new_state.broker_weights = tuple_alter(
            self.broker_weights,
            (broker, lambda broker_weight: broker_weight - partition_weight),
        )

        # Update the topic weights
        topic = self.partition_topic[partition]
        new_state.topic_weights = tuple_alter(
            self.topic_weights,
            (topic, lambda topic_weight: topic_weight - partition_weight)
        )

        # Update the total weight
        new_state.total_weight = self.total_weight - partition_weight

        # Update the topic broker counts
        topic = self.partition_topic[partition]
        new_state.topic_broker_count = tuple_alter(
            self.topic_broker_count,
            (topic, lambda broker_count: tuple_alter(
                broker_count,
                (broker, lambda count: count - 1),
            )),
        )

        # Update topic replica count
        new_state.topic_replica_count = tuple_alter(
            self.topic_replica_count,
            (topic, lambda replica_count: replica_count - 1),
        )

        # Update the topic broker imbalance
        new_state.topic_broker_imbalance = tuple_replace(
            self.topic_broker_imbalance,
            (topic, new_state._calculate_topic_imbalance(topic)),
        )

        new_state._weighted_topic_broker_imbalance = (
            self._weighted_topic_broker_imbalance -
            self.topic_weights[topic] * self.topic_broker_imbalance[topic] +
            new_state.topic_weights[topic] * new_state.topic_broker_imbalance[topic]
        )

        # Update the replication group replica counts
        rg = self.broker_rg[broker]
        new_state.rg_replicas = tuple_alter(
            self.rg_replicas,
            (rg, lambda replicas: tuple_alter(
                replicas,
                (partition, lambda replica_count: replica_count - 1),
            )),
        )

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
        return self._weighted_topic_broker_imbalance / self.total_weight

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
