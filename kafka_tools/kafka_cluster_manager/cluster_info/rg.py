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
import sys
from collections import defaultdict

from .error import EmptyReplicationGroupError
from .error import NotEligibleGroupError
from .util import separate_groups


class ReplicationGroup(object):
    """Represent attributes and functions specific to replication-groups
    abbreviated as rg.
    """

    log = logging.getLogger(__name__)

    def __init__(self, id, brokers=None):
        self._id = id
        if brokers and not isinstance(brokers, set):
            raise TypeError(
                "brokers has to be a set but type is {0}".format(type(brokers)),
            )
        self._brokers = brokers or set()
        self._sibling_distance = None

    @property
    def id(self):
        """Return name of replication-groups."""
        return self._id

    @property
    def brokers(self):
        """Return set of brokers."""
        return self._brokers

    def add_broker(self, broker):
        """Add broker to current broker-list."""
        if broker not in self._brokers:
            self._brokers.add(broker)
        else:
            self.log.warning(
                'Broker {broker_id} already present in '
                'replication-group {rg_id}'.format(
                    broker_id=broker.id,
                    rg_id=self.id,
                )
            )

    @property
    def partitions(self):
        """Evaluate and return set of all partitions in replication-group.
        rtype: list, replicas of partitions can reside in this group
        """
        return [
            partition
            for broker in self._brokers
            for partition in broker.partitions
        ]

    def count_replica(self, partition):
        """Return count of replicas of given partition."""
        return self.partitions.count(partition)

    def acquire_partition(self, partition, source_broker):
        """Move a partition from a broker to any of the eligible brokers
        of the replication group.

        :param partition: Partition to move
        :param source_broker: Broker the partition currently belongs to
        """
        broker_dest = self._elect_dest_broker(partition)
        if not broker_dest:
            raise NotEligibleGroupError(
                "No eligible brokers to accept partition {p}".format(p=partition),
            )
        source_broker.move_partition(partition, broker_dest)

    def move_partition(self, rg_destination, victim_partition):
        """Move partition(victim) from current replication-group to destination
        replication-group.

        Step 1: Evaluate source and destination broker
        Step 2: Move partition from source-broker to destination-broker
        """
        # Select best-fit source and destination brokers for partition
        # Best-fit is based on partition-count and presence/absence of
        # Same topic-partition over brokers
        broker_source, broker_destination = self._select_broker_pair(
            rg_destination,
            victim_partition,
        )
        # Actual-movement of victim-partition
        self.log.debug(
            'Moving partition {p_name} from broker {broker_source} to '
            'replication-group:broker {rg_dest}:{dest_broker}'.format(
                p_name=victim_partition.name,
                broker_source=broker_source.id,
                dest_broker=broker_destination.id,
                rg_dest=rg_destination.id,
            ),
        )
        broker_source.move_partition(victim_partition, broker_destination)

    def _select_broker_pair(self, rg_destination, victim_partition):
        """Select best-fit source and destination brokers based on partition
        count and presence of partition over the broker.

        * Get overloaded and underloaded brokers
        Best-fit Selection Criteria:
        Source broker: Select broker containing the victim-partition with
        maximum partitions.
        Destination broker: NOT containing the victim-partition with minimum
        partitions. If no such broker found, return first broker.

        This helps in ensuring:-
        * Topic-partitions are distributed across brokers.
        * Partition-count is balanced across replication-groups.
        """
        broker_source = self._elect_source_broker(victim_partition)
        broker_destination = rg_destination._elect_dest_broker(victim_partition)
        return broker_source, broker_destination

    def _elect_source_broker(self, victim_partition):
        """Select first over loaded broker having victim_partition.

        Note: The broker with maximum siblings of victim-partitions (same topic)
        is selected to reduce topic-partition imbalance.
        """
        over_loaded_brokers = sorted(
            [
                broker
                for broker in self._brokers
                if victim_partition in broker.partitions and not broker.inactive
            ],
            key=lambda b: len(b.partitions),
            reverse=True,
        )
        if not over_loaded_brokers:
            return None

        broker_topic_partition_cnt = [
            (broker, broker.count_partitions(victim_partition.topic))
            for broker in over_loaded_brokers
        ]
        max_count_pair = max(
            broker_topic_partition_cnt,
            key=lambda ele: ele[1],
        )
        return max_count_pair[0]

    def _elect_dest_broker(self, victim_partition):
        """Select first under loaded brokers preferring not having
        partition of same topic as victim partition.
        """
        under_loaded_brokers = sorted(
            [
                broker
                for broker in self._brokers
                if (victim_partition not in broker.partitions and
                    not broker.inactive and
                    not broker.decommissioned)
            ],
            key=lambda b: len(b.partitions)
        )
        if not under_loaded_brokers:
            return None

        broker_topic_partition_cnt = [
            (broker, broker.count_partitions(victim_partition.topic))
            for broker in under_loaded_brokers
            if victim_partition not in broker.partitions
        ]
        min_count_pair = min(
            broker_topic_partition_cnt,
            key=lambda ele: ele[1],
        )
        return min_count_pair[0]

    def get_active_brokers(self):
        return set(b for b in self.brokers if not b.inactive)

    # Re-balancing brokers
    def rebalance_brokers(self):
        """Rebalance partition-count across brokers."""
        total_partitions = sum(len(b.partitions) for b in self.brokers)
        blacklist = set(b for b in self.brokers if b.decommissioned)
        active_brokers = self.get_active_brokers() - blacklist
        if not active_brokers:
            raise EmptyReplicationGroupError("No active brokers in %s", self.id)
        # Separate brokers based on partition count
        over_loaded_brokers, under_loaded_brokers = separate_groups(
            active_brokers,
            lambda b: len(b.partitions),
            total_partitions,
        )
        # Decommissioned brokers are considered overloaded until they have
        # no more partitions assigned.
        over_loaded_brokers += [b for b in blacklist if not b.empty()]
        if not over_loaded_brokers and not under_loaded_brokers:
            self.log.info(
                'Brokers of replication-group: %s already balanced for '
                'partition-count.',
                self._id,
            )
            return

        sibling_distance = self.generate_sibling_distance()
        while under_loaded_brokers and over_loaded_brokers:
            # Get best-fit source-broker, destination-broker and partition
            broker_source, broker_destination, victim_partition = \
                self._get_target_brokers(
                    over_loaded_brokers,
                    under_loaded_brokers,
                    sibling_distance,
                )
            # No valid source or target brokers found
            if broker_source and broker_destination:
                # Move partition
                self.log.debug(
                    'Moving partition {p_name} from broker {broker_source} to '
                    'broker {broker_destination}'
                    .format(
                        p_name=victim_partition.name,
                        broker_source=broker_source.id,
                        broker_destination=broker_destination.id,
                    ),
                )
                broker_source.move_partition(victim_partition, broker_destination)
                sibling_distance = self.update_sibling_distance(
                    sibling_distance,
                    broker_destination,
                    victim_partition,
                )
            else:
                # Brokers are balanced or could not be balanced further
                break
            # Re-evaluate under and over-loaded brokers
            over_loaded_brokers, under_loaded_brokers = separate_groups(
                active_brokers,
                lambda b: len(b.partitions),
                total_partitions,
            )
            # As before add brokers to decommission.
            over_loaded_brokers += [b for b in blacklist if not b.empty()]

    def _get_target_brokers(self, over_loaded_brokers, under_loaded_brokers, sibling_distance):
        """Pick best-suitable source-broker, destination-broker and partition to
        balance partition-count over brokers in given replication-group.
        """
        # Sort given brokers to ensure determinism
        over_loaded_brokers = sorted(
            over_loaded_brokers,
            key=lambda b: len(b.partitions),
            reverse=True,
        )
        under_loaded_brokers = sorted(
            under_loaded_brokers,
            key=lambda b: len(b.partitions),
        )
        # pick pair of brokers from source and destination brokers with
        # minimum same-partition-count
        # Set result in format: (source, dest, preferred-partition)
        target = (None, None, None)
        min_distance = sys.maxint
        best_partition = None
        for source in over_loaded_brokers:
            for dest in under_loaded_brokers:
                # A decommissioned broker can have less partitions than
                # destination. We consider it a valid source because we want to
                # move all the partitions out from it.
                if (len(source.partitions) - len(dest.partitions) > 1 or
                        source.decommissioned):
                    best_partition = source.get_preferred_partition(
                        dest,
                        sibling_distance[dest][source],
                    )
                    # If no eligible partition continue with next broker.
                    if best_partition is None:
                        continue
                    distance = sibling_distance[dest][source][best_partition.topic]
                    if distance < min_distance:
                        min_distance = distance
                        target = (source, dest, best_partition)
                else:
                    # If relatively-unbalanced then all brokers in destination
                    # will be thereafter, return from here.
                    break
        return target

    def generate_sibling_distance(self):
        """Generate a dict containing the distance computed as difference in
        in number of partitions of each topic from under_loaded_brokers
        to over_loaded_brokers.

        Negative distance means that the destination broker has got less
        partitions of a certain topic than the source broker.

        returns: dict {dest: {source: {topic: distance}}}
        """
        sibling_distance = defaultdict(lambda: defaultdict(dict))
        for source in self.brokers:
            for dest in self.brokers:
                if source != dest:
                    for topic in source.topics:
                        sibling_distance[dest][source][topic] = \
                            dest.count_partitions(topic) - \
                            source.count_partitions(topic)
        return sibling_distance

    def update_sibling_distance(self, sibling_distance, dest, topic):
        """Update the sibling distance for topic and destination broker."""
        for source in sibling_distance[dest].iterkeys():
            sibling_distance[dest][source][topic] = \
                dest.count_partitions(topic) - \
                source.count_partitions(topic)
        return sibling_distance

    def move_partition_replica(self, under_loaded_rg, eligible_partition):
        """Move partition to under-loaded replication-group if possible."""
        # Evaluate possible source and destination-broker
        source_broker, dest_broker = self._get_eligible_broker_pair(
            under_loaded_rg,
            eligible_partition,
        )
        if source_broker and dest_broker:
            self.log.debug(
                'Moving partition {p_name} from broker {source_broker} to '
                'replication-group:broker {rg_dest}:{dest_broker}'.format(
                    p_name=eligible_partition.name,
                    source_broker=source_broker.id,
                    dest_broker=dest_broker.id,
                    rg_dest=under_loaded_rg.id,
                ),
            )
            # Move partition if eligible brokers found
            source_broker.move_partition(eligible_partition, dest_broker)

    def _get_eligible_broker_pair(self, under_loaded_rg, eligible_partition):
        """Evaluate and return source and destination broker-pair from over-loaded
        and under-loaded replication-group if possible, return None otherwise.

        Return source broker with maximum partitions and destination broker with
        minimum partitions based on following conditions:-
        1) At-least one broker in under-loaded group which does not have
        victim-partition. This is because a broker cannot have duplicate replica.
        2) At-least one broker in over-loaded group which has victim-partition
        """
        under_brokers = filter(
            lambda b: eligible_partition not in b.partitions,
            under_loaded_rg.brokers,
        )
        over_brokers = filter(
            lambda b: eligible_partition in b.partitions,
            self.brokers,
        )

        # Get source and destination broker
        source_broker = max(
            over_brokers,
            key=lambda broker: len(broker.partitions),
        )
        dest_broker = min(
            under_brokers,
            key=lambda broker: len(broker.partitions),
        )
        return (source_broker, dest_broker)

    def __str__(self):
        return "{0}".format(self._id)

    def __repr__(self):
        return "{0}".format(self)
