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
from __future__ import annotations

import logging
from typing import Any
from typing import TYPE_CHECKING

from kafka_utils.kafka_cluster_manager.cluster_info.rg import ReplicationGroup
from kafka_utils.kafka_cluster_manager.cluster_info.topic import Topic

if TYPE_CHECKING:
    from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition


class Broker:
    """Represent a Kafka broker.
    A broker object contains as attributes the broker id, metadata
    (content of the broker node in zookeeper), partitions and replication group.
    """

    log = logging.getLogger(__name__)

    def __init__(self, id: int, metadata: dict[str, Any] | None = None, partitions: set[Partition] | None = None):
        self._id = id
        self._metadata = metadata
        self._partitions = partitions or set()
        self._decommissioned = False
        self._revoked_leadership = False
        self._inactive = False
        self._replication_group: ReplicationGroup | None = None

    @property
    def metadata(self) -> dict[str, Any] | None:
        return self._metadata

    def mark_decommissioned(self) -> None:
        """Mark a broker as decommissioned. Decommissioned brokers can still
        have partitions assigned.
        """
        self._decommissioned = True

    def mark_revoked_leadership(self) -> None:
        """Mark a broker as to be revoked from leadership."""
        self._revoked_leadership = True

    def mark_inactive(self) -> None:
        """Mark a broker as inactive. Inactive brokers may not have metadata."""
        self._inactive = True

    @property
    def inactive(self) -> bool:
        return self._inactive

    @property
    def replication_group(self) -> ReplicationGroup | None:
        return self._replication_group

    @replication_group.setter
    def replication_group(self, group: ReplicationGroup) -> None:
        self._replication_group = group

    @property
    def decommissioned(self) -> bool:
        return self._decommissioned

    @property
    def revoked_leadership(self) -> bool:
        return self._revoked_leadership

    @property
    def partitions(self) -> set[Partition]:
        return self._partitions

    @property
    def id(self) -> int:
        return self._id

    @property
    def topics(self) -> set[Topic]:
        """Return the set of topics current in broker."""
        return {partition.topic for partition in self._partitions}

    @property
    def weight(self) -> float:
        """Return the total weight of all partitions on this broker."""
        return sum(partition.weight for partition in self.partitions)

    @property
    def size(self) -> float:
        """Return the total size of all partitions on this broker."""
        return sum(partition.size for partition in self.partitions)

    @property
    def leader_weight(self) -> float:
        return sum(
            partition.weight
            for partition in self.partitions
            if partition.leader == self
        )

    def empty(self) -> bool:
        """Return true if the broker has no replicas assigned"""
        return len(self.partitions) == 0

    def remove_partition(self, partition: Partition) -> None:
        """Remove partition from partition list."""
        if partition in self._partitions:
            # Remove partition from set
            self._partitions.remove(partition)
            # Remove broker from replica list of partition
            partition.replicas.remove(self)
        else:
            raise ValueError(
                'Partition: {topic_id}:{partition_id} not found in broker '
                '{broker_id}'.format(
                    topic_id=partition.topic.id,
                    partition_id=partition.partition_id,
                    broker_id=self._id,
                )
            )

    def add_partition(self, partition: Partition) -> None:
        """Add partition to partition list."""
        assert partition not in self._partitions
        # Add partition to existing set
        self._partitions.add(partition)
        # Add broker to replica list
        partition.add_replica(self)

    def move_partition(self, partition: Partition, broker_destination: Broker) -> None:
        """Move partition to destination broker and adjust replicas."""
        self.remove_partition(partition)
        broker_destination.add_partition(partition)

    def count_partitions(self, topic: Topic) -> int:
        """Return count of partitions for given topic."""
        return sum(1 for p in topic.partitions if p in self.partitions)

    def count_preferred_replica(self) -> int:
        """Return number of times broker is set as preferred leader."""
        return sum(
            1 for partition in self.partitions if partition.leader == self
        )

    def get_preferred_partition(self, broker: Broker, sibling_distance: dict[Topic, int]) -> Partition | None:
        """The preferred partition belongs to the topic with the minimum
        (also negative) distance between destination and source.

        :param broker:  Destination broker
        :param sibling_distance: dict {topic: distance} negative distance should
            mean that destination broker has got less partition of a certain topic
            than source self.
        :returns: A partition or None if no eligible partitions are available
        """
        # Only partitions not having replica in broker are valid
        # Get best fit partition, based on avoiding partition from same topic
        # and partition with least siblings in destination-broker.
        eligible_partitions = self.partitions - broker.partitions
        if eligible_partitions:
            pref_partition = min(
                eligible_partitions,
                key=lambda source_partition:
                    sibling_distance[source_partition.topic],
            )
            return pref_partition
        else:
            return None

    def request_leadership(self, opt_count: int, skip_brokers: list[Broker], skip_partitions: list[Partition]) -> None:
        """Under-balanced broker requests leadership from current leader, on the
        pretext that it recursively can maintain its leadership count as optimal.

        :key_terms:
        leader-balanced: Count of brokers as leader is at least opt-count

        Algorithm:
        =========
        Step-1: Broker will request leadership from current-leader of partitions
                it belongs to.
        Step-2: Current-leaders will grant their leadership if one of these happens:-
            a) Either they remain leader-balanced.
            b) Or they will recursively request leadership from other partitions
               until they are become leader-balanced.
            If both of these conditions fail, they will revoke their leadership-grant
        Step-3: If current-broker becomes leader-balanced it will return
                otherwise it moves ahead with next partition.
        """
        # Possible partitions which can grant leadership to broker
        owned_partitions = list(filter(
            lambda p: self is not p.leader and len(p.replicas) > 1,
            self.partitions,
        ))
        for partition in owned_partitions:
            # Partition not available to grant leadership when:
            # 1. Broker is already under leadership change or
            # 2. Partition has already granted leadership before
            if partition.leader in skip_brokers or partition in skip_partitions:
                continue
            # Current broker is granted leadership temporarily
            prev_leader = partition.swap_leader(self)
            # Partition shouldn't be used again
            skip_partitions.append(partition)
            # Continue if prev-leader remains balanced
            # If leadership of prev_leader is to be revoked, it is considered balanced
            if prev_leader.count_preferred_replica() >= opt_count or \
                    prev_leader.revoked_leadership:
                # If current broker is leader-balanced return else
                # request next-partition
                if self.count_preferred_replica() >= opt_count:
                    return
                else:
                    continue
            else:  # prev-leader (broker) became unbalanced
                # Append skip-brokers list so that it is not unbalanced further
                skip_brokers.append(prev_leader)
                # Try recursively arrange leadership for prev-leader
                prev_leader.request_leadership(opt_count, skip_brokers, skip_partitions)
                # If prev-leader couldn't be leader-balanced
                # revert its previous grant to current-broker
                if prev_leader.count_preferred_replica() < opt_count:
                    # Partition can be used again for rebalancing
                    skip_partitions.remove(partition)
                    partition.swap_leader(prev_leader)
                    # Try requesting leadership from next partition
                    continue
                else:
                    # If prev-leader successfully balanced
                    skip_partitions.append(partition)
                    # Removing from skip-broker list, since it can now again be
                    # used for granting leadership for some other partition
                    skip_brokers.remove(prev_leader)
                    if self.count_preferred_replica() >= opt_count:
                        # Return if current-broker is leader-balanced
                        return
                    else:
                        continue

    def donate_leadership(self, opt_count: int, skip_brokers: list[Broker], used_edges: list[tuple[Partition, Broker, Broker]]) -> None:
        """Over-loaded brokers tries to donate their leadership to one of their
        followers recursively until they become balanced.

        :key_terms:
        used_edges: Represent list of tuple/edges (partition, prev-leader, new-leader),
                    which have already been used for donating leadership from
                    prev-leader to new-leader in same partition before.
        skip_brokers: This is to avoid using same broker recursively for balancing
                      to prevent loops.

        :Algorithm:
        * Over-loaded leader tries to donate its leadership to one of its followers
        * Follower will try to be balanced recursively if it becomes over-balanced
        * If it is successful, over-loaded leader moves to next partition if required,
            return otherwise.
        * If it is unsuccessful, it tries for next-follower or next-partition whatever
            or returns if none available.
        """
        owned_partitions = list(filter(
            lambda p: self is p.leader and len(p.replicas) > 1,
            self.partitions,
        ))
        for partition in owned_partitions:
            # Skip using same partition with broker if already used before
            potential_new_leaders = list(filter(
                lambda f: f not in skip_brokers,
                partition.followers,
            ))
            for follower in potential_new_leaders:
                # Don't swap the broker-pair if already swapped before
                # in same partition
                if (partition, self, follower) in used_edges:
                    continue
                partition.swap_leader(follower)
                used_edges.append((partition, follower, self))
                # new-leader didn't unbalance
                if follower.count_preferred_replica() <= opt_count + 1:
                    # over-broker balanced
                    # If over-broker is the one which needs to be revoked from leadership
                    # it's considered balanced only if its preferred replica count is 0
                    if (self.count_preferred_replica() <= opt_count + 1 and not self.revoked_leadership) or \
                            (self.count_preferred_replica() == 0 and self.revoked_leadership):
                        return
                    else:
                        # Try next-partition, not another follower
                        break
                else:  # new-leader (broker) became over-balanced
                    skip_brokers.append(follower)
                    follower.donate_leadership(opt_count, skip_brokers, used_edges)
                    # new-leader couldn't be balanced, revert
                    if follower.count_preferred_replica() > opt_count + 1:
                        used_edges.append((partition, follower, self))
                        partition.swap_leader(self)
                        # Try next leader or partition
                        continue
                    else:
                        # New-leader was successfully balanced
                        used_edges.append((partition, follower, self))
                        # New-leader can be reused
                        skip_brokers.remove(follower)
                        # If broker is the one which needs to be revoked from leadership
                        # it's considered balanced only if its preferred replica count is 0
                        if (self.count_preferred_replica() <= opt_count + 1 and not self.revoked_leadership) or \
                                (self.count_preferred_replica() == 0 and self.revoked_leadership):
                            # Now broker is balanced
                            return
                        else:
                            # Try next-partition, not another follower
                            break

    def __str__(self) -> str:
        return f"{self._id}"

    def __repr__(self) -> str:
        return f"{self}"
