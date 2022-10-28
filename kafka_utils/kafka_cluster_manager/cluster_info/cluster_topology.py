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
from collections import OrderedDict
from typing import Any
from typing import Callable

from .broker import Broker
from .error import InvalidBrokerIdError
from .error import InvalidPartitionError
from .partition import Partition
from .partition_measurer import PartitionMeasurer
from .rg import ReplicationGroup
from .topic import Topic


class ClusterTopology:
    """Represent a Kafka cluster and functionalities supported over the cluster.

        :param assignment: cluster assignment is a dict (topic, partition): replicas
        :param brokers: dict representing the active brokers of the
            cluster broker_id: metadata (metadata is the content of the zookeeper
            node of the broker)
        :param partition_measurer: Instance of PartitionMeasurer to use when
            assigning partitions a weight and size.
        :param extract_group: function used to extract the replication group
            from each broker. The extract_group function is called for each
            broker passing the Broker object as argument. It should return a
            string representing the ReplicationGroup id.
    """

    def __init__(
        self,
        assignment: dict[tuple[str, int], list[int]],
        brokers: dict[int, dict[str, Any] | None],
        partition_measurer: PartitionMeasurer,
        extract_group: Callable[[Broker], str],
    ) -> None:
        self.extract_group = extract_group
        self.partition_measurer = partition_measurer
        self.log = logging.getLogger(self.__class__.__name__)
        self.topics: dict[str, Topic] = {}
        self.rgs: dict[str, ReplicationGroup] = {}
        self.brokers: dict[int, Broker] = {}
        self.partitions: dict[tuple[str, int], Partition] = {}
        self._build_brokers(brokers)
        self._build_partitions(assignment)
        self.log.debug(
            'Total partitions in cluster {partitions}'.format(
                partitions=len(self.partitions),
            ),
        )
        self.log.debug(
            'Total replication-groups in cluster {rgs}'.format(
                rgs=len(self.rgs),
            ),
        )
        self.log.debug(
            'Total brokers in cluster {brokers}'.format(
                brokers=len(self.brokers),
            ),
        )

    def _build_brokers(self, brokers: dict[int, dict[str, Any] | None]) -> None:
        """Build broker objects using broker-ids."""
        for broker_id, metadata in brokers.items():
            self.brokers[broker_id] = self._create_broker(broker_id, metadata)

    def _create_broker(self, broker_id: int, metadata: dict[str, Any] | None = None) -> Broker:
        """Create a broker object and assign to a replication group.
        A broker object with no metadata is considered inactive.
        An inactive broker may or may not belong to a group.
        """
        broker = Broker(broker_id, metadata)
        if not metadata:
            broker.mark_inactive()
        rg_id = self.extract_group(broker)
        group = self.rgs.setdefault(rg_id, ReplicationGroup(rg_id))
        group.add_broker(broker)
        broker.replication_group = group
        return broker

    def _build_partitions(self, assignment: dict[tuple[str, int], list[int]]) -> None:
        """Builds all partition objects and update corresponding broker and
        topic objects.
        """
        self.partitions = {}
        for partition_name, replica_ids in assignment.items():
            # Get topic
            topic_id = partition_name[0]
            partition_id = partition_name[1]
            topic = self.topics.setdefault(
                topic_id,
                Topic(topic_id, replication_factor=len(replica_ids))
            )

            # Creating partition object
            partition = Partition(
                topic,
                partition_id,
                weight=self.partition_measurer.get_weight(partition_name),
                size=self.partition_measurer.get_size(partition_name),
            )
            self.partitions[partition_name] = partition
            topic.add_partition(partition)

            # Updating corresponding broker objects
            for broker_id in replica_ids:
                # Check if broker-id is present in current active brokers
                if broker_id not in list(self.brokers.keys()):
                    self.log.warning(
                        "Broker %s containing partition %s is not in "
                        "active brokers.",
                        broker_id,
                        partition,
                    )
                    self.brokers[broker_id] = self._create_broker(broker_id)

                self.brokers[broker_id].add_partition(partition)

    @property
    def active_brokers(self) -> set[Broker]:
        """Set of brokers that are not inactive or decommissioned."""
        return {
            broker for broker in self.brokers.values()
            if not broker.inactive and not broker.decommissioned
        }

    @property
    def assignment(self) -> dict[tuple[str, int], list[int]]:
        assignment = {}
        for partition in self.partitions.values():
            assignment[
                (partition.topic.id, partition.partition_id)
            ] = [broker.id for broker in partition.replicas]
        # assignment map created in sorted order for deterministic solution
        return OrderedDict(sorted(list(assignment.items()), key=lambda t: t[0]))

    def replace_broker(self, source_id: int, dest_id: int) -> None:
        """Move all partitions in source broker to destination broker.

        :param source_id: source broker-id
        :param dest_id: destination broker-id, or None (to remove source broker)
        :raises: InvalidBrokerIdError, when either of given broker-ids is invalid.
        """
        try:
            source = self.brokers[source_id]
            dest = None
            if dest_id is not None:
                dest = self.brokers[dest_id]
            # Move all partitions from source to destination broker
            for partition in source.partitions.copy():  # Partitions set changes
                # We cannot move partition directly since that re-orders the
                # replicas for the partition
                source.partitions.remove(partition)
                if dest:
                    dest.partitions.add(partition)
                # Replace broker in replica
                partition.replace(source, dest)

        except KeyError as e:
            self.log.error("Invalid broker id %s.", e.args[0])
            raise InvalidBrokerIdError(
                f"Broker id {e.args[0]} does not exist in cluster"
            )

    def update_cluster_topology(self, assignment: dict[tuple[str, int], list[int]]) -> None:
        """Modify the cluster-topology with given assignment.

        Change the replica set of partitions as in given assignment.

        :param assignment: dict representing actions to be used to update the current
        cluster-topology
        :raises: InvalidBrokerIdError when broker-id is invalid
        :raises: InvalidPartitionError when partition-name is invalid
        """
        try:
            for partition_name, replica_ids in assignment.items():
                try:
                    new_replicas = [self.brokers[b_id] for b_id in replica_ids]
                except KeyError:
                    self.log.error(
                        "Invalid replicas %s for topic-partition %s-%s.",
                        ', '.join([str(id) for id in replica_ids]),
                        partition_name[0],
                        partition_name[1],
                    )
                    raise InvalidBrokerIdError(
                        "Invalid replicas {}.".format(
                            ', '.join([str(id) for id in replica_ids])
                        ),
                    )
                try:
                    partition = self.partitions[partition_name]
                    old_replicas = [broker for broker in partition.replicas]

                    # No change needed. Save ourself some CPU time.
                    # Replica order matters as the first one is the leader.
                    if new_replicas == old_replicas:
                        continue

                    # Remove old partitions from broker
                    # This also updates partition replicas
                    for broker in old_replicas:
                        broker.remove_partition(partition)

                    # Add new partition to brokers
                    for broker in new_replicas:
                        broker.add_partition(partition)
                except KeyError:
                    self.log.error(
                        "Invalid topic-partition %s-%s.",
                        partition_name[0],
                        partition_name[1],
                    )
                    raise InvalidPartitionError(
                        "Invalid topic-partition {}-{}."
                        .format(partition_name[0], partition_name[1]),
                    )
        except KeyError:
            self.log.error(f"Could not parse given assignment {assignment}")
            raise
