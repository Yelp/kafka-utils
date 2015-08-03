"""Contains information for partition layout on given cluster.

Also contains api's dealing with dealing with changes in partition layout.
The steps (1-6) and states S0 -- S2 and algorithm for re-assigning-partitions
is per the design document at:-

https://docs.google.com/document/d/1qloANcOHkzuu8wYVm0ZAMCGY5Mmb-tdcxUywNIXfQFI
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys
from collections import defaultdict
from collections import OrderedDict
from math import sqrt

from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface

from .broker import Broker
from .partition import Partition
from .rg import ReplicationGroup
from .topic import Topic


class ClusterTopology(object):
    """Represent a Kafka cluster and functionalities supported over the cluster.

    A Kafka cluster topology consists of:
    replication group (alias rg), broker, topic and partition.
    """
    def __init__(self, zk):
        self._name = zk.cluster_config.name
        self._zk = zk
        # Getting Initial assignment
        broker_ids = [
            int(broker) for broker in self._zk.get_brokers().iterkeys()
        ]
        topic_ids = sorted(self._zk.get_topics(names_only=True))
        self.fetch_initial_assignment(broker_ids, topic_ids)
        # Sequence of building objects
        self._build_topics(topic_ids)
        self._build_brokers(broker_ids)
        self._build_replication_groups()
        self._build_partitions()

    def _build_topics(self, topic_ids):
        """List of topic objects from topic-ids."""
        # Fetch topic list from zookeeper
        self.topics = {}
        for topic_id in topic_ids:
            self.topics[topic_id] = Topic(topic_id)

    def _build_brokers(self, broker_ids):
        """Build broker objects using broker-ids."""
        self.brokers = {}
        for broker_id in broker_ids:
            self.brokers[broker_id] = Broker(broker_id)

    def _build_replication_groups(self):
        """Build replication-group objects using the given assignment."""
        self.rgs = {}
        for broker in self.brokers.itervalues():
            rg_id = self._get_replication_group_id(broker)
            if rg_id not in self.rgs:
                self.rgs[rg_id] = ReplicationGroup(rg_id)
            self.rgs[rg_id].add_broker(broker)

    def _build_partitions(self):
        """Builds all partition objects and update corresponding broker and
        topic objects.
        """
        self.partitions = {}
        for partition_name, replica_ids in self._initial_assignment.iteritems():
            # Creating replica objects
            replicas = [self.brokers[broker_id] for broker_id in replica_ids]

            # Get topic
            topic_id = partition_name[0]
            topic = self.topics[topic_id]

            # Creating partition object
            partition = Partition(partition_name, topic, replicas)
            self.partitions[partition_name] = partition

            # Updating corresponding topic object
            topic.add_partition(partition)

            # Updating corresponding broker objects
            for broker_id in replica_ids:
                broker = self.brokers[broker_id]
                broker.add_partition(partition)

    def fetch_initial_assignment(self, broker_ids, topic_ids):
        """Fetch initial assignment from zookeeper.

        Assignment is ordered by partition name tuple.
        """
        # Requires running kafka-scripts
        kafka = KafkaInterface()
        self._initial_assignment = kafka.get_cluster_assignment(
            self._zk.cluster_config.zookeeper,
            broker_ids,
            topic_ids
        )

    def _get_replication_group_id(self, broker):
        """Fetch replication-group to broker map from zookeeper."""
        try:
            habitat = broker.hostname.rsplit('-', 1)[1]
            rg_name = habitat.split('.', 1)[0]
        except IndexError:
            if 'localhost' in broker.hostname:
                print(
                    '[WARNING] Setting replication-group as localhost for '
                    'broker {broker}'.format(broker=broker.id)
                )
                rg_name = 'localhost'
            else:
                print(
                    '[ERROR] Could not parse replication group for {broker} '
                    'with hostname:{host}'.format(
                        broker=broker.id,
                        host=broker.hostname
                    )
                )
                sys.exit(1)
        return rg_name

    def reassign_partitions(
        self,
        rebalance_option,
        max_changes,
        to_execute,
    ):
        """Display or execute the final-state based on rebalancing option."""
        self.rebalance_replication_groups()

    # Balancing api's
    # Balancing replication-groups: S0 --> S1
    def rebalance_replication_groups(self):
        """Rebalance partitions over placement groups (availability-zones)."""
        self.rebalance_partition_replicas_over_replication_groups()

    def rebalance_partition_replicas_over_replication_groups(self):
        """Rebalance given segregated replication-groups."""
        for partition in self.partitions.values():
            # Fetch potentially under-replicated and over-replicated
            # replication-groups for each partition
            under_replicated_rgs, over_replicated_rgs = \
                self.segregate_replication_groups(partition)
            replication_factor = len(partition.replicas)
            rg_count = len(self.rgs)
            opt_replica_count = replication_factor // rg_count

            # Move partition-replicas from over-replicated to under-replicated
            # replication-groups
            for rg_source in over_replicated_rgs:
                # Keep reducing partition-replicas over source over-replicated
                # replication-group until either it is evenly-replicated
                # or no under-replicated replication-group is found
                while rg_source.replica_count(partition) > opt_replica_count:
                    # Move partitions in under-replicated replication-groups
                    # until the group is empty
                    rg_destination = None
                    # Locate under-replicated replication-group with lesser
                    # replica count than source replication-group
                    for rg_under in under_replicated_rgs:
                        if rg_under.replica_count(partition) < \
                                rg_source.replica_count(partition) - 1:
                            rg_destination = rg_under
                            break
                    if rg_destination:
                        total_brokers_cluster = len(self.brokers)
                        total_partitions_cluster = len(self.get_all_partitions())
                        # Actual movement of partition
                        rg_source.move_partition(
                            partition,
                            rg_destination,
                            total_brokers_cluster,
                            total_partitions_cluster,
                        )
                        if rg_destination.replica_count(partition) == opt_replica_count:
                            under_replicated_rgs.remove(rg_destination)
                    else:
                        # Destination under-replicated replication-group not found
                        # Partition is evenly-replicated for source replication-group
                        # Or under-replicated replication-groups is empty
                        break
                if rg_source.replica_count(partition) > opt_replica_count + 1:
                    print(
                        '[WARNING] Could not re-balance over-replicated'
                        'replication-group {rg_id} for partition '
                        '{topic}:{p_id}'.format(
                            rg_id=rg_source.id,
                            partition=partition.topic.id,
                            p_id=partition.partition_id,
                        )
                    )
                over_replicated_rgs.remove(rg_source)

            # List remaining under-replicated replication-groups left, if any.
            if not under_replicated_rgs:
                for rg in under_replicated_rgs:
                    if rg.replica_count(partition) < opt_replica_count:
                        print(
                            '[WARNING] Could not re-balance under-replicated'
                            'replication-group {rg_id} for partition '
                            '{topic}:{p_id}'.format(
                                rg_id=rg.id,
                                partition=partition.topic.id,
                                p_id=partition.partition_id,
                            )
                        )

    def segregate_replication_groups(self, partition):
        """Separate replication-groups into under-replicated, over-replicated
        and optimally replicated groups.
        """
        under_replicated_rgs = []
        over_replicated_rgs = []
        replication_factor = len(partition.replicas)
        rg_count = len(self.rgs)
        opt_replica_count = replication_factor // len(self.rgs)
        for rg in self.rgs.values():
            replica_count = rg.replica_count(partition)
            if replica_count < opt_replica_count:
                under_replicated_rgs.append(rg)
            elif replica_count > opt_replica_count:
                over_replicated_rgs.append(rg)
            else:
                # replica_count == opt_replica_count
                if replication_factor % rg_count == 0:
                    # Case 2: Rp % G == 0: Replication-groups should have same replica-count
                    # Nothing to be done since it's replication-group is already balanced
                    pass
                else:
                    # Case 1 or 3: Rp % G !=0: Rp < G or Rp > G
                    # Helps in adjusting one extra replica if required
                    under_replicated_rgs.append(rg)
        return under_replicated_rgs, over_replicated_rgs

    # End Balancing replication-groups.

    def get_all_partitions(self):
        """Return list of partitions across all brokers.

        Contains partition-replicas as well.
        """
        partitions = []
        for rg in self.rgs.itervalues():
            partitions += rg.partitions
        return partitions

    def get_assignment_json(self):
        """Build and return cluster-topology in json format."""
        # TODO: Fix, version is hard-coded and rg missing
        assignment_json = {
            'version': 1,
            'partitions':
            [
                {
                    'topic': partition.topic.id,
                    'partition': partition.partition_id,
                    'replicas': [broker.id for broker in partition.replicas]
                }
                for partition in self.partitions.itervalues()
            ]
        }
        return assignment_json

    def get_initial_assignment_json(self):
        return {
            'version': 1,
            'partitions':
            [
                {
                    'topic': t_p_key[0],
                    'partition': t_p_key[1],
                    'replicas': replica
                } for t_p_key, replica in self._initial_assignment.iteritems()
            ]
        }

    @property
    def initial_assignment(self):
        return self._initial_assignment

    @property
    def assignment(self):
        kafka = KafkaInterface()
        return kafka.get_assignment_map(self.get_assignment_json())[0]

    def display_initial_cluster_topology(self):
        """Display the current cluster topology."""
        print(self.get_initial_assignment_json())

    def display_current_cluster_topology(self):
        print(self.get_assignment_json())
