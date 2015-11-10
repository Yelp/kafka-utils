"""Contains information for partition layout on given cluster.

Also contains api's dealing with changes in partition layout.
The steps (1-6) and states S0 -- S2 and algorithm for re-assigning-partitions
is per the design document at:-

https://docs.google.com/document/d/1qloANcOHkzuu8wYVm0ZAMCGY5Mmb-tdcxUywNIXfQFI
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import logging

from collections import OrderedDict
from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface
from .broker import Broker
from .partition import Partition
from .rg import ReplicationGroup
from .topic import Topic
from .util import (
    compute_group_optimum,
    separate_groups,
    smart_separate_groups,
)


class ClusterTopology(object):
    """Represent a Kafka cluster and functionalities supported over the cluster.

    A Kafka cluster topology consists of:
    replication group (alias rg), broker, topic and partition.
    """
    def __init__(self, zk, script_path=None):
        self._name = zk.cluster_config.name
        self._zk = zk
        self.log = logging.getLogger(self.__class__.__name__)
        self._kafka_script_path = script_path
        # Getting Initial assignment
        broker_ids = [
            int(broker) for broker in self._zk.get_brokers().iterkeys()
        ]
        topic_ids = sorted(self._zk.get_topics(names_only=True))
        self._fetch_initial_assignment(broker_ids, topic_ids)
        # Sequence of building objects
        self._build_topics(topic_ids)
        self._build_brokers(broker_ids)
        self._build_replication_groups()
        self._build_partitions()
        self.log.debug('Cluster-topology object created.')

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
            # Get topic
            topic_id = partition_name[0]
            partition_id = partition_name[1]
            topic = self.topics[topic_id]

            # Creating partition object
            partition = Partition(topic, partition_id)
            self.partitions[partition_name] = partition

            # Updating corresponding broker objects
            for broker_id in replica_ids:
                # Check if broker-id is present in current active brokers
                if broker_id in self.brokers.keys():
                    broker = self.brokers[broker_id]
                    broker.add_partition(partition)
                else:
                    error_msg = 'Broker {b_id} in replicas {replicas} for partition '\
                        '{partition} not present in active brokers {active_b}.'\
                        .format(
                            b_id=broker_id,
                            replicas=replica_ids,
                            partition=partition_name,
                            active_b=self.brokers.keys(),
                        ),
                    self.log.exception(error_msg)
                    raise ValueError(error_msg)
            # Updating corresponding topic object. Should be done in end since
            # replication-factor is updated once partition is updated.
            topic.add_partition(partition)

    def _fetch_initial_assignment(self, broker_ids, topic_ids):
        """Fetch initial assignment from zookeeper.

        Assignment is ordered by partition name tuple.
        """
        kafka = KafkaInterface(self._kafka_script_path)
        self._initial_assignment = kafka.get_cluster_assignment(
            self._zk,
            broker_ids,
            topic_ids,
        )

    def _get_replication_group_id(self, broker):
        """Fetch replication-group to broker map from zookeeper."""
        try:
            hostname = broker.get_hostname(self._zk)
            if 'localhost' in hostname:
                self.log.warning(
                    "Setting replication-group as localhost for broker %s",
                    broker.id,
                )
                rg_name = 'localhost'
            else:
                habitat = hostname.rsplit('-', 1)[1]
                rg_name = habitat.split('.', 1)[0]
        except IndexError:
            error_msg = "Could not parse replication group for broker {id} with"\
                " hostname:{hostname}".format(id=broker.id, hostname=hostname)
            self.log.exception(error_msg)
            raise ValueError(error_msg)
        return rg_name

    @property
    def initial_assignment(self):
        return self._initial_assignment

    @property
    def assignment(self):
        assignment = {}
        for partition in self.partitions.itervalues():
            #  assignment_json['partitions']:
            assignment[
                (partition.topic.id, partition.partition_id)
            ] = [broker.id for broker in partition.replicas]
        # assignment map created in sorted order for deterministic solution
        return OrderedDict(sorted(assignment.items(), key=lambda t: t[0]))

    @property
    def partition_replicas(self):
        """Return list of all partitions in the cluster.
        Note: List contains all replicas as well.
        """
        return [
            partition
            for rg in self.rgs.values()
            for partition in rg.partitions
        ]

    # Balancing replication-groups: S0 --> S1
    def rebalance_replication_groups(self):
        """Rebalance partitions over replication groups (availability-zones).

        First step involves rebalancing replica-count for each partition across
        replication-groups.
        Second step involves rebalancing partition-count across replication-groups
        of the cluster.
        """
        # Balance replicas over replication-groups for each partition
        for partition in self.partitions.itervalues():
            self._rebalance_partition(partition)

        # Balance partition-count over replication-groups
        self.rebalance_groups_partition_cnt()

    def _rebalance_partition(self, partition):
        """Rebalance replication group for given partition."""
        # Separate replication-groups into under and over replicated
        over_replicated_rgs, under_replicated_rgs = separate_groups(
            self.rgs.values(),
            lambda g: g.count_replica(partition),
        )
        # Move replicas from over-replicated to under-replicated groups
        while under_replicated_rgs and over_replicated_rgs:
            # Decide source and destination group
            rg_source = self._elect_source_replication_group(
                over_replicated_rgs,
                partition,
            )
            rg_destination = self._elect_dest_replication_group(
                rg_source.count_replica(partition),
                under_replicated_rgs,
                partition,
            )
            if rg_source and rg_destination:
                # Actual movement of partition
                self.log.debug(
                    'Moving partition {p_name} from replication-group '
                    '{rg_source} to replication-group {rg_dest}'.format(
                        p_name=partition.name,
                        rg_source=rg_source.id,
                        rg_dest=rg_destination.id,
                    ),
                )
                rg_source.move_partition(rg_destination, partition)
            else:
                # Groups balanced or cannot be balanced further
                break
            # Re-compute under and over-replicated replication-groups
            over_replicated_rgs, under_replicated_rgs = separate_groups(
                self.rgs.values(),
                lambda g: g.count_replica(partition),
            )

    def _elect_source_replication_group(
        self,
        over_replicated_rgs,
        partition,
    ):
        """Decide source replication-group based as group with highest replica
        count.
        """
        return max(
            over_replicated_rgs,
            key=lambda rg: rg.count_replica(partition),
        )

    def _elect_dest_replication_group(
        self,
        replica_count_source,
        under_replicated_rgs,
        partition,
    ):
        """Decide destination replication-group based on replica-count."""
        min_replicated_rg = min(
            under_replicated_rgs,
            key=lambda rg: rg.count_replica(partition),
        )
        # Locate under-replicated replication-group with lesser
        # replica count than source replication-group
        if min_replicated_rg.count_replica(partition) < replica_count_source - 1:
            return min_replicated_rg
        return None

    # Re-balancing partition count across brokers
    def rebalance_groups_partition_cnt(self):
        """Re-balance partition-count across replication-groups.

        Algorithm:
        The key constraint is not to create any replica-count imbalance while
        moving partitions across replication-groups.
        1) Divide replication-groups into over and under loaded groups in terms
           of partition-count.
        2) For each over-loaded replication-group, select eligible partitions
           which can be moved to under-replicated groups. Partitions with greater
           than optimum replica-count for the group have the ability to donate one
           of their replicas without creating replica-count imbalance.
        3) Destination replication-group is selected based on minimum partition-count
           and ability to accept one of the eligible partition-replicas.
        4) Source and destination brokers are selected based on :-
            * their ability to donate and accept extra partition-replica respectively.
            * maximum and minimum partition-counts respectively.
        5) Move partition-replica from source to destination-broker.
        6) Repeat steps 1) to 5) until groups are balanced or cannot be balanced further.
        """
        # Segregate replication-groups based on partition-count
        over_loaded_rgs, under_loaded_rgs, _ = smart_separate_groups(
            self.rgs.values(),
            lambda rg: len(rg.partitions),
        )
        if over_loaded_rgs and under_loaded_rgs:
            self.log.info(
                'Over-loaded replication-groups {over_loaded}, under-loaded '
                'replication-groups {under_loaded} based on partition-count'
                .format(
                    over_loaded=[rg.id for rg in over_loaded_rgs],
                    under_loaded=[rg.id for rg in under_loaded_rgs],
                )
            )
        else:
            self.log.info('Replication-groups are balanced based on partition-count.')
            return

        # Get optimal partition-count per replication-group
        opt_partition_cnt, _ = compute_group_optimum(
            self.rgs.values(),
            lambda rg: len(rg.partitions),
        )
        # Balance replication-groups
        for over_loaded_rg in over_loaded_rgs:
            for under_loaded_rg in under_loaded_rgs:
                # Filter unique partition with replica-count > opt-replica-count
                # in over-loaded-rgs and <= opt-replica-count in under-loaded-rgs
                eligible_partitions = set(filter(
                    lambda partition:
                    over_loaded_rg.count_replica(partition) >
                        len(partition.replicas) // len(self.rgs) and
                    under_loaded_rg.count_replica(partition) <=
                        len(partition.replicas) // len(self.rgs),
                    over_loaded_rg.partitions,
                ))
                # Move all possible partitions
                for eligible_partition in eligible_partitions:
                    over_loaded_rg.move_partition_replica(
                        under_loaded_rg,
                        eligible_partition,
                    )
                    # Move to next replication-group if either of the groups got
                    # balanced, otherwise try with next eligible partition
                    if (len(under_loaded_rg.partitions) == opt_partition_cnt or
                            len(over_loaded_rg.partitions) == opt_partition_cnt):
                        break
                if len(over_loaded_rg.partitions) == opt_partition_cnt:
                    # Move to next over-loaded replication-group if balanced
                    break

    # Re-balancing partition count across brokers
    def rebalance_brokers(self):
        """Rebalance partition-count across brokers within each replication-group."""
        for rg in self.rgs.values():
            rg.rebalance_brokers()

    # Re-balancing leaders
    def rebalance_leaders(self):
        """Re-order brokers in replicas such that, every broker is assigned as
        preferred leader evenly.
        """
        opt_leader_cnt = len(self.partitions) // len(self.brokers)
        # Balanced brokers transfer leadership to their under-balanced followers
        self.rebalancing_non_followers(opt_leader_cnt)

    def rebalancing_non_followers(self, opt_cnt):
        """Transfer leadership to any under-balanced followers on the pretext
        that they remain leader-balanced or can be recursively balanced through
        non-followers (followers of other leaders).

        Context:
        Consider a graph G:
        Nodes: Brokers (e.g. b1, b2, b3)
        Edges: From b1 to b2 s.t. b1 is a leader and b2 is its follower
        State of nodes:
            1. Over-balanced/Optimally-balanced: (OB)
                if leadership-count(broker) >= opt-count
            2. Under-balanced (UB) if leadership-count(broker) < opt-count
            leader-balanced: leadership-count(broker) is in [opt-count, opt-count+1]

        Algorithm:
            1. Use Depth-first-search algorithm to find path between
            between some UB-broker to some OB-broker.
            2. If path found, update UB-broker and delete path-edges (skip-partitions).
            3. Continue with step-1 until all possible paths explored.
        """
        under_brokers = filter(
            lambda b: b.count_preferred_replica() < opt_cnt,
            self.brokers.values(),
        )
        if under_brokers:
            skip_brokers, skip_partitions = [], []
            for broker in under_brokers:
                skip_brokers.append(broker)
                broker.request_leadership(opt_cnt, skip_brokers, skip_partitions)

        over_brokers = filter(
            lambda b: b.count_preferred_replica() > opt_cnt + 1,
            self.brokers.values(),
        )
        # Any over-balanced brokers tries to donate their leadership to followers
        if over_brokers:
            skip_brokers, used_edges = [], []
            for broker in over_brokers:
                skip_brokers.append(broker)
                broker.donate_leadership(opt_cnt, skip_brokers, used_edges)
