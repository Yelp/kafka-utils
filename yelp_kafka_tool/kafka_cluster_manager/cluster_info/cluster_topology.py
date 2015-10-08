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

from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface
from .broker import Broker
from .partition import Partition
from .rg import ReplicationGroup
from .topic import Topic
from .stats import (
    get_partition_imbalance_stats,
    get_leader_imbalance_stats,
    get_topic_imbalance_stats,
    get_replication_group_imbalance_stats,
)
from .util import (
    compute_optimal_count,
    get_assignment_map,
)


class ClusterTopology(object):
    """Represent a Kafka cluster and functionalities supported over the cluster.

    A Kafka cluster topology consists of:
    replication group (alias rg), broker, topic and partition.
    """
    def __init__(self, zk):
        self._name = zk.cluster_config.name
        self._zk = zk
        self.log = logging.getLogger(self.__class__.__name__)
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

            # Updating corresponding topic object
            topic.add_partition(partition)

            # Updating corresponding broker objects
            for broker_id in replica_ids:
                broker = self.brokers[broker_id]
                broker.add_partition(partition)

    def _fetch_initial_assignment(self, broker_ids, topic_ids):
        """Fetch initial assignment from zookeeper.

        Assignment is ordered by partition name tuple.
        """
        # Requires running kafka-scripts
        self._initial_assignment = KafkaInterface().get_cluster_assignment(
            self._zk.cluster_config.zookeeper,
            broker_ids,
            topic_ids
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

    def reassign_partitions(self, replication_groups=False, leaders=False):
        """Rebalance current cluster-state to get updated state based on
        rebalancing option.
        """
        # Rebalance replication-groups
        if replication_groups:
            self.log.info(
                'Re-balancing replication groups: {groups}...'
                .format(groups=', '.join(self.rgs.keys())),
            )
            self.rebalance_replication_groups()
        if leaders:
            self.log.info(
                'Re-balancing leader-count across brokers: {brokers}...'
                .format(brokers=', '.join(str(self.brokers.keys()))),
            )
            self.rebalance_leaders()

    def get_assignment_json(self):
        """Build and return cluster-topology in json format."""
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
        return get_assignment_map(self.get_assignment_json())

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

    # Re-balancing analytical statistics
    def partition_imbalance(self):
        """Report partition count imbalance over brokers for given assignment
        or assignment in current state.
        """
        return get_partition_imbalance_stats(self.brokers.values())

    def leader_imbalance(self):
        """Report leader imbalance count over each broker."""
        return get_leader_imbalance_stats(self.brokers.values())

    def topic_imbalance(self):
        """Return count of topics and partitions on each broker having multiple
        partitions of same topic.
        """
        return get_topic_imbalance_stats(
            self.brokers.values(),
            self.topics.values(),
        )

    def replication_group_imbalance(self):
        """Calculate same replica count over each replication-group."""
        return get_replication_group_imbalance_stats(
            self.rgs.values(),
            self.partitions.values(),
        )

    # Balancing replication-groups: S0 --> S1
    def rebalance_replication_groups(self):
        """Rebalance partitions over replication groups (availability-zones)."""
        # Balance replicas over replication-groups for each partition
        for partition in self.partitions.itervalues():
            self._rebalance_partition(partition)

    def _rebalance_partition(self, partition):
        """Rebalance replication group for given partition."""
        # Get optimal replica count
        opt_replica_count, extra_replicas_cnt = \
            compute_optimal_count(
                partition.replication_factor,
                len(self.rgs.values()),
            )

        # Segregate replication-groups into under and over replicated
        evenly_distribute_replicas = not extra_replicas_cnt
        under_replicated_rgs, over_replicated_rgs = \
            self._segregate_replication_groups(
                partition,
                opt_replica_count,
                evenly_distribute_replicas,
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
                rg_source.move_partition(
                    rg_destination,
                    partition,
                )
                # Update replication groups after movement of partition
                source_replica_cnt = rg_source.count_replica(partition)
                dest_replica_cnt = rg_destination.count_replica(partition)
                # Remove group from list if balanced
                if source_replica_cnt == opt_replica_count:
                    over_replicated_rgs.remove(rg_source)
                allowed_dest_cnt = dest_replica_cnt + 1 \
                    if not evenly_distribute_replicas else dest_replica_cnt
                if allowed_dest_cnt == opt_replica_count:
                    under_replicated_rgs.remove(rg_destination)
            else:
                # Groups balanced or cannot be balanced further
                break

    def _segregate_replication_groups(
        self,
        partition,
        opt_replica_count,
        evenly_distribute_replicas,
    ):
        """Separate replication-groups into under-replicated, over-replicated
        and optimally replicated groups.
        """
        under_replicated_rgs = []
        over_replicated_rgs = []
        for rg in self.rgs.itervalues():
            replica_cnt = rg.count_replica(partition)
            if replica_cnt < opt_replica_count:
                under_replicated_rgs.append(rg)
            elif replica_cnt > opt_replica_count:
                over_replicated_rgs.append(rg)
            else:
                if not evenly_distribute_replicas:
                    # Case 1 or 3: Rp % G !=0: Rp < G or Rp > G
                    # Helps in adjusting one extra replica if required
                    under_replicated_rgs.append(rg)
        return under_replicated_rgs, over_replicated_rgs

    def _elect_source_replication_group(
        self,
        over_replicated_rgs,
        partition,
    ):
        """Decide source replication-group based as group with highest replica
        count.
        """
        # TODO: optimization: decide based on partition-count and
        # same-topic-partition count
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

    # Re-balancing leaders
    def rebalance_leaders(self):
        """Re-order brokers in replicas such that, every broker is assigned as
        preferred leader evenly.
        """
        opt_leader_cnt = len(self.partitions) // len(self.brokers)
        # Balanced brokers transfer leadership to their under-balanced followers
        self.rebalancing_non_followers(opt_leader_cnt)

    def rebalancing_followers(self, opt_leader_cnt):
        """Transfer leadership from current over-balanced leaders to their
        under-balanced followers.

        @key-term:
        over-balanced brokers:  Brokers with leadership-count > opt-count
                                Note: Even brokers with leadership-count
                                == opt-count are assumed as over-balanced.
        under-balanced brokers: Brokers with leadership-count < opt-count
        """
        for broker in self.brokers.values():
            if broker.count_preferred_replica() > opt_leader_cnt:
                broker.decrease_leader_count(
                    self.partitions.values(),
                    opt_leader_cnt,
                )

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
        # Any over-balanced brokers tries to donate thier leadership to followers
        if over_brokers:
            skip_brokers, used_edges = [], []
            for broker in over_brokers:
                skip_brokers.append(broker)
                broker.donate_leadership(opt_cnt, skip_brokers, used_edges)
