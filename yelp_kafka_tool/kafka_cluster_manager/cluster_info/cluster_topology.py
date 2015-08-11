"""Contains information for partition layout on given cluster.

Also contains api's dealing with dealing with changes in partition layout.
The steps (1-6) and states S0 -- S2 and algorithm for re-assigning partitions
is per the design document at:-

https://docs.google.com/document/d/1qloANcOHkzuu8wYVm0ZAMCGY5Mmb-tdcxUywNIXfQFI
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
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


class ClusterTopology(object):
    """Represent a Kafka cluster and functionalities supported over the cluster.

    A Kafka cluster topology consists of:
    replication group (alias rg), broker, topic and partition.
    """
    def __init__(self, zk):
        self._name = zk.cluster_config.name
        self._zk = zk
        logging.basicConfig()
        self.log = logging.getLogger(self.__class__.__name__)
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
            self.log.error(
                "Could not parse replication group for %s with hostname:%s",
                broker.id,
                hostname,
            )
        return rg_name

    def reassign_partitions(
        self,
        rebalance_option,
        max_changes,
        to_execute,
    ):
        """Display or execute the final-state based on rebalancing option."""
        pass

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

    def partition_imbalance(self):
        """Report partition count imbalance over brokers for given assignment
        or assignment in current state.
        """
        return get_partition_imbalance_stats(self.brokers.values())

    def leader_imbalance(self):
        """Report leader imbalance count over each broker."""
        return get_leader_imbalance_stats(
            self.brokers.values(),
            self.partitions.values(),
        )

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
