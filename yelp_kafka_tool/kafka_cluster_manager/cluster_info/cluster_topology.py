"""Contains information for partition layout on given cluster.

Also contains api's dealing with dealing with changes in partition layout.
The steps (1-6) and states S0 -- S2 and algorithm for re-assgining partitions
is per the design document at:-

https://docs.google.com/document/d/1qloANcOHkzuu8wYVm0ZAMCGY5Mmb-tdcxUywNIXfQFI
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys
from collections import OrderedDict
from math import sqrt

from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface

from .broker import Broker
from .partition import Partition
from .rg import ReplicationGroup
from .topic import Topic
from .util import (
    _display_same_replica_count_rg,
    _display_same_topic_partition_count_broker,
    _display_partition_count_per_broker,
    _display_leader_count_per_broker,
)


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

    def _get_partitions_per_broker(self):
        """Return partition count for each broker."""
        partitions_per_broker = dict(
            (broker, len(broker.partitions))
            for replication_group in self.rgs.itervalues()
            for broker in replication_group.brokers
        )
        return OrderedDict(
            sorted(partitions_per_broker.items(), key=lambda x: x[0].id)
        )

    def _get_leaders_per_broker(self):
        """Return count for each broker the number of times
        it is assigned as preferred leader.
        """
        leaders_per_broker = dict(
            (broker, 0)
            for replication_group in self.rgs.itervalues()
            for broker in replication_group.brokers
        )
        for partition in self.partitions.itervalues():
            leaders_per_broker[partition.leader] += 1
        return leaders_per_broker

    def _get_same_topic_partition_count_per_broker(self):
        """Return count of topics and partitions on each broker having multiple
        partitions of same topic.

        :rtype dict(broker_id: (same-topic-count, same-topic-partition-count))
        """
        brokers_same_topic_partition_count = {}
        for replication_group in self.rgs.values():
            for broker in replication_group.brokers:
                partition_count = sum(
                    broker.get_per_topic_partitions_count().values()
                )
                if partition_count > 0:
                    brokers_same_topic_partition_count[broker] = \
                        partition_count - 1
        return brokers_same_topic_partition_count

    # Get imbalance stats
    def _standard_deviation(self, data):
        avg_data = sum(data) * 1.0 / len(data)
        variance = map(lambda x: (x - avg_data) ** 2, data)
        avg_variance = sum(variance) * 1.0 / len(data)
        return sqrt(avg_variance)

    def _actual_imbalance(self, count_per_broker):
        """Calculate and return actual imbalance based on given count of
        partitions or leaders per broker.
        """
        actual_imbalance = 0
        opt_count = sum(count_per_broker) // \
            len(count_per_broker)
        more_opt_count_allowed = sum(count_per_broker) % \
            len(count_per_broker)
        for count in count_per_broker:
            if count > opt_count:
                if more_opt_count_allowed > 0:
                    more_opt_count_allowed -= 1
                    actual_imbalance += (count - opt_count - 1)
                else:
                    actual_imbalance += (count - opt_count)
        return actual_imbalance

    def partition_imbalance(self):
        """Report partition count imbalance over brokers for given assignment
        or assignment in current state.
        """
        partitions_per_broker = self._get_partitions_per_broker()

        # Calculate standard deviation of partition imbalance
        stdev_imbalance = self._standard_deviation(
            partitions_per_broker.values()
        )
        # Actual total imbalance of partition count over all brokers
        actual_imbalance = self._actual_imbalance(
            partitions_per_broker.values()
        )
        _display_partition_count_per_broker(
            self,
            partitions_per_broker,
            stdev_imbalance,
            actual_imbalance,
        )
        return stdev_imbalance, actual_imbalance

    def leader_imbalance(self):
        """Report leader imbalance count over each broker."""
        leaders_per_broker = self._get_leaders_per_broker()

        # Calculate standard deviation of leader imbalance
        stdev_imbalance = self._standard_deviation(leaders_per_broker.values())

        # Calcuation actual imbalance
        actual_imbalance = self._actual_imbalance(leaders_per_broker.values())
        _display_leader_count_per_broker(
            self,
            leaders_per_broker,
            stdev_imbalance,
            actual_imbalance,
        )
        return stdev_imbalance, actual_imbalance

    def topic_imbalance(self):
        """Calculate count of partitions of same topic over a broker."""
        same_topic_partition_count = \
            self._get_same_topic_partition_count_per_broker()
        _display_same_topic_partition_count_broker(
            self,
            same_topic_partition_count,
        )
        return sum(same_topic_partition_count.values())

    def replication_group_imbalance(self):
        """Calculate same replica count over each replication-group.
        Can only be calculated on current cluster-state.
        """
        same_replica_per_rg = dict((rg_id, 0) for rg_id in self.rgs.keys())

        # Get broker-id to rg-id map
        broker_rg_id = {}
        for rg in self.rgs.itervalues():
            for broker in rg.brokers:
                broker_rg_id[broker.id] = rg.id

        # Evaluate duplicate replicas count in each replication-group
        for partition in self.partitions.itervalues():
            rg_ids = []
            for broker in partition.replicas:
                rg_id = broker_rg_id[broker.id]
                # Duplicate replica found
                if rg_id in rg_ids:
                    same_replica_per_rg[rg_id] += 1
                else:
                    rg_ids.append(rg_id)
        rg_imbalance = sum(same_replica_per_rg.values())
        _display_same_replica_count_rg(self, same_replica_per_rg)
        return rg_imbalance
