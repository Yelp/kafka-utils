"""This file incorporates handling partitions over replication-groups
(Availability-zones) in our case.
"""
import logging

from .util import compute_optimal_count


class ReplicationGroup(object):
    """Represent attributes and functions specific to replication-groups
    (Availability zones) abbreviated as rg.
    """
    def __init__(self, id, brokers=None):
        self._id = id
        self._brokers = brokers or set()
        self.log = logging.getLogger(self.__class__.__name__)

    @property
    def id(self):
        """Return name of replication-groups."""
        return self._id

    @property
    def brokers(self):
        """Return list of brokers."""
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

    def move_partition(
            self,
            rg_destination,
            victim_partition,
    ):
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
        broker_source.move_partition(victim_partition, broker_destination)

    def _select_broker_pair(
        self,
        rg_destination,
        victim_partition,
    ):
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
        # Get overloaded brokers in source replication-group
        over_loaded_brokers = self._select_over_loaded_brokers(
            victim_partition,
        )
        # Get underloaded brokers in destination replication-group
        under_loaded_brokers = rg_destination._select_under_loaded_brokers(
            victim_partition,
        )
        broker_source = self._elect_source_broker(over_loaded_brokers)
        broker_destination = self._elect_dest_broker(
            under_loaded_brokers,
            victim_partition,
        )
        return broker_source, broker_destination

    def _select_over_loaded_brokers(
        self,
        victim_partition,
    ):
        """Get over-loaded brokers as sorted broker in partition-count and
        containing victim-partition.
        """
        over_loaded_brokers = [
            broker
            for broker in self._brokers
            if victim_partition in broker.partitions
        ]
        return sorted(
            over_loaded_brokers,
            key=lambda b: len(b.partitions),
            reverse=True,
        )

    def _select_under_loaded_brokers(
        self,
        victim_partition,
    ):
        """Get brokers in ascending sorted order of partition-count
        not containing victim-partition.
        """
        under_loaded_brokers = [
            broker
            for broker in self._brokers
            if victim_partition not in broker.partitions
        ]
        return sorted(under_loaded_brokers, key=lambda b: len(b.partitions))

    def _elect_source_broker(self, over_loaded_brokers):
        """Select first broker from given brokers having victim_partition."""
        return over_loaded_brokers[0]

    def _elect_dest_broker(self, under_loaded_brokers, victim_partition):
        """Select first broker from under_loaded_brokers preferring not having
        partition of same topic as victim partition.
        """
        # Pick broker having least partitions of the given topic
        broker_topic_partition_cnt = [
            (broker, broker.count_partitions(victim_partition.topic))
            for broker in under_loaded_brokers
        ]
        min_count_pair = min(
            broker_topic_partition_cnt,
            key=lambda ele: ele[1],
        )
        return min_count_pair[0]

    # Re-balancing brokers
    def rebalance_brokers(self):
        """Rebalance partition-count across brokers on given replication-group.

        If no replication-group is given, brokers are balanced across all
        brokers of the cluster as a whole.
        """
        # Get optimal value
        opt_partition_count, extra_partition_cnt = \
            compute_optimal_count(len(self.partitions), len(self.brokers))
        # Count of brokers which shall have 1 more partition than optimal-count
        extra_partition_per_broker = int(extra_partition_cnt > 0)

        # Segregate partitions
        over_loaded_brokers, under_loaded_brokers = self._segregate_brokers(
            opt_partition_count,
            extra_partition_per_broker,
        )

        while under_loaded_brokers and over_loaded_brokers:
            # Get best-fit source broker, destination-broker and partition to be moved
            broker_source, broker_dest, victim_partition = self.get_target_brokers(
                over_loaded_brokers,
                under_loaded_brokers,
                extra_partition_per_broker,
            )
            # No valid source or target brokers found
            if not broker_source or not broker_dest:
                break
            # Move partition
            broker_source.move_partition(victim_partition, broker_dest)
            # Remove newly balanced-brokers if any
            if len(broker_source.partitions) == opt_partition_count:
                over_loaded_brokers.remove(broker_source)
            if len(broker_dest.partitions) == opt_partition_count + extra_partition_per_broker:
                under_loaded_brokers.remove(broker_dest)

    def _segregate_brokers(self, opt_partition_count, extra_partition_per_broker):
        """Segregate brokers in terms of partition count into over-loaded
        or under-loaded partitions.
        """
        over_loaded_brokers = []
        under_loaded_brokers = []
        for broker in self.brokers:
            partition_cnt = len(broker.partitions)
            if partition_cnt > opt_partition_count:
                over_loaded_brokers.append(broker)
            elif partition_cnt < opt_partition_count:
                under_loaded_brokers.append(broker)
            else:
                if extra_partition_per_broker:
                    under_loaded_brokers.append(broker)
        return (
            sorted(over_loaded_brokers, key=lambda b: len(b.partitions), reverse=True),
            sorted(under_loaded_brokers, key=lambda b: len(b.partitions)),
        )

    def get_target_brokers(
        self,
        over_loaded_brokers,
        under_loaded_brokers,
        extra_partition_per_broker,
    ):
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
        # pick source-broker from over-loaded
        preferred_source = None
        preferred_dest = None
        preferred_partition = None
        min_sibling_partition_cnt = -1
        for b_source in over_loaded_brokers:
            for b_dest in under_loaded_brokers:
                if b_source.is_relatively_unbalanced(
                    b_dest,
                    extra_partition_per_broker,
                ):
                    best_fit_partition, sibling_partition_cnt = \
                        b_source.get_eligible_partition(b_dest)
                    if sibling_partition_cnt < min_sibling_partition_cnt \
                            or min_sibling_partition_cnt == -1:
                        min_sibling_partition_cnt = sibling_partition_cnt
                        preferred_source = b_source
                        preferred_dest = b_dest
                        preferred_partition = best_fit_partition
        return preferred_source, preferred_dest, preferred_partition
