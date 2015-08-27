from yelp_kafka_tool.kafka_cluster_manager.reassign.broker_rebalance import (
    segregate_brokers,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    get_optimal_metrics,
)


def get_target_replication_groups(
    under_replicated_rgs,
    over_replicated_rgs,
    partition,
):
    """Decide source replication-group and destination replication-group
    based on partition-count.
    """
    # TODO: decide based on partition-count
    rg_source = over_replicated_rgs[0]
    # Move partitions in under-replicated replication-groups
    # until the group is empty
    rg_destination = None
    # Locate under-replicated replication-group with lesser
    # replica count than source replication-group
    for under_replicated_rg in under_replicated_rgs:
        if under_replicated_rg.count_replica(partition) < \
                rg_source.count_replica(partition) - 1:
            rg_destination = under_replicated_rg
            break
    return rg_source, rg_destination


def rebalance_replicas(partitions, brokers, rgs):
    """Rebalance given segregated replication-groups."""
    # Balance replicas over replication-groups for each partition
    for partition in partitions:
        # Get optimal replica count
        opt_replica_count, extra_replicas_cnt, evenly_distribute_replicas = \
            get_optimal_metrics(partition.replication_factor, len(rgs))

        # Segregate replication-groups into under and over replicated
        under_replicated_rgs, over_replicated_rgs = \
            segregate_replication_groups(
                rgs,
                partition,
                opt_replica_count,
                evenly_distribute_replicas,
            )

        # Move replicas from over-replicated to under-replicated groups
        while under_replicated_rgs and over_replicated_rgs:
            # Decide source and destination group
            rg_source, rg_destination = get_target_replication_groups(
                under_replicated_rgs,
                over_replicated_rgs,
                partition,
            )

            if rg_destination and rg_source:
                assert(rg_source in over_replicated_rgs)
                assert(rg_destination in under_replicated_rgs)
                assert(partition.name in [p.name for p in rg_source.partitions])

                # Get current count stats
                dest_repica_cnt = rg_destination.count_replica(partition)
                source_replica_cnt = rg_source.count_replica(partition)
                total_partitions_all = len(get_all_partitions(rgs))
                opt_partition_count, extra_partition_count, evenly_distribute_partitions = \
                    get_optimal_metrics(total_partitions_all, len(brokers))
                # Actual movement of partition
                move_partition(
                    rg_source,
                    rg_destination,
                    partition,
                    opt_partition_count,
                    evenly_distribute_partitions,
                )

                # Remove group if balanced
                if source_replica_cnt == opt_partition_count:
                    over_replicated_rgs.remove(rg_source)
                if dest_repica_cnt + evenly_distribute_replicas == \
                        opt_replica_count:
                    under_replicated_rgs.remove(rg_destination)
            else:
                # Groups are balanced OR could not be further balanced
                break

            # Re-sort replication-groups based on current-replica count
            sorted(
                under_replicated_rgs,
                key=lambda rg: rg.count_replica(partition),
            )
            sorted(
                over_replicated_rgs,
                key=lambda rg: rg.count_replica(partition),
                reverse=True,
            )


def segregate_replication_groups(
    rgs,
    partition,
    opt_replica_count,
    evenly_distribute_replicas,
):
    """Separate replication-groups into under-replicated, over-replicated
    and optimally replicated groups.
    """
    under_replicated_rgs = []
    over_replicated_rgs = []
    replication_factor = partition.replication_factor
    for rg in rgs:
        replica_cnt = rg.count_replica(partition)
        if replica_cnt < opt_replica_count:
            under_replicated_rgs.append(rg)
        elif replica_cnt > opt_replica_count:
            over_replicated_rgs.append(rg)
        else:
            if evenly_distribute_replicas:
                # Case 2: Rp % G == 0: Replication-groups should have same replica-count
                # Nothing to be done since it's replication-group is already balanced
                pass
            else:
                # Case 1 or 3: Rp % G !=0: Rp < G or Rp > G
                # Helps in adjusting one extra replica if required
                under_replicated_rgs.append(rg)
    return (
        sorted(under_replicated_rgs, key=lambda rg: rg.count_replica(partition)),
        sorted(over_replicated_rgs, key=lambda rg: rg.count_replica(partition), reverse=True),
    )


def get_all_partitions(rgs):
    """Return list of partitions across all brokers."""
    return [partition for rg in rgs for partition in rg.partitions]


def move_partition(
    rg_source,
    rg_destination,
    victim_partition,
    opt_partition_count,
    evenly_distribute_partitions,
):
    """Move partition(victim) from current replication-group to destination
    replication-group.

    Step 1: Get overloaded and underloaded brokers
    Step 2: Evaluate source and destination broker
    Step 3: Move partition from source-broker to destination-broker

    Decide source broker and destination broker to move the partition.
    """
    # Get overloaded brokers in source replication-group
    over_loaded_brokers = segregate_brokers(
        rg_source.brokers,
        opt_partition_count,
        evenly_distribute_partitions,
    )[0]
    if not over_loaded_brokers:
        over_loaded_brokers = sorted(
            rg_source.brokers,
            key=lambda b: len(b.partitions),
            reverse=True,
        )

    # Get underloaded brokers in destination replication-group
    under_loaded_brokers = segregate_brokers(
        rg_destination.brokers,
        opt_partition_count,
        evenly_distribute_partitions,
    )[1]
    if not under_loaded_brokers:
        under_loaded_brokers = sorted(
            rg_destination.brokers, key=lambda b: len(b.partitions)
        )

    # Select best-fit source and destination brokers for partition
    # Best-fit is based on partition-count and presence/absence of
    # Same topic-partition over brokers
    assert(over_loaded_brokers and under_loaded_brokers)
    broker_source, broker_destination = broker_selection(
        over_loaded_brokers,
        under_loaded_brokers,
        victim_partition,
    )
    assert(victim_partition.name in [p.name for p in broker_source.partitions])
    assert(broker_source in rg_source.brokers)
    assert(broker_destination in rg_destination.brokers)
    assert(rg_destination.id != rg_source.id)

    # Actual-movement of victim-partition
    broker_source.move_partition(victim_partition, broker_destination)


def broker_selection(
        over_loaded_brokers,
        under_loaded_brokers,
        victim_partition,
):
    """Select best-fit source and destination brokers based on partition
    count and presence of partition over the broker.

    Best-fit Selection Criteria:
    Source broker: Select broker containing the victim-partition with
    maximum partitions.
    Destination broker: NOT containing the victim-partition with minimum
    partitions. If no such broker found, return first broker.

    This helps in ensuring:-
    * Topic-partitions are distributed across brokers.
    * Partition-count is balanced across replication-groups.
    """
    broker_source = None
    broker_destination = None
    for broker in over_loaded_brokers:
        partition_ids = [p.name for p in broker.partitions]
        if victim_partition.name in partition_ids:
            broker_source = broker
            break

    # Pick broker not having topic in that broker
    preferred_destination = None
    for broker in under_loaded_brokers:
        topic_ids = [partition.topic.id for partition in broker.partitions]
        # TODO: change this to check for count of topic-ids and not presence
        if victim_partition.topic.id not in topic_ids:
            preferred_destination = broker
            break
    # If no valid broker found pick broker with minimum partitions
    if preferred_destination:
        broker_destination = preferred_destination
    else:
        broker_destination = under_loaded_brokers[0]

    assert(
        broker_source
        and broker_destination
        and broker_destination != broker_source
    )
    return broker_source, broker_destination
