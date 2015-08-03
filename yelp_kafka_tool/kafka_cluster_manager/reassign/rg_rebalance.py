

def rebalance_replicas(partitions, brokers, rgs):
    """Rebalance given segregated replication-groups."""
    for partition in partitions.values():
        # Fetch potentially under-replicated and over-replicated
        # replication-groups for each partition
        under_replicated_rgs, over_replicated_rgs = \
            segregate_replication_groups(partition, rgs)
        replication_factor = len(partition.replicas)
        rg_count = len(rgs)
        opt_replica_count = replication_factor // rg_count

        # Move partition-replicas from over-replicated to under-replicated
        # replication-groups
        for rg_source in over_replicated_rgs:
            # Keep reducing partition-replicas over source over-replicated
            # replication-group until either it is evenly-replicated
            # or no under-replicated replication-group is found
            source_replica_cnt = replica_count(partition, rg_source)
            while source_replica_cnt > opt_replica_count:
                # Move partitions in under-replicated replication-groups
                # until the group is empty
                rg_destination = None
                # Locate under-replicated replication-group with lesser
                # replica count than source replication-group
                for rg_under in under_replicated_rgs:
                    if replica_count(partition, rg_under) < \
                            source_replica_cnt - 1:
                        rg_destination = rg_under
                        break
                if rg_destination:
                    total_brokers_cluster = len(brokers)
                    total_partitions_cluster = len(get_all_partitions(rgs))
                    # Actual movement of partition
                    rg_source.move_partition(
                        partition,
                        rg_destination,
                        total_brokers_cluster,
                        total_partitions_cluster,
                    )
                    if replica_count(partition, rg_destination) == opt_replica_count:
                        under_replicated_rgs.remove(rg_destination)
                else:
                    # Destination under-replicated replication-group not found
                    # Partition is evenly-replicated for source replication-group
                    # Or under-replicated replication-groups is empty
                    break
            if source_replica_cnt > opt_replica_count + 1:
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
                if replica_count(partition, rg) < opt_replica_count:
                    print(
                        '[WARNING] Could not re-balance under-replicated'
                        'replication-group {rg_id} for partition '
                        '{topic}:{p_id}'.format(
                            rg_id=rg.id,
                            partition=partition.topic.id,
                            p_id=partition.partition_id,
                        )
                    )


def segregate_replication_groups(partition, rgs):
    """Separate replication-groups into under-replicated, over-replicated
    and optimally replicated groups.
    """
    under_replicated_rgs = []
    over_replicated_rgs = []
    replication_factor = len(partition.replicas)
    rg_count = len(rgs)
    opt_replica_count = replication_factor // len(rgs)
    for rg in rgs.values():
        replica_cnt = replica_count(partition, rg)
        if replica_cnt < opt_replica_count:
            under_replicated_rgs.append(rg)
        elif replica_cnt > opt_replica_count:
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


def get_all_partitions(rgs):
    """Return list of partitions across all brokers."""
    partitions = []
    for rg in rgs.itervalues():
        partitions += rg.partitions
    return partitions


def move_partition(
    rg_source,
    rg_destination,
    victim_partition,
    total_brokers_cluster,
    total_partitions_cluster,
):
    """Move partition(victim) from current replication-group to destination
    replication-group.

    Step 1: Get overloaded and underloaded brokers
    Step 2: Evaluate source and destination broker
    Step 3: Move partition from source-broker to destination-broker

    Decide source broker and destination broker to move the partition.
    """
    # Get overloaded brokers in source replication-group
    over_loaded_brokers = get_over_loaded_brokers(
        rg_source,
        total_brokers_cluster,
        total_partitions_cluster,
        victim_partition,
    )
    if not over_loaded_brokers:
        over_loaded_brokers = sorted(
            rg_source.brokers,
            key=lambda b: len(b.partitions),
            reverse=True,
        )

    # Get underloaded brokers from destination replication-group
    under_loaded_brokers = get_under_loaded_brokers(
        rg_destination,
        total_brokers_cluster,
        total_partitions_cluster,
    )
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


def get_over_loaded_brokers(
    replication_group,
    total_brokers,
    total_partitions,
    partition=None,
):
    """Get list of overloaded brokers in sorted order containing
    the more partitions than optimal count.
    """
    opt_partition_count = total_partitions // total_brokers
    over_loaded_brokers = [
        broker for broker in replication_group.brokers
        if broker.partition_count() > opt_partition_count + 1
    ]

    if partition:
        result = [
            broker for broker in over_loaded_brokers
            if partition.name in [p.name for p in broker.partitions]
        ]
    else:
        result = over_loaded_brokers
    return sorted(
        result,
        key=lambda b: len(b.partitions),
        reverse=True,
    )


def get_under_loaded_brokers(
    replication_group,
    total_brokers_cluster,
    total_partitions_cluster,
    partition=None,
):
    """Get list of brokers with lesser partitions than optimal amount for
    each broker containing the given partition.
    """
    opt_partition_count = total_partitions_cluster // total_brokers_cluster
    under_loaded_brokers = [
        broker for broker in replication_group.brokers
        if broker.partition_count() < opt_partition_count
    ]
    if partition:
        result = [
            broker for broker in under_loaded_brokers
            if partition.name in [p.name for p in broker.partitions]
        ]
    else:
        result = under_loaded_brokers
    return sorted(result, key=lambda b: len(b.partitions))


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
    for broker in under_loaded_brokers:
        topic_ids = [partition.topic.id for partition in broker.partitions]
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


def replica_count(partition, replication_group):
    """Return the count of replicas of given partitions."""
    return len(
        [
            broker for broker in partition.replicas
            if broker in replication_group._brokers
        ]
    )
