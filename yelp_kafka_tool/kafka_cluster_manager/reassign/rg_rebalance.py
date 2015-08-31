from yelp_kafka_tool.kafka_cluster_manager.reassign.broker_rebalance import (
    segregate_brokers,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    compute_optimal_count,
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
        opt_replica_count, extra_replicas_cnt = \
            compute_optimal_count(partition.replication_factor, len(rgs))

        # Segregate replication-groups into under and over replicated
        evenly_distribute_replicas = bool(not extra_replicas_cnt)
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

                # Get current count stats
                dest_replica_cnt = rg_destination.count_replica(partition)
                source_replica_cnt = rg_source.count_replica(partition)
                total_partitions_all = len(get_all_partitions(rgs))
                opt_partition_count, extra_partition_count = \
                    compute_optimal_count(total_partitions_all, len(brokers))

                evenly_distribute_partitions = bool(not extra_partition_count)
                # Actual movement of partition
                rg_source.move_partition(
                    rg_destination,
                    partition,
                    opt_partition_count,
                    evenly_distribute_partitions,
                )

                # Remove group if balanced
                if source_replica_cnt == opt_partition_count:
                    over_replicated_rgs.remove(rg_source)
                if dest_replica_cnt + evenly_distribute_replicas == \
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
