

def segregate_brokers(
    replication_group,
    opt_partition_count=None,
):
    """Segregate brokers in terms of partition count into over-loaded
    or under-loaded partitions.
    """
    if not opt_partition_count:
        opt_partition_count = len(replication_group.partitions) // \
            len(replication.brokers)
    over_loaded_brokers = []
    under_loaded_brokers = []
    for broker in replication_group.brokers:
        if broker.partition_count() > opt_partition_count:
            over_loaded_brokers.append(broker)
        elif broker.partition_count() < opt_partition_count:
            under_loaded_brokers.append(broker)
        else:
            # Case#1: partition-count % optimum-count == 0
            # partition-count is optimal
            if broker.partition_count() % opt_partition_count == 0:
                pass
            else:
                under_loaded_brokers.append(broker)

    return (
        sorted(over_loaded_brokers, key=lambda b: len(b.partitions), reverse=True,),
        sorted(under_loaded_brokers, key=lambda b: len(b.partitions)),
    )
