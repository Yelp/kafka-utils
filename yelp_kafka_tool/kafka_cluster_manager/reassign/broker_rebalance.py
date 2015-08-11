

def segregate_brokers(
    brokers,
    opt_partition_count,
    extra_partition_per_broker,
):
    """Segregate brokers in terms of partition count into over-loaded
    or under-loaded partitions.
    """
    over_loaded_brokers = []
    under_loaded_brokers = []
    for broker in brokers:
        if broker.partition_count > opt_partition_count:
            over_loaded_brokers.append(broker)
        elif broker.partition_count < opt_partition_count:
            under_loaded_brokers.append(broker)
        else:
            if extra_partition_per_broker:
                under_loaded_brokers.append(broker)
            else:
                # Case#1: partition-count % broker-count == 0
                # partition-count is optimal for given broker
                pass

    return (
        sorted(over_loaded_brokers, key=lambda b: len(b.partitions), reverse=True),
        sorted(under_loaded_brokers, key=lambda b: len(b.partitions)),
    )
