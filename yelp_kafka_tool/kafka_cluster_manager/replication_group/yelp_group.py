import logging

log = logging.getLogger(__name__)


def extract_yelp_replication_group(broker):
    """Extract Yelp replication group from a Broker instance.
    The replication group is the habitat of the broker, which in EC2
    corresponds to the Availability Zone of the broker. This information is
    extracted from the hostname of the broker.
    """
    if broker.inactive:
        # Can't extract replication group from inactive brokers because they
        # don't have metadata
        return None
    try:
        hostname = broker.metadata['host']
        if 'localhost' in hostname:
            log.warning(
                "Setting replication-group as localhost for broker %s",
                broker.id,
            )
            rg_name = 'localhost'
        else:
            habitat = hostname.rsplit('-', 1)[1]
            rg_name = habitat.split('.', 1)[0]
    except IndexError:
        error_msg = "Could not parse replication group for broker {id} with" \
            " hostname:{hostname}".format(id=broker.id, hostname=hostname)
        log.exception(error_msg)
        raise ValueError(error_msg)
    return rg_name
