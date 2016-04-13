def extract_yelp_replication_group(self, broker):
    """Extract Yelp replication group from a Broker instance.
    The replication group is the habitat of the broker, which in EC2
    corresponds to the Availability Zone of the broker. This information is
    extracted from the hostname of the broker.
    """
    try:
        hostname = broker.metadata['host']
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
        error_msg = "Could not parse replication group for broker {id} with" \
            " hostname:{hostname}".format(id=broker.id, hostname=hostname)
        self.log.exception(error_msg)
        raise ValueError(error_msg)
    return rg_name
