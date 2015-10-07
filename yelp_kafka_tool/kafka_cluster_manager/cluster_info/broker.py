import logging


class Broker(object):
    """Broker class object, consisting of following attributes
        -id: Id of broker
        -partitions: partitions under a given broker
    """
    def __init__(self, id, partitions=None):
        self._id = id
        self._partitions = partitions or set()
        self.log = logging.getLogger(self.__class__.__name__)

    def get_hostname(self, zk):
        """Get hostname of broker from zookeeper."""
        try:
            hostname = zk.get_brokers(self._id)
            result = hostname[self._id]['host']
        except KeyError:
            self.log.warning(
                'Unknown host for broker {broker}. Returning as'
                ' localhost'.format(broker=self._id)
            )
            result = 'localhost'
        return result

    @property
    def partitions(self):
        return self._partitions

    @property
    def id(self):
        return self._id

    @property
    def topics(self):
        """Return the set of topics current in broker."""
        return set([partition.topic for partition in self._partitions])

    def remove_partition(self, partition):
        """Remove partition from partition list."""
        if partition in self._partitions:
            # Remove partition from set
            self._partitions.remove(partition)
            # Remove broker from replica list of partition
            partition.replicas.remove(self)
        else:
            raise ValueError(
                'Partition: {topic_id}:{partition_id} not found in broker '
                '{broker_id}'.format(
                    topic_id=partition.topic.id,
                    partition_id=partition.partition_id,
                    broker_id=self._id,
                )
            )

    def add_partition(self, partition):
        """Add partition to partition list."""
        assert(partition not in self._partitions)
        # Add partition to existing set
        self._partitions.add(partition)
        # Add broker to replica list
        partition.replicas.append(self)

    def move_partition(self, partition, broker_destination):
        """Move partition to destination broker and adjust replicas."""
        self.remove_partition(partition)
        broker_destination.add_partition(partition)

    def count_partitions(self, topic):
        """Return count of partitions for given topic."""
        return sum([
            1
            for p in self._partitions
            if p.topic == topic
        ])

    def count_preferred_replica(self):
        """Return number of times broker is set as preferred leader."""
        return sum(
            [1 for partition in self.partitions if partition.leader == self],
        )

    def request_leadership(self, opt_count, skip_brokers, skip_partitions, optimal=False):
        """Under-balanced broker requests leadership from current leader, on the
        pretext that it recursively can maintain its leadership count as optimal.

        @key_terms:
        leader-balanced: Count of brokers as leader is at least opt-count

        Algorithm:
        =========
        Step-1: Broker will request leadership from current-leader of partitions
                it belongs to.
        Step-2: Current-leaders will grant their leadership if one of these happens:-
            a) Either they remain leader-balanced.
            b) Or they will recursively request leadership from other partitions
               until they are become leader-balanced.
            If both of these conditions fail, they will revoke their leadership-grant
        Step-3: If current-broker becomes leader-balanced it will return
                otherwise it moves ahead with next partition.
        """
        # Possible partitions which can grant leadership to broker
        owned_partitions = filter(
            lambda p: self is not p.leader and len(p.replicas) > 1,
            self.partitions,
        )
        for partition in owned_partitions:
            # Partition not available to grant leadership when:-
            # 1. Broker is already under leadership change or
            # 2. Partition has already granted leadership before
            if partition.leader in skip_brokers or partition in skip_partitions:
                continue
            # Current broker is granted leadership temporarily
            prev_leader = partition.swap_leader(self)
            # Continue if prev-leader remains balanced
            if prev_leader.count_preferred_replica() >= opt_count:
                # If current broker is leader-balanced return else
                # request next-partition
                skip_partitions.append(partition)
                if self.count_preferred_replica() >= opt_count:
                    return True
                else:
                    continue
            else:  # prev-leader (broker) became unbalanced
                # Append skip-brokers list so that it is not unbalanced further
                skip_brokers.append(prev_leader)
                # Try recursively arrange leadership for prev-leader
                prev_leader.request_leadership(opt_count, skip_brokers, skip_partitions)
                # If prev-leader couldn't be leader-balanced
                # revert its previous grant to current-broker
                if prev_leader.count_preferred_replica() < opt_count:
                    partition.swap_leader(prev_leader)
                    # Try requesting leadership from next partition
                    continue
                else:
                    # If prev-leader successfully balanced
                    skip_partitions.append(partition)
                    # Removing from skip-broker list, since it can now again be
                    # used for granting leadership for some other partition
                    skip_brokers.remove(prev_leader)
                    if self.count_preferred_replica() >= opt_count:
                        # Return if current-broker is leader-balanced
                        return True
                    else:
                        continue
        # Leadership-grant unsuccessful
        return False

    def donate_leadership(self, opt_count, skip_brokers, skip_partitions):
        owned_partitions = filter(
            lambda p: self is p.leader and len(p.replicas) > 1,
            self.partitions,
        )
        for partition in owned_partitions:
            # Skip partition if already considered, before
            # TODO: partition can be considered again for different follower?
            if partition in skip_partitions:
                continue
            potential_new_leaders = filter(
                lambda f: f not in skip_brokers,
                partition.followers,
            )
            for new_leader in potential_new_leaders:
                prev_leader = partition.swap_leader(new_leader)
                assert(prev_leader == self)
                # new-leader didn't imbalance
                if new_leader.count_preferred_replica() <= opt_count + 1:
                    skip_partitions.append(partition)
                    # over-broker balanced
                    if self.count_preferred_replica() <= opt_count + 1:
                        return True
                    else:
                        # Try new-leader
                        continue
                else:  # new-leader (broker) became over-balanced
                    skip_brokers.append(new_leader)
                    new_leader.donate_leadership(opt_count, skip_brokers, skip_partitions)
                    # new-leader couldn't be balanced, revert
                    if new_leader.count_preferred_replica() > opt_count + 1:
                        partition.swap_leader(self)
                        # try next leader or partition
                        continue
                    else:
                        # new-leader was successfuly balanced
                        skip_partitions.append(partition)
                        # new-leader can be reused
                        skip_brokers.remove(new_leader)
                        # now broker is balanced
                        if self.count_preferred_replica() <= opt_count + 1:
                            return True
                        else:
                            # Further reduction required
                            continue
        return False
