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

    def decrease_leader_count(self, partitions, leaders_per_broker, opt_count):
        """Re-order eligible replicas to balance preferred leader assignment.

        @params:
        self:               Current object is leader-broker with > opt_count as
                            leaders and will be tried to reduce the same.
        partitions:         Set of all partitions in the cluster.
        leaders_per_broker: Broker-as-leader-count per broker.
        opt_count:          Optimal value for each broker to act as leader.
        """
        # Generate a list of partitions for which we can change the leader.
        # Filter out partitions with one replica (Replicas cannot be changed).
        # self is current-leader
        possible_partitions = [
            partition
            for partition in partitions
            if self == partition.leader and len(partition.replicas) > 1
        ]
        for partition in possible_partitions:
            for broker in partition.followers:
                if (leaders_per_broker[broker] < opt_count and
                        leaders_per_broker[self] -
                        leaders_per_broker[broker] > 1):
                    # Assign 'broker' as new leader for 'partition'
                    broker.grant_leadership(partition, leaders_per_broker)
                    break
            if leaders_per_broker[self] == opt_count:
                return

    def grant_leadership(self, partition, leaders_per_broker):
        """Assign broker as new leader of given partition and return previous
        leader.

        Also, update the leaders-per-broker map.
        """
        curr_leader = partition.swap_leader(self)
        leaders_per_broker[self] += 1
        leaders_per_broker[curr_leader] -= 1
        return curr_leader

    def request_leadership(self, curr_leaders_cnt, opt_count, skip_brokers, skip_partitions):
        """
        @key_terms:
        leader-balanced: Count of brokers as leader is atleast opt-count
        grant-leadership: Swap current leader with some of its follower

        Algorithm:
        =========
        Step1: Current broker is will request leadership from current-leaders
        Step2: Current-leaders will grant their leadership if one of these happens:-
            a) Either they remain leader-balanced
            b) Or they will recursively request leadership from other partitions
               untill they are become leader-balanced
            If both of these conditions fail, they will revoke their leadership-grant
        Step 3: If current-broker becomes leader-balanced it will return otherwise
                it moves ahead with next partition
        """
        # Possible partitions which can grant leadership to broker
        eligible_partitions = [
            p for p in self.partitions
            if len(p.replicas) > 1 and self is not p.leader
        ]
        for partition in eligible_partitions:
            # Partition not available to grant leadership when:-
            # 1. Broker is already under leadership change or
            # 2. Partition has already granted leadership before
            if partition.leader in skip_brokers or partition in skip_partitions:
                continue
            # Current broker is granted leadership temporarily
            prev_leader = self.grant_leadership(partition, curr_leaders_cnt)
            # Continue if prev-leader remains balanced
            if curr_leaders_cnt[prev_leader] >= opt_count:
                # If current broker is leader-balanced return else
                # request next-partition
                skip_partitions.append(partition)
                if curr_leaders_cnt[self] >= opt_count:
                    return curr_leaders_cnt
                else:
                    continue
            else:  # prev-leader (broker) became unbalanced
                # Append skip-brokers list so that it is not unbalanced further
                skip_brokers.append(prev_leader)
                # Try recursively arrange leadership for prev-leader
                new_leaders_cnt = prev_leader.request_leadership(
                    dict(curr_leaders_cnt),
                    opt_count,
                    skip_brokers,
                    skip_partitions,
                )
                # If prev-leader couldn't be leader-balanced
                # revert its previous grant to current-broker
                if new_leaders_cnt[prev_leader] < opt_count:
                    prev_leader.grant_leadership(partition, curr_leaders_cnt)
                    # Trying requestng leadership from next partition
                    continue
                else:
                    # If prev-leader successfully balanced
                    skip_partitions.append(partition)
                    # Removing from skip-broker list, since it can now again be
                    # used for granting leadership for some other partition
                    skip_brokers.remove(prev_leader)
                    if new_leaders_cnt[self] >= opt_count:
                        # Return if curren-broker is leader-balaned
                        return new_leaders_cnt
                    else:
                        curr_leaders_cnt = new_leaders_cnt
                        continue
        return curr_leaders_cnt
