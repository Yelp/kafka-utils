Cluster Manager
***************
This tool provides a set of commands to manipulate and modify the cluster topology
and get metrics for different states of the cluster. These include balancing the
cluster-state, decommissioning brokers, evaluating metrics for the current state of
the cluster. Each of these commands is as described below.

Replication group parser
========================
The tool supports the grouping of brokers in replication groups.
:code:`kafka-cluster-manager` will try to distribute replicas of the same partition
across different replication group.
The user can use this feature to map replication groups to failure zones, so that
a balanced cluster will be more resilient to zone failures.

By default all brokers are considered as part of a single replication group.
Custom replication group parsers can be defined by extending the class
:code:`ReplicationGroupParser` as shown in the example below:

.. code:: python

	from kafka_utils.kafka_cluster_manager.cluster_info.replication_group_parser \
		import ReplicationGroupParser


	class SampleGroupParser(ReplicationGroupParser):

		def get_replication_group(self, broker):
			"""Extract the replication group from a Broker instance.
			Suppose each broker hostname is in the form broker-rack<n>, this
			function will return "rack<n>" as replication group
			"""
			if broker.inactive:
				# Can't extract replication group from inactive brokers because they
				# don't have metadata
				return None
			hostname = broker.metadata['host']
			return hostname.rsplit('-', 1)[1]


Create a file named :code:`sample_parser.py` into a directory containing the
:code:`__init__.py`.

Example:

.. code-block:: none

   $HOME/parser
     |-- __init__.py
     |-- sample_parser.py


To use the custom parser:

.. code-block:: bash

    $ kafka-cluster-manager --cluster-type sample_type --group-parser $HOME/parser:sample_parser rebalance --replication-groups

Cluster Balancers
=================
Every command attempts to find a partition assignment that improves or
maintains the balance of the cluster. This tool provides two different cluster
balancers that implement different cluster balancing strategies. The
`Partition Count Balancer`_ is the default cluster balancer and is recommended
for most users. The `Genetic Balancer`_ is recommended for users that are able
to provide partition measurements. See `partition measurement`_ for more
information.

Partition Count Balancer
------------------------
This balancing strategy attempts to balance the number of partitions and
leaders across replication groups and brokers. Balancing is done in four
stages.

1. **Replica distribution**: Uniform distribution of partition replicas across
   replication groups.
2. **Partition distribution**: Uniform distribution of partitions across groups
   and brokers.
3. **Leader distribution**: Uniform distribution of preferred partition leaders
   across brokers.
4. **Topic-partition distribution**: Uniform distribution of partitions of the
   same topic across brokers.

Genetic Balancer
----------------
This balancing strategy considers not only the number of partitions on each
broker, but the weight of each partition (see `partition measurement`_). It
uses a genetic algorithm to approximate the optimal assignment while also
limiting the total size of the partitions moved.

The uniform distribution of replicas across replication groups is guaranteed
by an initial stage that greedily reassigns replicas across replication groups.

The fitness function used by the genetic algorithm to score partition
assignments considers the following:

1. **Broker weight balance**: The sum of the weights of the partitions on each
   broker should be as balanced as possible.
2. **Leader weight balance**: The sum of the weights of the preferred leader
   partitions on each broker should be as balanced as possible.
3. **Weighted topic-partition balance**: The distribution of partitions of the
   same topic across brokers weighted by the total weight of each topic.

The Genetic Balancer can be enabled by using the :code:`--genetic-balancer`
toggle.

Partition Measurement
=====================
Throughput can vary significantly across the topics of a cluster. To
prevent placing too many high-throughput partitions on the same brokers, the
cluster manager needs additional information about each partition. For the
purposes of this tool, there are two values that need to be defined for each
partition: weight and size.

The weight of a partition is a measure of how much load that partition places
on the broker that it is assigned to. The weight can have any unit and should
represent the relative weight of one partition compared to another.  For
example a partition with weight 2 is assumed to cause twice as much load on a
broker as a partition with weight 1. In practice, a possible metric could be
the average byte in/out rate over a certain time period.

The size of a partition is a measure of how expensive it is to move the
partition. This value is also relative and can have any unit. The length of the
partition's log in bytes is one possible metric.

Since Kafka doesn't keep detailed partition usage information, the task of
collecting this information is left to the user. By default every partition is
given an equal weight and size of 1. Custom partition measurement approaches
can be implemented by extending the :code:`PartitionMeasurer` class. Here is a
sample measurer that pulls partition metrics from an external service.

.. code-block:: python

    import argparse
    from requests import get

    from kafka.kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer \
            import PartitionMeasurer

    class SampleMeasurer(PartitionMeasurer):

        def __init__(self, cluster_config, brokers, assignment, args):
            super(SampleMeasurer, self).__init__(
                cluster_config,
                brokers,
                assignment,
                args
            )
            self.metrics = {}
            for partition_name in assignment.keys():
                self.metrics[partition_name] = get(self.args.metric_url +
                    "/{cluster_type}/{cluster_name}/{topic}/{partition}"
                    .format(
                        cluster_type=cluster_config.type,
                        cluster_name=cluster_config.name,
                        topic=partition_name[0],
                        partition=partition_name[1],
                    )
                ).json()

        def parse_args(self, measurer_args):
            parser = argparse.ArgumentParser(prog='SampleMeasurer')
            parser.add_argument(
                '--metric-url',
                type=string,
                required=True,
                help='URL of the metric service.',
            )
            return parser.parse_args(measurer_args, self.args)

        def get_weight(self, partition_name):
            return self.metrics[partition_name]['bytes_in_per_sec'] + \
                self.metrics[partition_name]['bytes_out_per_sec']

        def get_size(self, partition_name):
            return self.metrics[partition_name]['size']

Place this file in a file called :code:`sample_measurer.py` and place it in a
python module.

Example:

.. code-block:: none

   $HOME/measurer
     |-- __init__.py
     |-- sample_measurer.py

To use the partition measurer:

.. code-block:: bash

    $ kafka-cluster-manager \
    --cluster-type sample_type \
    --partition-measurer $HOME/measurer:sample_measurer \
    --measurer-args "--metric-url $METRIC_URL" \
    stats

Cluster rebalance
=================
This command provides the functionality to re-distribute partitions across the
cluster to bring it into a more balanced state. The behavior of this command
is determined by the choice of `cluster balancer <#cluster-balancers>`_.

The command provides three toggles to control how the cluster is rebalanced:

- :code:`--replication-groups`: Rebalance partition replicas across replication
  groups.
- :code:`--brokers`: Rebalance partitions across brokers.
- :code:`--leaders`: Rebalance partition preferred leaders across brokers.

The command also provides toggles to control how many partitions are moved at
once:

- :code:`--max-partition-movements`: The maximum number of partition replicas
  that will be moved. Default: 1.
- :code:`--max-leader-changes`: The maximum number of partition preferred
  leader changes. Default: 5.
- :code:`--max-movement-size`: The maximum total size of the partition replicas
  that will be moved. Default: No limit.
- :code:`--auto-max-movement-size`: Set :code:`--max-movement-size` to the
  size of the largest partition in the cluster.
- :code:`--score-improvement-threshold`: When the `Genetic Balancer`_ is being
  used, this option checks the `Genetic Balancer`_'s scoring function and only
  applies the new assignment if the improvement in score is greater than this
  threshold.

.. code-block:: bash

    $ kafka-cluster-manager --group-parser $HOME/parser:sample_parser --apply
    --cluster-type sample_type rebalance --replication-groups --brokers --leaders
    --max-partition-movements 10 --max-leader-changes 25

Or using the `Genetic Balancer`_:

.. code-block:: bash

    $ kafka-cluster-manager --group-parser $HOME/parser:sample_parser --apply
    --cluster-type sample_type --genetic-balancer --partition-measurer
    $HOME/measurer:sample_measurer rebalance --replication-groups --brokers
    --leaders --max-partition-movements 10 --max-leader-changes 25
    --auto-max-partition-size --score-improvement-threshold 0.01

Brokers decommissioning
=======================
This command provides functionalities to decommission a given list of brokers. The key
idea is to move all partitions from brokers that are going to be decommissioned to other
brokers in either their replication group (preferred) or others replication groups
while keeping the cluster balanced as above.

.. note:: While decommissioning brokers we need to ensure that we have at least 'n' number
   of active brokers where n is the max replication-factor of a partition.

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type decommission 123456 123457 123458

Or using the `Genetic Balancer`_:

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type --genetic-balancer
  --partition-measurer $HOME/measurer:sample_measurer decommission
  123456 123457 123458

Revoke Leadership
=================
This command provides functionalities to revoke leadership for a particular given
set of brokers. The key idea is to move leadership for all partitions on given brokers
to other brokers while keeping the cluster balanced.

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type revoke-leadership 123456 123457 123458

Set Replication Factor
======================
This command provides the ability to increase or decrease the replication-factor
of a topic. Replicas are added or removed in such a way that the balance of the
cluster is maintained. Additionally, when the replication-factor is decreased,
any out-of-sync replicas will be removed first.

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type set_replication_factor --topic sample_topic 3


Or using the `Genetic Balancer`_:

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type --genetic-balancer
  --partition-measurer $HOME/measurer:sample_measurer set_replication_factor
  --topic sample_topic 3

Stats
=====
This command provides statistics for the current imbalance state of the cluster. It also
provides imbalance statistics of the cluster if a given partition-assignment plan were
to be applied to the cluster. The details include the imbalance value of each of the above
layers for the overall cluster, each broker and across each replication-group.

.. code-block:: bash

    $ kafka-cluster-manager --group-parser $HOME/parser:sample_parser --cluster-type
    sample_type stats

Store assignments
=================
Dump the current cluster-topology in json format.

.. code-block:: bash

    $ kafka-cluster-manager --group-parser $HOME/parser:sample_parser --cluster-type
    sample_type store_assignments
