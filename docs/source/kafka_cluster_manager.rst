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

	from kafka_tools.kafka_cluster_manager.cluster_info.replication_group_parser \
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

Cluster rebalance
=================
This command provides the functionality to re-distribute partitions across the
cluster to bring it into a more balanced state. The goal is to load balance the
cluster based on the distribution of the replicas across replication-groups
(availability-zones or racks), distribution of partitions and leaderships across
brokers. The imbalance state of a cluster has been characterized into 4 different layers.

.. note:: The tool is very conservative while rebalancing the cluster, ensuring
    that large assignments are executed in smaller chunks, controlling the number of
    partition movements and preferred-leader changes.

Replica-distribution
--------------------
Uniform distribution of replicas across replication groups.

.. code-block:: bash

   $ kafka-cluster-manager --cluster-type sample_type rebalance --replication-groups

Partition distribution
-----------------------
Uniform distribution of partitions across groups and brokers.

.. code-block:: bash

   $ kafka-cluster-manager --cluster-type sample_type rebalance --brokers

Broker as leaders distribution
------------------------------
Some brokers might be elected as leaders for more partitions than others.
This creates load-imbalance for these brokers. Balancing this layer ensures
the uniform election of brokers as leaders.

.. note:: The rebalancing of this layer doesn't move any partitions across brokers.

It re-elects a new leader for the partitions to ensure that every broker is chosen
as a leader uniformly. The tool does not take into account partition size.

.. code-block:: bash

   $ kafka-cluster-manager --cluster-type sample_type rebalance --leaders

Topic-partition distribution
----------------------------
Uniform distribution of partitions of the same topic across brokers.

The command provides the ability to balance one or more of these layers except for
the topic-partition imbalance layer which will be balanced implicitly with replica or
partition rebalancing.

:py:mod:`kafka_tools.kafka_cluster_manager.cluster_topology` provides APIs to create
a cluster-topology object based on the distribution of topics, partitions, brokers and
replication-groups across the cluster.

Rebalancing all layers
----------------------

Rebalance all layers for given cluster. This command will generate a plan with a
maximum of 10 partition movements and 25 leader-only changes after rebalancing
the cluster for all layers discussed before prior to sending it to zookeeper.

.. code-block:: bash

    $ kafka-cluster-manager --group-parser $HOME/parser:sample_parser --apply
    --cluster-type sample_type rebalance --replication-groups --brokers --leaders
    --max-partition-movements 10 --max-leader-changes 25

Brokers decommission
====================
This command provides functionality for decommissioning a given list of brokers. The key
idea is to move all partitions from brokers that are going to be decommissioned to other
brokers in either their replication group (preferred) or others replication groups
while keeping the cluster balanced as above.

.. note:: While decommissioning brokers we need to ensure that we have at least 'n' number
   of active brokers where n is the max replication-factor of a partition.

.. code-block:: bash

  $ kafka-cluster-manager --cluster-type sample_type decommission 123456 123457 123458

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
