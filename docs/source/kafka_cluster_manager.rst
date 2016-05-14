Kafka-Cluster-Manager
*********************
This tool provides a set of commands to manipulate and modify the cluster topology
and get metrics for different states of the cluster. These include balancing the
cluster-state, decommissioning brokers, evaluating metrics for the current state of
the cluster. Each of these commands is as described below.

Rebalancing cluster
===================
This command provides the functionality to re-distribute partitions across the
cluster to bring it into a more balanced state. The goal is to load balance the
cluster based on the distribution of the replicas across replication-groups
(availability-zones or racks), distribution of partitions and leaderships across
brokers. The imbalance state of a cluster has been characterized into 4 different layers.

.. note:: The tool is very conservative while rebalancing the cluster, ensuring
    that assignment is sent to zookeeper in chunks, controlling the number of
    partition movements and preferred-leader changes.

Replica-distribution
--------------------
    * Uniform distribution of replicas across availability-zones.

    .. code-block:: bash

      $ kafka-cluster-manager --cluster-type sample_type rebalance --replication-groups

Partition distribution
-----------------------
    * Uniform distribution of partitions across groups and brokers.

    .. code-block:: bash

       $ kafka-cluster-manager --cluster-type sample_type rebalance --brokers

Broker as leaders distribution
------------------------------
    * Some brokers might be elected as leaders for more partitions than others.
      This creates load-imbalance for these brokers. Balancing this layer ensures
      the uniform election of brokers as leaders.

      .. note:: The rebalancing of this layer doesn't move any partitions across brokers.

      It re-elects a new leader for the partitions to ensure that every broker is chosen
      as a leader uniformly. Also, we consider each partition equally, independently from
      the partition size/rate.

      .. code-block:: bash

          $ kafka-cluster-manager --cluster-type sample_type rebalance --leaders

Topic-partition distribution
----------------------------
    * Uniform distribution of partitions of the same topic across brokers.

The command provides the ability to balance one or more of these layers except for
the topic-partition imbalance layer which will be balanced implicitly with replica or
partition rebalancing.

:py:mod:`kafka_tools.kafka_cluster_manager.cluster_topology` provides APIs to create
a cluster-topology object based on the distribution of topics, partitions, brokers and
replication-groups across the cluster.


.. autoclass:::wq


Cluster-topology
----------------

.. code-block:: python

   from kafka_tools.kafka_cluster_manager.cluster_info.cluster_topology import ClusterTopology

   # Create cluster-topology object from given assignment
   ct = ClusterTopology(assignment, brokers, extract_group)
   # Create the balanced assignment
   assignment = self.build_balanced_assignment(ct.assignment, ct)
   # Process the assignment to be sent to zookeeper to modify cluster-topology
   rebalance_cluster.process_assignment(assignment)

Decommission-Brokers
====================
This command provides functionality for decommissioning a given list of brokers. The key
idea is to move all partitions from brokers that are going to be decommissioned to other
brokers in their replication-group while keeping the cluster balanced as above.

.. note:: While decommissioning brokers we need to ensure that we have at least 'n' number
   of brokers per replication group where n is the max replication-factor of a partition.

.. code-block:: python

    # Decommission given list of brokers
    cluster_topology.decommission_brokers(broker_ids)
    # Process and send the new reduced-assignment to zookeeper
    decommission_brokers.process_assignment(reduced_assignment)

Stats
=====
This command provides statistics for the current imbalance state of the cluster. It also
provides imbalance statistics of the cluster if a given partition-assignment plan were
to be applied to the cluster. The details include the imbalance value of each of the above
layers for the overall cluster, each broker and across each replication-group.

Store assignments
=================
Display the current cluster-topology in valid json format.


Usage examples
==============

Rebalancing all layers
----------------------

Rebalance all layers for given cluster. This command will generate a plan with a
maximum of 10 partition movements and 25 leader-only changes after rebalancing
the cluster for all layers discussed before prior to sending it to zookeeper.

.. code-block:: bash
    $ kafka-cluster-manager --cluster-type <type> rebalance --replication-groups
    --brokers --leaders  --apply --max-partition-movements 10 --max-leader-changes 25


Imbalance Statistics
--------------------
Get imbalance statistics of the current cluster-state.

.. code-block:: bash
    kafka-cluster-manager --cluster-type sample_type stats
