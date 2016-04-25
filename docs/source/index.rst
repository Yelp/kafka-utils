Yelp-Kafka-Tools v\ |version|
#############################

Yelp-Kafka-Tools is a library containing tools to interact with kafka clusters and manage them.
For more information about Apache Kafka see the official `Kafka documentation`_.


.. _Kafka documentation: http://kafka.apache.org/documentation.html#introduction

Getting Started
###############

Kafka-cluster-manager
*********************
This tool provides a set of commands to manipulate and modify the cluster topology
and get metrics for different states of the cluster. These include balancing the
cluster-state, decommissioning brokers, evaluating metrics for the current state of
the cluster. Each of these commands is as described below.

Rebalancing cluster
===================
This command provides the functionality to re-distribute partitions across the cluster
to bring it into a more balanced state. The goal is to load balance the cluster based
on the distribution of the replicas across availability-zones (replication-groups or racks),
distribution of partitions across the brokers, ingress-rate load on leaders. The
imbalance-state of cluster has been characterized into 4 different layers :-

Replica-distribution
--------------------
    -- Uniform distribution of replicas across availability-zones.

Partition distribution
-----------------------
    -- Uniform distribution of partitions across groups and brokers.

Broker as leaders distribution
------------------------------
    -- Some brokers might be elected as leaders for more partitions than others.
    This creates load-imbalance for these brokers. Balancing this layer ensures
    the uniform election of brokers as leaders.
.. note:: The rebalancing of this layer doesn't move any partitions across brokers.
It re-elects a new leader for the partitions to ensure that every broker is chosen
as a leader uniformly. Also, we consider each partition equally, independently from
the partition size/rate.

Topic-partition distribution
----------------------------
    -- Uniform distribution of partitions of the same topic across brokers.

The command provides the ability to balance one or more of these layers except for
the topic-partition imbalance layer which will be balanced implicitly with replica or
partition rebalancing.

:py:mod:`yelp_kafka_tool.kafka_cluster_manager.cluster_topology` provides APIs to create
a cluster-topology object based on the distribution of topics, partitions, brokers and
replication-groups across the cluster.


.. autoclass:::wq


Cluster-topology
----------------

.. code-block:: python

   from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import ClusterTopology

   # Create cluster-topology object from given assignment
   ct = ClusterTopology(assignment, brokers, extract_group)
   # Create the balanced assignment
   assignment = self.build_balanced_assignment(ct.assignment, ct)
   # Process the assignment to be sent to zookeeper to modify cluster-topology
   rebalance_cluster.process_assignment(assignment)

Decommission-Brokers
********************
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
*****
This command provides statistics for the current imbalance state of the cluster. It also
provides imbalance statistics of the cluster if a given partition-assignment plan were
to be applied to the cluster. The details include the imbalance value of each of the above
layers for the overall cluster, each broker and across each availability-zone.

Store assignments
*****************
Display the current cluster-topology in valid json format.

Kafka-consumer-manager
**********************

This kafka tool provides the ability to view and manipulate consumer offsets for a specific
consumer group. For a given cluster, this tool provides  us with the following functionalities:-

Manipulating consumer-groups
============================

Listing consumer-groups subscribed to the cluster. Copying, deleting and renaming of the group.

Manipulating offsets
====================
For a given consumer-group, fetching current offsets, low and high watermarks for topics and
partitions subscribed to the group.
Setting, advancing, rewinding, saving and restoring of current-offsets.

Manipulating topics
===================

For a given consumer-group and cluster, listing and unsubscribing topics.

.. toctree::
   :maxdepth: 2


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
