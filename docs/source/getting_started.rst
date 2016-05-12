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
layers for the overall cluster, each broker and across each availability-zone.

Store assignments
=================
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

Rolling restart
***************

The kafka-rolling-restart script can be used to safely restart an entire
cluster, one server at a time. The script finds all the servers in a cluster,
checks their health status and executes the restart.

Cluster health
==============

The health of the cluster is defined in terms of broker availability and under
replicated partitions. Kafka-rolling-restart will check that all brokers are
answering to JMX requests, and that the total numer of under replicated
partitions is zero. If both conditions are fulfilled, the cluster is considered
healthy and the next broker will be restarted.

The JMX metrics are accessed via `Jolokia <https://jolokia.org>`_, which must be
running on all brokers.

.. note:: If a broker is not registered in Zookeeper when the tool is executed,
   it will not appear in the list of known brokers and it will be ignored.

Parameters
==========

The parameters specific for kafka-rolling-restart are:

* :code:`--check-interval INTERVAL`: the number of seconds between each check.
  Default 10.
* :code:`--check-count COUNT`: the number of consecutive checks that must result
  in cluster healthy before restarting the next server. Default 12.
* :code:`--unhealthy-time-limit LIMIT`: the maximum time in seconds that a
  cluster can be unhealthy for. If the limit is reached, the script will
  terminate with an error. Default 600.
* :code:`--jolokia-port PORT`: The Jolokia port. Default 8778.
* :code:`--jolokia-prefix PREFIX`: The Jolokia prefix. Default "jolokia/".
* :code:`--no-confirm`: If specified, the script will not ask for confirmation.
* :code:`--skip N`: Skip the first N servers. Useful to recover from a partial
  rolling restart. Default 0.
* :code:`--verbose`: Turn on verbose output.

Examples
========

Restart the generic dev cluster, checking the JXM metrics every 30 seconds, and
restarting the next broker after 5 consecutive checks have confirmed the health
of the cluster:

.. code-block:: bash

   kafka-rolling-restart --cluster-type generic --cluster-name dev --check-interval 30 --check-count 5

Check the generic prod cluster. It will report an error if the cluster is
unhealthy for more than 900 seconds:

.. code-block:: bash

   kafka-rolling-restart --cluster-type generic --cluster-name prod --unhealthy-time-limit 900
