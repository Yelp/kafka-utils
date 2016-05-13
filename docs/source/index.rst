Kafka-Tools v\ |version|
########################

Description
***********
Kafka-Tools is a library containing tools to interact with kafka clusters and manage them. The tool provides utilities
like listing of all the clusters, balancing the partition distribution across brokers and replication-groups, managing
consumer groups, rolling-restarts of the cluster, checking of min-isr status of cluster partitions.

For more information about Apache Kafka see the official `Kafka documentation`_.

How to install
**************

Kafka-tools
***********
This tool provides the version information of the library and lists the cluster information.

Usage Examples
--------------

List of the version of tool.

.. option::
    kafka-tools -v

List available clusters configuration data.

.. option::
    kafka-tools


.. _Kafka documentation: http://kafka.apache.org/documentation.html#introduction

.. toctree::
   :maxdepth: -1

   self
   config
   kafka_cluster_manager
   kafka_rolling_restart


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
