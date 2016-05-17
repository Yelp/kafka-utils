Kafka-Utils v\ |version|
########################

Description
***********
Kafka-Utils is a library containing tools to interact with kafka clusters and manage them. The tool provides utilities
like listing of all the clusters, balancing the partition distribution across brokers and replication-groups, managing
consumer groups, rolling-restart of the cluster, cluster healthchecks.

For more information about Apache Kafka see the official `Kafka documentation`_.

How to install
**************
.. code-block:: bash

    $ pip install kafka-utils


List available clusters.

.. code-block:: bash

    $ kafka-utils
    Cluster type sample_type:
        Cluster name: cluster-1
        broker list: cluster-elb-1:9092
        zookeeper: 11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1


.. _Kafka documentation: http://kafka.apache.org/documentation.html#introduction

.. toctree::
   :maxdepth: -1

   config
   kafka_cluster_manager
   kafka_consumer_manager
   kafka_rolling_restart
   kafka_check


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
