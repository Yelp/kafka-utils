Check ISR
*********

This kafka tool provides the ability to check in-sync replicas for each topic-partition
in the cluster with configuration for that topic in Zookeeper or default min.isr param
if it is specified and there is no settings in Zookeeper for partition. 

Subcommands
===========

* min_isr

Checking in-sync replicas
=========================

The :code:`min_isr` command checks if replicas of each topic-partition is in-sync
with configuration for that topic in Zookeeper or with :code:`--default_min_isr`,
if provided.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr 
   OK: All replicas in sync.

In case of mismatch in isr:
.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr --default_min_isr 3

    isr=2 is lower than min_isr=3 for sample_topic:0
    CRITICAL: 1 partition(s) have the number of replicas in sync that is lower
    than the specified min ISR.

Parameters
==========

The parameters specific for kafka-check are:

* :code:`--default_min_isr DEFAULT_MIN_ISR`: Default min.isr value for cases without
  settings in Zookeeper for some topics.
* :code:`--data-path DATA_PATH`: Path to the Kafka data folder.
* :code:`--controller-only`: If this parameter is specified, it will do nothing and
  succeed on non-controller brokers.
