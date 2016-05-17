Kafka Check
***********

Checking in-sync replicas
=========================
This kafka tool provides the ability to check in-sync replicas for each topic-partition
in the cluster.

The :code:`min_isr` command checks if the number of in-sync replicas for a
partition is equal or greater than the minimum number of in-sync replicas
configured for the topic the partition belongs to. A topic specific
:code:`min.insync.replicas` overrides the given default.

The parameters for min_isr check are:

* :code:`--default_min_isr DEFAULT_MIN_ISR`: Default min.isr value for cases without
  settings in Zookeeper for some topics.
* :code:`--data-path DATA_PATH`: Path to the Kafka data folder.
* :code:`--controller-only`: If this parameter is specified, it will do nothing and
  succeed on non-controller brokers. If :code:`--broker-id` is also set as -1
  then broker-id will be computed from given data-path.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr 
   OK: All replicas in sync.

In case of min isr violations:
.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr --default_min_isr 3

    isr=2 is lower than min_isr=3 for sample_topic:0
    CRITICAL: 1 partition(s) have the number of replicas in sync that is lower
    than the specified min ISR.

