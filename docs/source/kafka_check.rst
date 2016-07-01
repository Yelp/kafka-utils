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

Checking under replicated partitions
====================================
This kafka tool provides the ability to check and report number of under replicated
partitions for all brokers in the cluster.

The :code:`under_replicated` command checks if the number of under replicated partitions
is equal to zero. It will report the aggregated result of under replicated partitions
of each broker if any.

The parameters specific to under_replicated check are:

* :code:`--first-broker-only`: If this parameter is specified, the command will
  check for under-replicated partitions for given broker only if it's the first
  broker in broker-list fetched from zookeeper. Otherwise, it does nothing and succeeds.
  If :code:`--broker-id` is also set as -1 then broker-id will be computed from given
  data-path.
* :code:`--minimum-replication MINIMUM_REPLICATION`: Minimum number of in-sync replicas
  for under replicated partition. If the current number of in-sync replicas for partition which has
  under replicated replicas below that param, the check will tell about this topic-partition.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type under_replicated
   OK: No under replicated partitions.

In case of not first broker in the broker list in Zookeeper:

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type --broker-id 3 under_replicated --first-broker-only
   OK: Provided broker is not the first in broker-list.

In case where some partitions are under-replicated.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type under_replicated
   CRITICAL: 2 under replicated partitions.
