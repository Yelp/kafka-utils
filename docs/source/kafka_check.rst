Kafka Check
***********

The kafka-check command performs multiple checks on the health of the cluster.
Each subcommand will run a different check. The tool can run on the broker
itself or on any other machine, and it will check the health of the entire
cluster.

One possible way to deploy the tool is to install the kafka-utils package on
every broker, and schedule kafka-check to run periodically on each machine
with cron. Kafka-check provides two simple coordination mechanisms to make
sure that the check only runs on a single broker per cluster.

Coordination strategies:
* First broker only: the script will only run on the broker with lowest
  broker id.
* Controller only: the script will only run on the controller of the cluster.

Coordination parameters:
* :code:`--broker-id`: the id of the broker where the script is running.
  Set it to -1 if automatic broker ids are used.
* :code:`--data-path DATA_PATH`: Path to the Kafka data folder, used in case of
  automatic broker ids to find the assigned id.
* :code:`--controller-only`: if is specified, the script will only run on the
  controller. The execution on other brokers won't perform any check and it
  will always succeed.
* :code:`--first-broker-only`: if specified, the command will only perform the
  check if broker_id is the lowest broker id in the cluster. If it is not the '
  lowest, it will not perform any check and succeed immediately.

Checking in-sync replicas
=========================
The :code:`min_isr` subcommand checks if the number of in-sync replicas for a
partition is equal or greater than the minimum number of in-sync replicas
configured for the topic the partition belongs to. A topic specific
:code:`min.insync.replicas` overrides the given default.

The parameters for min_isr check are:

* :code:`--default_min_isr DEFAULT_MIN_ISR`: Default min.isr value for cases without
  settings in Zookeeper for some topics.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr
   OK: All replicas in sync.

In case of min isr violations:

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type min_isr --default_min_isr 3

    isr=2 is lower than min_isr=3 for sample_topic:0
    CRITICAL: 1 partition(s) have the number of replicas in sync that is lower
    than the specified min ISR.

Checking replicas available
===========================
The :code:`replica_unavailability` subcommand checks if the number of replicas not
available for communication is equal to zero. It will report the aggregated result
of unavailable replicas of each broker if any.

The parameters specific to replica_unavailability check are:

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type replica_unavailability
   OK: All replicas available for communication.

In case of not first broker in the broker list in Zookeeper:

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type --broker-id 3 replica_unavailability --first-broker-only
   OK: Provided broker is not the first in broker-list.

In case where some partitions replicas not available for communication.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type replica_unavailability
   CRITICAL: 2 replica(s) unavailable for communication.

Checking offline partitions
===========================
The :code:`offline` subcommand checks if there are any offline partitions in the cluster.
If any offline partition is found, it will terminate with an error, indicating the number
of offline partitions.

.. code-block:: bash

   $ kafka-check --cluster-type=sample_type offline
   CRITICAL: 64 offline partitions.
