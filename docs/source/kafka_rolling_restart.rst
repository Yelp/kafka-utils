Rolling Restart
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

* ``--check-interval INTERVAL``: the number of seconds between each check.
  Default 10.
* ``--check-count COUNT``: the number of consecutive checks that must result
  in cluster healthy before restarting the next server. Default 12.
* ``--unhealthy-time-limit LIMIT``: the maximum time in seconds that a
  cluster can be unhealthy for. If the limit is reached, the script will
  terminate with an error. Default 600.
* ``--jolokia-port PORT``: The Jolokia port. Default 8778.
* ``--jolokia-prefix PREFIX``: The Jolokia prefix. Default "jolokia/".
* ``--no-confirm``: If specified, the script will not ask for confirmation.
* ``--skip N``: Skip the first N servers. Useful to recover from a partial
  rolling restart. Default 0.
* ``--verbose``: Turn on verbose output.

Examples
========

Restart the generic dev cluster, checking the JMX metrics every 30 seconds, and
restarting the next broker after 5 consecutive checks have confirmed the health
of the cluster:

.. code-block:: bash

   $ kafka-rolling-restart --cluster-type generic --cluster-name dev --check-interval 30 --check-count 5

Check the generic prod cluster. It will report an error if the cluster is
unhealthy for more than 900 seconds:

.. code-block:: bash

   $ kafka-rolling-restart --cluster-type generic --cluster-name prod --unhealthy-time-limit 900
