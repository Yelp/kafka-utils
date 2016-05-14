Usage examples
###############

Kafka-cluster-manager
*********************

Rebalancing all layers
======================

Rebalance all layers for given cluster. This command will generate a plan with a
maximum of 10 partition movements and 25 leader-only changes after rebalancing
the cluster for all layers discussed before prior to sending it to zookeeper.

.. option::
    $ kafka-cluster-manager --cluster-type <type> rebalance --replication-groups --brokers --leaders  --apply
    --max-partition-movements 10 --max-leader-changes 25


Rebalancing only leaders
========================
Rebalances only the distribution of leaders across the brokers and generates the plan.

.. option::
    $ kafka-cluster-manager --cluster-type scribe rebalance --leaders

Decommissioning brokers
=======================
Decommission given broker(s).

.. option::
    $ kafka-cluster-manager --cluster-type scribe decommission <broker-id>

Imbalance Statistics
====================
Get imbalance statistics of the current cluster-state.

.. option::
    $ kafka-cluster-manager --cluster-type scribe stats

Get imbalance statistics after applying the given assignment.
Sample assignment in plan.json : {"versions": 1, "partitions": [{"topic": "t1": 0, "replicas":[0, 1]}]}

.. option::
    $ kafka-cluster-manager --cluster-type scribe stats --read-from-file plan.json

Replace Broker
==============
Move partitions from the source broker 1 to destination broker 2 for sample-cluster.

.. option::
    $ kafka-cluster-manager --cluster-type scribe --cluster-name sample-cluster replace-broker --source-broker 1
    --dest-broker 2
