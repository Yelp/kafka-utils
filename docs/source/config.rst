Configuration
#############

Kafka-Utils reads the cluster configuration needed to access Kafka clusters from yaml files.
Each cluster is identified by *type* and *name*.
Multiple clusters of the same type should be listed in the same `type.yaml` file.
The yaml files are read from :code:`$KAFKA_DISCOVERY_DIR`, :code:`$HOME/.kafka_discovery` and :code:`/etc/kafka_discovery`,
the former overrides the latter.

Sample configuration for :code:`sample_type` cluster at :code:`/etc/kafka_discovery/sample_type.yaml`

.. code-block:: yaml

    ---
      clusters:
        cluster-1:
          broker_list:
            - "cluster-elb-1:9092"
          zookeeper: "11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1"
        cluster-2:
          broker_list:
            - "cluster-elb-2:9092"
          zookeeper: "11.11.11.211:2181,11.11.11.212:2181,11.11.11.213:2181/kafka-2"
      local_config:
        cluster: cluster-1

For example the kafka-cluster-manager command:

.. code:: bash

    $ kafka-cluster-manager --cluster-type sample_type stats

will pick up default cluster `cluster-1` from the local_config at /etc/kafka_discovery/sample_type.yaml to display
statistics of default kafka-configuration.
