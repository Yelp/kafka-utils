Configuring Kafka-discovery
###########################

The cluster configuration is set up by default from yaml files located at /nail/etc/kafka_discovery.
The naming convention of the yaml files is <cluster-type>.yaml.

Sample configuration for scribe cluster can be found at /nail/etc/kafka_discovery/scribe.yaml

.. code-block:: yaml
    ---
      clusters:
        cluster-1:
          broker_list:
            - "kafka-scribe-elb-cluster1:9092"
          zookeeper: "11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-scribe"
        cluster-2:
          broker_list:
            - "kafka-scribe-elb-cluster2:9092"
          zookeeper: "11.11.11.211:2181,11.11.11.212:2181,11.11.11.213:2181/kafka-scribe"
      local_config:
        cluster: cluster-1

For example the kafka-cluster-manager command :-

.. option::
    kafka-cluster-manager --cluster-type scribe stats

will pick up default cluster `cluster-1` from the local_config at /nail/etc/kafka_discovery/scribe.yaml to display
statistics of default kafka-configuration.

.. note:: For kafka-cluster-manager the path can be overridden with --discovery-base-path param
