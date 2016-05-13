Kafka-Tools
===========

A framework for managing library containing tools to interact with kafka clusters and manage them.

Configuration of kafka-clusters
-------------------------------

The cluster configuration is set-up by default from yaml files at /nail/etc/kafka_discovery as <cluster-type>.yaml files

Sample configuration for scribe cluster at /nail/etc/kafka_discovery/scribe.yaml

```yaml
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
```

Install
-------

From PyPI:
```shell
    $ pip install kafka-tools
```


Kafka-tools command-line interface
----------------------------------

Setup the scribe.yaml as discussed above for cluster configuration.


* Rebalances the distribution of leaders across the brokers and generates the plan

```shell
    $ kafka-cluster-manager --cluster-type scribe rebalance --leaders
```

* Rebalance all layers of scribe cluster

```shell
    $ kafka-cluster-manager --cluster-type scribe rebalance --replication-groups --brokers --leaders  --apply
```

* Get imbalance statistics of the current cluster-state

```shell
    $ kafka-cluster-manager --cluster-type scribe --cluster-name cluster-2 stats
```

* Decommission given broker 123

```shell
    $ kafka-cluster-manager --cluster-type scribe decommission 123
```
