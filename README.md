# Kafka-Tools

A suite of tools to interact and manage Apache Kafka cluster.

## Configuration of kafka-clusters

The cluster configuration is set-up by default from yaml files at /nail/etc/kafka_discovery as <cluster-type>.yaml files

Sample configuration for sample_type cluster at /nail/etc/kafka_discovery/sample_type.yaml

```yaml
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
```

## Install

From PyPI:
```shell
    $ pip install kafka-tools
```


## Kafka-tools command-line interface

Setup the sample_type.yaml as discussed above for cluster configuration.


* Rebalance all layers of sample_type cluster

```shell
    $ kafka-cluster-manager --cluster-type sample_type rebalance --replication-groups --brokers --leaders  --apply
```

* Get imbalance statistics of the current cluster-state

```shell
    $ kafka-cluster-manager --cluster-type sample_type --cluster-name cluster-2 stats
```
