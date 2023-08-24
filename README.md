**Deprecation Warning**

Please note that this repo is not maintained in the open source community. The code and examples
contained in this repository are for demonstration purposes only.

You can read the latest from Yelp Engineering on our [tech blog](https://engineeringblog.yelp.com/).

[![Build Status](https://github.com/Yelp/kafka-utils/workflows/kafka-utils-ci/badge.svg?branch=master)](https://github.com/Yelp/kafka-utils)

# Kafka-Utils

A suite of python tools to interact and manage Apache Kafka clusters.
Kafka-Utils runs on python 3.7+.

## Configuration

Kafka-Utils reads cluster configuration needed to access Kafka clusters from yaml files. Each cluster is identified by *type* and *name*.
Multiple clusters of the same type should be listed in the same `type.yaml` file.
The yaml files are read from `$KAFKA_DISCOVERY_DIR`, `$HOME/.kafka_discovery` and `/etc/kafka_discovery`, the former overrides the latter.


Sample configuration for `sample_type` cluster at `/etc/kafka_discovery/sample_type.yaml`

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
    $ pip install kafka-utils
```


## Kafka-Utils command-line interface

### List all clusters

```shell
    $ kafka-utils
    cluster-type sample_type:
        cluster-name: cluster-1
        broker-list: cluster-elb-1:9092
        zookeeper: 11.11.11.111:2181,11.11.11.112:2181,11.11.11.113:2181/kafka-1
        cluster-name: cluster-2
        broker-list: cluster-elb-2:9092
        zookeeper: 11.11.11.211:2181,11.11.11.212:2181,11.11.11.213:2181/kafka-2
```

### Get consumer offsets

```shell
    $ kafka-consumer-manager --cluster-type sample_type offset_get sample_consumer
```

### Get consumer watermarks

```shell
    $ kafka-consumer-manager --cluster-type sample_type get_topic_watermark sample.topic

```


### Rebalance cluster cluster1 of type sample_cluster

```shell
    $ kafka-cluster-manager --cluster-type sample_type --cluster-name cluster1
    --apply rebalance --brokers --leaders --max-partition-movements 10
    --max-leader-changes 15
```

### Rolling-restart a cluster

```shell
    $ kafka-rolling-restart --cluster-type sample_type
```

### Check in-sync replicas

```shell
    $ kafka-check --cluster-type sample_type min_isr
```

### Check number of unavailable replicas

```shell
    $ kafka-check --cluster-type sample_type replica_unavailability
```

## Documentation

Read the documentation at [Read the Docs](http://kafka-utils.readthedocs.io/en/latest/).

## License

Kafka-Utils is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Contributing

Everyone is encouraged to contribute to Kafka-Utils by forking the
[Github repository](http://github.com/Yelp/kafka-utils) and making a pull request or opening an issue.
