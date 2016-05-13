# Kafka-Tools


Kafka-Tools is a library containing tools to interact with kafka clusters and manage them.

## Kafka-cluster-manager
This tool provides a set of commands to manipulate and modify the cluster topology and get metrics for different states of the cluster. These include balancing the cluster-state, decommissioning brokers, evaluating metrics for the current state of the cluster. Each of these commands is as described below.

## Rebalancing cluster
This command provides the functionality to re-distribute partitions across the
cluster to bring it into a more balanced state. The goal is to load balance the
cluster based on the distribution of the replicas across replication-groups
(availability-zones or racks), distribution of partitions across the brokers,
ingress-rate load on leaders. The imbalance-state of cluster has been
characterized into 4 different layers :-

## Replica-distribution
– Uniform distribution of replicas across replication-groups.

## Partition distribution
– Uniform distribution of partitions across groups and brokers.

## Broker as leaders distribution
– Some brokers might be elected as leaders for more partitions than others. This creates load-imbalance for these brokers. Balancing this layer ensures the uniform election of brokers as leaders.

## Configuration of kafka-clusters

The cluster configuration is set-up by default from yaml files at `/nail/etc/kafka_discovery` as <cluster-type>.yaml files

Sample configuration for scribe cluster at `/nail/etc/kafka_discovery/scribe.yaml`

```yaml
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
