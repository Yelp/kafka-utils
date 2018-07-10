@kafka_offset_storage
Feature: Change topic configuration using ZK

  @kafka10
  Scenario: Calling ZK to change topic level configs
    Given we have an existing kafka cluster with a topic
      when we set the configuration of the topic to 0 bytes
      then we produce to a kafka topic it should fail
      when we change the topic config in zk to 10000 bytes for kafka 10
      then we produce to a kafka topic it should succeed
