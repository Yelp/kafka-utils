Feature: kafka_consumer_manager delete_group subcommand

  Scenario: Calling the delete_group command with zookeeper storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the delete_group command with zookeeper storage
      when we call the offset_get command with zookeeper storage
      then the specified group will not be found
