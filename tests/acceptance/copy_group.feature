Feature: kafka_consumer_manager copy_group subcommand

  Scenario: Calling the copy_group command with zookeeper storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the copy_group command with a new groupid with zookeeper storage
      then the committed offsets in the new group will match the old group

  Scenario: Calling the copy_group command with default storage
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
     Given we have a kafka consumer group with storage option kafka
      when we call the copy_group command with a new groupid with default storage
      then the committed offsets in kafka for the new group will match the old group
