Feature: kafka_consumer_manager copy_group subcommand

  Scenario: Calling the copy_group command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the copy_group command with a new groupid
      then the committed offsets in the new group will match the old group
