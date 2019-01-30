Feature: kafka_consumer_manager rename_group subcommand

  Scenario: Calling the rename_group command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the rename_group command
      then the committed offsets in the new group will match the expected values
      then the group named has been changed
