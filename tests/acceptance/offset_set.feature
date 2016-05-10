Feature: kafka_consumer_manager offset_set subcommand

  Scenario: Calling the offset_set command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_set command with a groupid and offset data
      then the committed offsets will match the specified offsets

  Scenario: Calling the offset_set command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_set command with a new groupid and the force option
      then the committed offsets will match the specified offsets
