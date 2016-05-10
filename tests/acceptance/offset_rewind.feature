Feature: kafka_consumer_manager offset_rewind subcommand

  Scenario: Calling the offset_rewind command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_rewind command with a groupid and topic
      then the committed offsets will match the earliest message offsets

  @kafka9
  Scenario: Calling the offset_rewind command with kafka storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_rewind command and commit into kafka
      when we call the offset_get command
      then the correct offset will be shown

  Scenario: Calling the offset_rewind command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_rewind command with a new groupid and the force option
      then the committed offsets will match the earliest message offsets
