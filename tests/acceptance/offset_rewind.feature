Feature: kafka_consumer_manager offset_rewind subcommand

  Scenario: Calling the offset_rewind command
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
     Given we have a kafka consumer group
      when we call the offset_rewind command and commit into kafka
      when we call the offset_get command
      then consumer_group wont exist since it is rewind to low_offset 0

  Scenario: Calling the offset_rewind command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_rewind command with a new groupid and the force option
      then the committed offsets will match the earliest message offsets
