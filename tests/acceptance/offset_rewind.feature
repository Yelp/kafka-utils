Feature: kafka_consumer_manager offset_rewind subcommand

  Scenario: Calling the offset_rewind command
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
     Given we have a kafka consumer group
      when we call the offset_rewind command and commit into kafka
      when we call the offset_get command
      then consumer_group wont exist since it is rewind to low_offset 0
