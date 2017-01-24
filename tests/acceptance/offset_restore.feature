Feature: kafka_consumer_manager offset_restore subcommand

  Scenario: Calling the offset_restore command with zookeeper storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
     Given we have a json offsets file
      when we call the offset_restore command with the offsets file with zookeeper storage
      then the committed offsets will match the offsets file
