Feature: kafka_consumer_manager offset_advance

  Scenario: Calling the offset_advance command
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command and commit into kafka
      when we call the offset_get command
      then the latest message offsets will be shown
