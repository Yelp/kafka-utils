Feature: kafka_consumer_manager offset_advance

  Scenario: Calling the offset_advance command with zookeeper storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command with a groupid and topic with zk storage
      then the committed offsets will match the latest message offsets

  @kafka9
  Scenario: Calling the offset_advance command with default storage
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command and commit into kafka
      when we call the offset_get command with kafka storage
      then the latest message offsets will be shown

  Scenario: Calling the offset_advance command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_advance command with a new groupid and the force option with zk storage
      then the committed offsets will match the latest message offsets
