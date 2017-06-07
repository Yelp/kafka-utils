Feature: kafka_consumer_manager unsubscribe_topics subcommand

  Scenario: Calling the unsubscribe_topics command with zookeeper storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the unsubscribe_topics command with zookeeper storage
      then the committed offsets will no longer exist in zookeeper

  @kafka_offset_storage
  Scenario: Calling the unsubscribe_topics command with kafka storage
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the unsubscribe_topics command with kafka storage
      then the committed offsets will no longer exist in kafka
