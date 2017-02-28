Feature: kafka_consumer_manager list_topics subcommand

  Scenario: Calling the list_topics command with zookeeper storage
     Given we have a set of existing topics and a consumer group
      when we call the list_topics command with zookeeper storage
      then the topics will be listed

  @kafka_offset_storage    
  Scenario: Calling the list_topics command with default storage
     Given we have an existing kafka cluster with a topic
      given we have a set of existing topics and a consumer group with default storage
      when we call the list_topics command with default storage
      then the topics will be listed
