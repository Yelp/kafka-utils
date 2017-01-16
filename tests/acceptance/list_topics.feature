Feature: kafka_consumer_manager list_topics subcommand

  Scenario: Calling the list_topics command with default storage
     Given we have a set of existing topics and a consumer group
      when we call the list_topics command
      then the topics will be listed

  @kafka9
  Scenario: Calling the list_topics command with kafka storage
     Given we have an existing kafka cluster with a topic
      given we have a set of existing topics and a consumer group with kafka storage
      when we call the list_topics command with kafka storage
      then the topics will be listed
