Feature: kafka_consumer_manager list_topics subcommand

  Scenario: Calling the list_topics command
     Given we have an existing kafka cluster with a topic
      given we have a set of existing topics and a consumer group
      when we call the list_topics command
      then the topics will be listed
