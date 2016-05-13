Feature: kafka_consumer_manager delete_group subcommand

  Scenario: Calling the delete_group command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the delete_group command
      when we call the offset_get command
      then the specified group will not be found

  Scenario: Not Calling the delete_group command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_get command
      then the specified group will be found

  @kafka9
  Scenario: Calling the delete_group command with kafka storage
     Given we have an existing kafka cluster with a topic
      when we commit some offsets for a group into kafka
      when we call the delete_group command with kafka storage
      when we call the offset_get command with kafka storage
      then the specified group will not be found

  @kafka9
  Scenario: Calling the delete_group command with kafka storage after consuming
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command and commit into kafka
      when we call the delete_group command with kafka storage
      when we call the offset_get command with kafka storage
      then the specified group will not be found
