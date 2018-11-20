Feature: kafka_consumer_manager offset_get subcommand

  Scenario: Calling the offset_get command with json option
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_get command with the json option
      then the correct json output will be shown

  Scenario: Committing offsets into Kafka and fetching offsets
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
      when we commit some offsets for a group into kafka
      when we fetch offsets for the group
      then the fetched offsets will match the committed offsets

  Scenario: Calling the offset_get command
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_set command and commit into kafka
      when we call the offset_get command
      then the offset that was committed into Kafka will be shown
