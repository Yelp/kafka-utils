Feature: kafka_consumer_manager

  Scenario: Calling the list_groups command
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed

  Scenario: Calling the list_topics command
     Given we have a set of existing topics and a consumer group
      when we call the list_topics command
      then the topics will be listed

  Scenario: Calling the offset_get command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_get command
      then the correct offset will be shown

  Scenario: Calling the offset_restore command
     Given we have an existing kafka cluster with a topic
     Given we have a json offsets file
      when we call the offset_restore command with the offsets file
      then the committed offsets will match the offsets file

  Scenario: Calling the offset_rewind command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_rewind command with a groupid and topic
      then the committed offsets will match the earliest message offsets

  Scenario: Calling the offset_save command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_save command with an offsets file
      then the correct offsets will be saved into the given file

  Scenario: Calling the offset_set command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_set command with a groupid and offset data
      then the committed offsets will match the specified offsets

  Scenario: Calling the offset_advance command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command with a groupid and topic
      then the committed offsets will match the latest message offsets

  Scenario: Calling the unsubscribe_topics command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the unsubscribe_topics command
      then the committed offsets will no longer exist in zookeeper

  Scenario: Calling the copy_group command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the copy_group command with a new groupid
      then the committed offsets in the new group will match the old group


  # Test offset fetch and commit with Kafka 0.9.0
  Scenario: Committing offsets into Kafka and fetching offsets with dual option
     Given we have an existing kafka cluster with a topic
      when we commit some offsets for a group into kafka
      when we fetch offsets for the group with the dual option
      then the fetched offsets will match the committed offsets

  Scenario: Calling the offset_get command with dual storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_set command and commit into kafka
      when we call the offset_get command with the dual storage option
      then the offset that was committed into Kafka will be shown

  Scenario: Calling the offset_rewind command with kafka storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_rewind command and commit into kafka
      when we call the offset_get command
      then the correct offset will be shown

  Scenario: Calling the offset_advance command with kafka storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_advance command and commit into kafka
      when we call the offset_get command
      then the correct offset will be shown

  Scenario: Calling the offset_save command with kafka storage
     Given we have an existing kafka cluster with a topic
     Given we have a json offsets file
      when we call the offset_restore command with the offsets file and kafka storage
      when we call the offset_save command with an offsets file and kafka storage
      then the restored offsets will be saved into the given file

  Scenario: Calling the offset_set command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_set command with a new groupid and the force option
      then the committed offsets will match the specified offsets

  Scenario: Calling the offset_advance command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_advance command with a new groupid and the force option
      then the committed offsets will match the latest message offsets

  Scenario: Calling the offset_rewind command when the group doesn't exist
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the offset_rewind command with a new groupid and the force option
      then the committed offsets will match the earliest message offsets
