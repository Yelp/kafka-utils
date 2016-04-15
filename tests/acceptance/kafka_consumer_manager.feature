Feature: kafka_consumer_manager

  Scenario: Calling the list_groups command
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed

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

  Scenario: Calling the offset_save command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the offset_save command with an offsets file
      then the correct offsets will be saved into the given file

  Scenario: Calling the offset_set command
     Given we have an existing kafka cluster with a topic
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
