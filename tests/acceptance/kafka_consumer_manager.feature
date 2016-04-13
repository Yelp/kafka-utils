Feature: kafka_consumer_manager

  Scenario: Calling the list_groups command
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed

  Scenario: Calling the offset_get command
     Given we have an existing kafka cluster with a topic
      when we consume some number of messages from the topic
      when we call the offset_get command
      then the correct offset will be shown

  Scenario: Calling the offset_restore command
     Given we have a kafka cluster and a json offsets file
      when we call the offset_restore command with the offsets file
      then the correct offsets will be commited

  Scenario: Calling the offset_save command
     Given we have an existing kafka cluster with a topic
      when we consume some number of messages from the topic
      when we call the offset_save command with an offsets file
      then the correct offsets will be saved into the given file