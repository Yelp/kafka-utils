Feature: kafka_consumer_manager copy_group subcommand

  Scenario: Calling the copy_group command
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage
     Given we have a kafka consumer group
      when we call the copy_group command with a new groupid
      then the committed offsets in kafka for the new group will match the old group
