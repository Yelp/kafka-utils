Feature: get the offset for a groupid and topic

  Scenario: offset_get command called with an existing topic
     Given we have an existing consumer group for a topic in the kafka cluster
      when we call the offset_get command
      then the correct offset  will be shown