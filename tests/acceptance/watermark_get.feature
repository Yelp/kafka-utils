Feature: kafka_consumer_manager watermark_get subcommand

  Scenario: Calling the watermark_get command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we call the watermark_get command
      then the correct watermark will be shown

    Scenario: Calling the watermark_get command with -r
      Given we have an existing kafka cluster with multiple topics
       when we call the watermark_get command with -r
       then the correct two topics will be shown

    Scenario: Calling the watermark_get command without -r
       Given we have an existing kafka cluster with multiple topics
        when we call the watermark_get command without -r
        then the correct single topic will be shown
