Feature: kafka_consumer_manager watermark_get subcommand

  Scenario: Calling the watermark_get command
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the watermark_get command
      then the correct watermark will be shown
