Feature: under_replicated

  Scenario: Calling the under_replicated command on a cluster without under replicated partitions
     Given we have an existing kafka cluster with a topic
      when we call the under_replicated command
      then OK under_replicated will be printed
