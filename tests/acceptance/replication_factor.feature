Feature: replication_factor

  @kafka11
  Scenario: Calling the replication_factor check on empty cluster
      Given we have an existing kafka cluster
       when we call the replication_factor check
       then OK from replication_factor will be printed

  Scenario: Calling the replication_factor check on a cluster with proper replication factor for each topic
      Given we have an existing kafka cluster with a topic
       when we call the replication_factor check with adjusted min.isr
       then OK from replication_factor will be printed

  @kafka11
  Scenario: Calling the replication_factor check on a cluster with wrong replication factor for one of the topics
      Given we have an existing kafka cluster with a topic
       when we call the replication_factor check
       then CRITICAL from replication_factor will be printed
