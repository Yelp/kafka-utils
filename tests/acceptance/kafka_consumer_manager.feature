Feature: kafka_consumer_manager

  Scenario: Calling the list_groups command
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed
