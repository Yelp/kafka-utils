Feature: kafka-consumer-manager has a list_groups command

  Scenario: Existing cluster with multiple consumer groups
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed
