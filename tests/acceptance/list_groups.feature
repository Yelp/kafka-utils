Feature: kafka_consumer_manager list_groups subcommand

  Scenario: Calling the list_group command
     Given we have initialized kafka offsets storage
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed
