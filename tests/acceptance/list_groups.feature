Feature: kafka_consumer_manager list_groups subcommand

  Scenario: Calling the list_groups command with default storage
     Given we have a set of existing consumer groups
      when we call the list_groups command with default storage
      then the groups will be listed

  @kafka9
  Scenario: Calling the list_group command with kafka storage
     Given we have initialized kafka offsets storage
     Given we have a set of existing consumer groups with kafka storage
      when we call the list_groups command with kafka storage
      then the groups will be listed
