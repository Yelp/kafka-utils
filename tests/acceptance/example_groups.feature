Feature: creating and listing consumer groups

  Scenario: list_groups command shows existing groups
     Given we have a set of existing consumer groups
      when we call the list_groups command
      then the groups will be listed