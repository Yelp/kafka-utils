Feature: commit the offset specified from file for a groupid and topic

  Scenario: offset_restore command called with a json file input file
     Given we have a kafka cluster and a json file describing offset information
      when we call the offset_restore command
      then the correct offsets will be commited